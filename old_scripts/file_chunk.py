import os
import re
import json
from pathlib import Path

# --- Configuration ---
input_file = 'sqls/building_database.sql'
output_dir = 'chunks'
base_name = Path(input_file).stem

create_file = os.path.join(output_dir, f"{base_name}_0.sql")
insert_temp_file = os.path.join(output_dir, f"{base_name}_temp.sql")
extra_file = os.path.join(output_dir, f"{base_name}_temp_extra.sql")
json_index_file = os.path.join(output_dir, f"{base_name}_indexes.json")

Path(output_dir).mkdir(parents=True, exist_ok=True)


# --- Helpers ---
def adjust_varchar_and_key_length(line: str) -> tuple[str, int | None]:
    """Adjust VARCHAR length (<=100 → 100, >100 → 191) and KEY hints."""
    varchar_length = None
    match = re.search(r'`(\w+)`\s+varchar\((\d+)\)', line, re.IGNORECASE)
    if match:
        length = int(match.group(2))
        new_length = 100 if length <= 100 else 191
        varchar_length = new_length
        line = re.sub(r'varchar\(\d+\)', f'VARCHAR({new_length})', line, flags=re.IGNORECASE)
    line = re.sub(r'(`\w+`)\(\d+\)', r'\1(191)', line)
    return line, varchar_length


def parse_enum_values(dtype: str):
    match = re.match(r"enum\((.*)\)", dtype, re.IGNORECASE)
    if not match:
        return None
    vals = [v.strip().strip("'").strip('"') for v in match.group(1).split(',')]
    return vals


def parse_column_definition(line: str, varchar_length: int | None):
    col_def = line.strip().rstrip(',').lower()
    if not col_def or col_def.startswith(('primary', 'unique', 'key', 'constraint', 'index', ')')):
        return None

    match = re.match(r'`?(\w+)`?\s+([a-z]+(\([^)]+\))?)', col_def)
    if not match:
        return None

    col_name = match.group(1)
    dtype = match.group(2)
    nullable = 'not null' not in col_def
    default_match = re.search(r'default\s+((\'[^\']*?\')|("[^"]*?")|[^,\s]+)', col_def, re.IGNORECASE)
    default_value = default_match.group(1) if default_match else None
    enum_values = parse_enum_values(dtype)

    return {
        "name": col_name,
        "type": dtype,
        "nullable": nullable,
        "default": default_value,
        "index": None,
        "varchar_length": varchar_length,
        "enum_values": enum_values
    }


# --- Step 1: Extract CREATE TABLEs and column metadata ---
column_metadata_map = {}
inside_create = False
create_block = []
current_table = None
column_index = 0

with open(input_file, 'r', encoding='utf-8') as fin, \
     open(create_file, 'w', encoding='utf-8') as fcreate, \
     open(extra_file, 'w', encoding='utf-8') as fextra:

    for line in fin:
        stripped = line.strip()

        # --- CREATE TABLE ---
        if stripped.lower().startswith("create table"):
            inside_create = True
            create_block = [line]
            column_index = 0
            table_match = re.search(r"create\s+table\s+[`\"]?(\w+)[`\"]?", stripped, re.IGNORECASE)
            if table_match:
                current_table = table_match.group(1)
                column_metadata_map[current_table] = []
            continue

        # --- CREATE VIEW/TRIGGER/PROCEDURE/FUNCTION ---
        if stripped.lower().startswith(("create algorithm","create view", "create trigger", "create procedure", "create function")):
            block = [line]
            while not stripped.endswith(";"):
                line = next(fin)
                stripped = line.strip()
                block.append(line)
            fextra.writelines(block)
            fextra.write("\n")
            continue

        # --- Inside CREATE TABLE ---
        if inside_create:
            adjusted_line, varchar_length = adjust_varchar_and_key_length(line)
            create_block.append(adjusted_line)

            if current_table:
                col_meta = parse_column_definition(adjusted_line, varchar_length)
                if col_meta:
                    col_meta["index"] = column_index
                    column_metadata_map[current_table].append(col_meta)
                    column_index += 1

            if stripped.endswith(';'):
                fcreate.writelines(create_block)
                fcreate.write("\n")
                inside_create = False
                current_table = None


# --- Save Column Metadata as JSON ---
with open(json_index_file, 'w', encoding='utf-8') as fjson:
    json.dump(column_metadata_map, fjson, indent=2)


# --- Step 2: Extract INSERT statements (handles multiple on one line) ---
def write_insert_statements(src_path: str, dst_path: str, extra_path: str):
    with open(src_path, 'r', encoding='utf-8') as f:
        s = f.read()

    i, n = 0, len(s)
    in_single = in_double = escape = False
    start = None

    def is_insert_at(pos: int) -> bool:
        return s[pos:pos+11].lower() == 'insert into'

    with open(dst_path, 'w', encoding='utf-8') as out, \
         open(extra_path, 'a', encoding='utf-8') as fextra:

        while i < n:
            ch = s[i]

            if start is None:
                if is_insert_at(i):
                    start = i
                    i += 11
                    continue
                i += 1
                continue

            if escape:
                escape = False
            else:
                if ch == '\\' and (in_single or in_double):
                    escape = True
                elif ch == "'" and not in_double:
                    in_single = not in_single
                elif ch == '"' and not in_single:
                    in_double = not in_double

            if ch == ';' and not in_single and not in_double:
                stmt = s[start:i+1]
                insert_match = re.match(r"insert\s+into\s+[`\"]?(\w+)[`\"]?", stmt.strip(), re.IGNORECASE)
                if insert_match:
                    table_name = insert_match.group(1)
                    if table_name.lower().startswith("v_"):
                        # Comment out view INSERTs
                        commented_stmt = "-- " + stmt.replace("\n", "\n-- ")
                        fextra.write(commented_stmt + "\n")
                    else:
                        out.write(stmt.strip() + "\n")
                else:
                    # Comment out unknown insert targets
                    commented_stmt = "-- " + stmt.replace("\n", "\n-- ")
                    fextra.write(commented_stmt + "\n")
                start = None
            i += 1


write_insert_statements(input_file, insert_temp_file, extra_file)

print("✅ file_chunk.py Done!")
print(f"  • CREATE TABLEs → {create_file}")
print(f"  • Column metadata JSON → {json_index_file}")
print(f"  • INSERTs → {insert_temp_file}")
print(f"  • Views/Triggers/Procedures/Functions → {extra_file}")
