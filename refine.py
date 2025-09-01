import os
import re
import json
import shutil
import datetime
from pathlib import Path
from typing import Optional, Tuple, Dict, List, Callable

# --- Utility: Logging with timestamps and durations ---
def log_step_start(step_name: str, log_func: Optional[Callable[[str], None]] = None) -> datetime.datetime:
    start_time = datetime.datetime.now()
    msg = f"[{start_time.strftime('%Y-%m-%d %H:%M:%S')}] 🚀 Starting: {step_name}"
    if log_func:
        log_func(msg)
    else:
        print(msg)
    return start_time

def log_step_end(step_name: str, start_time: datetime.datetime, log_func: Optional[Callable[[str], None]] = None) -> None:
    end_time = datetime.datetime.now()
    elapsed = end_time - start_time
    minutes, seconds = divmod(elapsed.total_seconds(), 60)
    msg = (f"[{end_time.strftime('%Y-%m-%d %H:%M:%S')}] ✅ Finished: {step_name} "
           f"(took {int(minutes)}m {int(seconds)}s)\n")
    if log_func:
        log_func(msg)
    else:
        print(msg)

# --- Helpers for CREATE TABLE parsing ---
def adjust_varchar_and_key_length(line: str) -> Tuple[str, Optional[int]]:
    varchar_length = None
    match = re.search(r'`(\w+)`\s+varchar\((\d+)\)', line, re.IGNORECASE)
    if match:
        length = int(match.group(2))
        new_length = 100 if length <= 100 else 191
        varchar_length = new_length
        line = re.sub(r'varchar\(\d+\)', f'VARCHAR({new_length})', line, flags=re.IGNORECASE)
    line = re.sub(r'(`\w+`)\(\d+\)', r'\1(191)', line)
    return line, varchar_length

def parse_enum_values(dtype: str) -> Optional[List[str]]:
    match = re.match(r"enum\((.*)\)", dtype, re.IGNORECASE)
    if not match:
        return None
    vals = [v.strip().strip("'").strip('"') for v in match.group(1).split(',')]
    return vals

def parse_column_definition(line: str, varchar_length: Optional[int]) -> Optional[Dict]:
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

# --- Main pipeline function ---
def refine_sql(input_file: str, log_func: Optional[Callable[[str], None]] = None, output_folder: str = "results") -> Path:
    """
    Refine a MySQL dump file and generate cleaned SQL in the output folder.
    
    Args:
        input_file: Path to the original SQL dump file.
        log_func: Optional function to log messages (used by GUI).
        output_folder: Folder where refined SQL file will be saved.
        
    Returns:
        Path to the final refined SQL file.
    """
    input_file = Path(input_file)
    output_dir = Path("chunks")
    final_output_dir = Path(output_folder)
    final_output_dir.mkdir(parents=True, exist_ok=True)
    
    base_name = input_file.stem
    create_file = output_dir / f"{base_name}_0.sql"
    insert_temp_file = output_dir / f"{base_name}_temp.sql"
    extra_file = output_dir / f"{base_name}_temp_extra.sql"
    json_index_file = output_dir / f"{base_name}_indexes.json"
    refined_file = output_dir / f"{base_name}_temp_refined.sql"
    final_output_file = final_output_dir / f"{base_name}.sql"
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # --- Step 1: Extract CREATE TABLEs and metadata ---
    step1_start = log_step_start("Step 1: Extract CREATE TABLEs and metadata", log_func)
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

            if stripped.lower().startswith("create table"):
                inside_create = True
                create_block = [line]
                column_index = 0
                table_match = re.search(r"create\s+table\s+[`\"]?(\w+)[`\"]?", stripped, re.IGNORECASE)
                if table_match:
                    current_table = table_match.group(1)
                    column_metadata_map[current_table] = []
                continue

            if stripped.lower().startswith(("create algorithm","create view", "create trigger", "create procedure", "create function")):
                block = [line]
                while not stripped.endswith(";"):
                    line = next(fin)
                    stripped = line.strip()
                    block.append(line)
                fextra.writelines(block)
                fextra.write("\n")
                continue

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

    with open(json_index_file, 'w', encoding='utf-8') as fjson:
        json.dump(column_metadata_map, fjson, indent=2)

    log_step_end("Step 1: Extract CREATE TABLEs and metadata", step1_start, log_func)

    # --- Step 2: Extract INSERT statements ---
    step2_start = log_step_start("Step 2: Extract INSERT statements", log_func)
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
                            commented_stmt = "-- " + stmt.replace("\n", "\n-- ")
                            fextra.write(commented_stmt + "\n")
                        else:
                            out.write(stmt.strip() + "\n")
                    else:
                        commented_stmt = "-- " + stmt.replace("\n", "\n-- ")
                        fextra.write(commented_stmt + "\n")
                    start = None
                i += 1

    write_insert_statements(input_file, insert_temp_file, extra_file)
    log_step_end("Step 2: Extract INSERT statements", step2_start, log_func)

    # --- Step 3: Refine INSERT values ---
    step3_start = log_step_start("Step 3: Refine INSERT values", log_func)
    with open(json_index_file, 'r', encoding='utf-8') as f:
        table_metadata = json.load(f)

    def get_fallback(dtype, enum_values=None):
        dtype = dtype.lower()
        if enum_values:
            return f"'{enum_values[0]}'"
        if dtype.startswith('int'):
            return "'0'"
        if 'float' in dtype or 'double' in dtype or 'decimal' in dtype:
            return "'0.0'"
        if dtype.startswith('date'):
            return "'1970-01-01'"
        if dtype.startswith('datetime') or dtype.startswith('timestamp'):
            return "'1970-01-01 00:00:00'"
        return "''"

    def sanitize_value(inner: str) -> str:
        return re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', inner)

    def truncate_value(value, col):
        max_len = col.get('varchar_length')
        enum_values = col.get('enum_values')
        is_quoted = (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"'))
        if is_quoted:
            inner = value[1:-1]
            inner = sanitize_value(inner)
            if enum_values and inner not in enum_values:
                inner = enum_values[0]
            if max_len:
                inner_bytes = inner.encode('utf-8')
                if len(inner_bytes) > max_len:
                    truncated_bytes = inner_bytes[:max_len]
                    while True:
                        try:
                            inner = truncated_bytes.decode('utf-8')
                            break
                        except UnicodeDecodeError:
                            truncated_bytes = truncated_bytes[:-1]
            inner_fixed = inner.replace("\\", "\\\\").replace("'", "''")
            return f"'{inner_fixed}'"
        return value

    def fix_row(row_line, column_info):
        row = row_line.strip()
        if not row.startswith('('):
            return row_line
        ends_with_comma = row.endswith(',') or row.endswith(',\n')
        ends_with_semicolon = row.endswith(');')
        if ends_with_comma: row = row[:-1]
        elif ends_with_semicolon: row = row[:-2]
        row = row.strip().lstrip('(').rstrip(')')
        parts, current, in_quotes, escape, quote_char = [], '', False, False, ''
        for char in row:
            if escape:
                current += "\\" + char
                escape = False
            elif char == '\\':
                escape = True
            elif char in ["'", '"']:
                current += char
                if not in_quotes:
                    in_quotes = True; quote_char = char
                elif char == quote_char:
                    in_quotes = False
            elif char == ',' and not in_quotes:
                parts.append(current.strip()); current = ''
            else:
                current += char
        parts.append(current.strip())
        for col in column_info:
            idx = col['index']
            if idx >= len(parts): continue
            val = parts[idx]
            if val in ["''", '""']:
                if col['nullable']: parts[idx] = 'NULL'
                elif col.get('default') is not None: parts[idx] = col['default']
                else: parts[idx] = get_fallback(col['type'], col.get('enum_values'))
            else:
                parts[idx] = truncate_value(val, col)
        fixed_row = '(' + ', '.join(parts)
        if ends_with_comma: fixed_row += '),'
        elif ends_with_semicolon: fixed_row += ');'
        else: fixed_row += ')'
        return fixed_row + '\n'

    with open(insert_temp_file, 'r', encoding='utf-8') as infile, open(refined_file, 'w', encoding='utf-8') as outfile:
        current_table, current_columns, row_buffer, accumulating_row = None, [], [], False
        for line in infile:
            stripped = line.strip()
            if stripped.lower().startswith('insert into'):
                if row_buffer:
                    full_row = ''.join(row_buffer)
                    outfile.write(fix_row(full_row, current_columns))
                    row_buffer = []; accumulating_row = False
                match = re.match(r'insert into [`"]?(\w+)[`"]?\s+values', stripped, re.IGNORECASE)
                if match:
                    current_table = match.group(1)
                    current_columns = table_metadata.get(current_table, [])
                outfile.write(line)
            elif stripped.startswith('(') or accumulating_row:
                row_buffer.append(line); accumulating_row = True
                if stripped.endswith('),') or stripped.endswith(');'):
                    full_row = ''.join(row_buffer)
                    outfile.write(fix_row(full_row, current_columns))
                    row_buffer = []; accumulating_row = False
            else:
                outfile.write(line)
        if row_buffer:
            full_row = ''.join(row_buffer)
            outfile.write(fix_row(full_row, current_columns))

    log_step_end("Step 3: Refine INSERT values", step3_start, log_func)

    # --- Step 4: Concatenate into final SQL ---
    step4_start = log_step_start("Step 4: Concatenate into final SQL", log_func)
    with open(final_output_file, 'w', encoding='utf-8') as fout:
        for fpath in [create_file, refined_file, extra_file]:
            if os.path.exists(fpath):
                with open(fpath, 'r', encoding='utf-8') as fin:
                    fout.write(f"-- Start of {os.path.basename(fpath)}\n")
                    fout.write(fin.read())
                    fout.write(f"\n-- End of {os.path.basename(fpath)}\n\n")
    log_func and log_func(f"✅ Final concatenated SQL → {final_output_file}")
    log_step_end("Step 4: Concatenate into final SQL", step4_start, log_func)

    # --- Step 5: Cleanup ---
    step5_start = log_step_start("Step 5: Cleanup", log_func)
    shutil.rmtree(output_dir, ignore_errors=True)
    log_func and log_func("🧹 Cleanup complete — only results folder kept.")
    log_step_end("Step 5: Cleanup", step5_start, log_func)

    return final_output_file


# --- For command-line testing ---
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Refine large MySQL dump files")
    parser.add_argument("input_file", help="Path to SQL dump file")
    parser.add_argument("--output", default="results", help="Output folder for refined SQL")
    args = parser.parse_args()
    refine_sql(args.input_file, log_func=print, output_folder=args.output)
