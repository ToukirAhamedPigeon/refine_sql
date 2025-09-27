import os
import re
import json
import shutil
import datetime
from pathlib import Path
from typing import Optional, Tuple, Dict, List, Callable, Set

# --- Utility: Logging with timestamps and durations ---
def log_step_start(step_name: str, log_func: Optional[Callable[[str, Optional[int]], None]] = None, progress: int = 0) -> datetime.datetime:
    start_time = datetime.datetime.now()
    msg = f"[{start_time.strftime('%Y-%m-%d %H:%M:%S')}] 🚀 Starting: {step_name}"
    if log_func:
        # support old signature log_func(msg) or log_func(msg, progress)
        try:
            log_func(msg, progress)
        except TypeError:
            log_func(msg)
    else:
        print(msg)
    return start_time

def log_step_end(step_name: str, start_time: datetime.datetime, log_func: Optional[Callable[[str, Optional[int]], None]] = None, progress: int = 100) -> None:
    end_time = datetime.datetime.now()
    elapsed = end_time - start_time
    minutes, seconds = divmod(elapsed.total_seconds(), 60)
    msg = f"[{end_time.strftime('%Y-%m-%d %H:%M:%S')}] ✅ Finished: {step_name} (took {int(minutes)}m {int(seconds)}s)"
    if log_func:
        try:
            log_func(msg, progress)
        except TypeError:
            log_func(msg)
    else:
        print(msg)

# --- Helpers for CREATE TABLE parsing ---
def adjust_varchar_and_key_length(line: str) -> Tuple[str, Optional[int]]:
    """
    Adjust VARCHAR lengths (<=100 -> 100, >100 -> 191)
    and try to standardize index length declarations to (191).
    Returns modified line and varchar_length if a column definition contained VARCHAR(...) that we adjusted.
    """
    varchar_length = None
    match = re.search(r'`(\w+)`\s+varchar\((\d+)\)', line, re.IGNORECASE)
    if match:
        length = int(match.group(2))
        new_length = 100 if length <= 100 else 191
        varchar_length = new_length
        line = re.sub(r'varchar\(\d+\)', f'VARCHAR({new_length})', line, flags=re.IGNORECASE)
    # === CHANGED ===
    # Change index-lengths conservatively only when they appear like `col`(n) (prevents accidental replacements).
    def replace_index_key_len(m):
        return f"{m.group(1)}(191)"
    line = re.sub(r'(`\w+`)\(\d+\)(?=\s*(?:,|\)|\s))', replace_index_key_len, line)
    return line, varchar_length

def parse_enum_values(dtype: str) -> Optional[List[str]]:
    match = re.match(r"enum\((.*)\)", dtype, re.IGNORECASE)
    if not match:
        return None
    vals = [v.strip().strip("'").strip('"') for v in match.group(1).split(',')]
    return vals

def parse_column_definition(line: str, varchar_length: Optional[int]) -> Optional[Dict]:
    col_def = line.strip().rstrip(',').strip()
    # ignore non-column lines
    if not col_def or col_def.lower().startswith(('primary', 'unique', 'key', 'constraint', 'index', 'fulltext', 'spatial', 'check', ')')):
        return None
    match = re.match(r'`?(\w+)`?\s+([a-z]+(\([^)]+\))?)', col_def, re.IGNORECASE)
    if not match:
        return None
    col_name = match.group(1)
    dtype = match.group(2)
    nullable = 'not null' not in col_def.lower()
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
        "enum_values": enum_values,
        "is_primary": False,   # will be set later if needed
    }

# === CHANGED ===
# New helper functions to collect and reorder CREATE TABLE blocks based on FK dependencies and to parse FK details.

# Regex variants to capture constraint-based and unnamed foreign keys
_constraint_fk_re = re.compile(
    r'CONSTRAINT\s+`?([^`\s(]+)`?\s+FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+`?([^`\s(]+)`?\s*\(([^)]+)\)([^,;]*)',
    re.IGNORECASE | re.DOTALL
)
_unnamed_fk_re = re.compile(
    r'FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+`?([^`\s(]+)`?\s*\(([^)]+)\)([^,;]*)',
    re.IGNORECASE | re.DOTALL
)
_simple_fk_regex = re.compile(
    r'FOREIGN\s+KEY\s*\([^)]+\)\s+REFERENCES\s+`?([^`\s(]+)`?\s*\([^)]+\)[^,;]*',
    re.IGNORECASE | re.DOTALL
)

def extract_foreign_refs(create_text: str) -> List[str]:
    """Return list of referenced table names found in FOREIGN KEY definitions."""
    refs = set()
    for m in _constraint_fk_re.finditer(create_text):
        refs.add(m.group(3))
    for m in _unnamed_fk_re.finditer(create_text):
        refs.add(m.group(2))
    for m in _simple_fk_regex.finditer(create_text):
        # fallback simple detection (may duplicate)
        cols = m.group(0)
        maybe = re.search(r'REFERENCES\s+`?([^`\s(]+)`?', cols, re.IGNORECASE)
        if maybe:
            refs.add(maybe.group(1))
    return list(refs)

def extract_fk_details(create_text: str) -> List[Dict]:
    """
    Parse detailed FK definitions from a CREATE TABLE block.
    Returns list of dicts:
      { "constraint": name or None,
        "child_cols": [col1, ...],
        "parent_table": name,
        "parent_cols": [col1, ...],
        "clause": full_clause_text }
    """
    details = []
    # constraint-style FKs
    for m in _constraint_fk_re.finditer(create_text):
        name = m.group(1)
        child_cols_raw = m.group(2)
        parent_table = m.group(3)
        parent_cols_raw = m.group(4)
        clause = m.group(0)
        child_cols = [c.strip().strip('`').strip() for c in child_cols_raw.split(',')]
        parent_cols = [c.strip().strip('`').strip() for c in parent_cols_raw.split(',')]
        details.append({
            "constraint": name,
            "child_cols": child_cols,
            "parent_table": parent_table,
            "parent_cols": parent_cols,
            "clause": clause.strip()
        })
    # unnamed style
    for m in _unnamed_fk_re.finditer(create_text):
        child_cols_raw = m.group(1)
        parent_table = m.group(2)
        parent_cols_raw = m.group(3)
        clause = m.group(0)
        child_cols = [c.strip().strip('`').strip() for c in child_cols_raw.split(',')]
        parent_cols = [c.strip().strip('`').strip() for c in parent_cols_raw.split(',')]
        details.append({
            "constraint": None,
            "child_cols": child_cols,
            "parent_table": parent_table,
            "parent_cols": parent_cols,
            "clause": clause.strip()
        })
    return details

def strip_fk_constraints_and_collect_alters(table_name: str, create_text: str) -> Tuple[str, List[str]]:
    """
    Remove FOREIGN KEY CONSTRAINT lines from the CREATE TABLE SQL and return:
      - modified CREATE TABLE SQL without those CONSTRAINT lines (safe to run even if referenced table missing)
      - a list of ALTER TABLE ... ADD CONSTRAINT ...; statements to re-add them later
    """
    alters = []
    # First capture CONSTRAINT ... FOREIGN KEY
    def repl_constraint(m):
        full = m.group(0).strip().rstrip(',')
        # convert to ALTER TABLE ADD <constraint body>
        alters.append(f"ALTER TABLE `{table_name}` ADD {full};")
        return ''
    # remove constraint clauses
    new_text = _constraint_fk_re.sub(repl_constraint, create_text)

    # Then remove bare FOREIGN KEY ... REFERENCES ... (without CONSTRAINT name)
    # For these we convert to ALTER TABLE ... ADD FOREIGN KEY
    def repl_unnamed(m):
        full = m.group(0).strip().rstrip(',')
        child_cols_raw = m.group(1)
        parent_table = m.group(2)
        parent_cols_raw = m.group(3)
        # build an ALTER clause
        alters.append(f"ALTER TABLE `{table_name}` ADD FOREIGN KEY ({child_cols_raw}) REFERENCES `{parent_table}` ({parent_cols_raw});")
        return ''
    new_text = _unnamed_fk_re.sub(repl_unnamed, new_text)
    # clean up trailing commas before closing paren
    cleaned_create = re.sub(r',\s*\)', '\n)', new_text, flags=re.DOTALL)
    return cleaned_create, alters

def topo_sort_tables(deps: Dict[str, List[str]]) -> Tuple[List[str], bool, List[List[str]]]:
    """
    Perform topological sort on dependency graph: deps[table] = [referenced_table1, ...]
    Returns (sorted_list, success_bool, cycles_list)
    If cycles present, success_bool=False and cycles_list contains lists of tables in cycles.
    """
    # Kahn's algorithm
    all_nodes = set(deps.keys())
    for vs in deps.values():
        all_nodes.update(vs)
    indeg = {n: 0 for n in all_nodes}
    for t, refs in deps.items():
        for r in refs:
            indeg[t] += 1  # edge from r -> t (t depends on r)
    # nodes with zero indegree are those that don't depend on any table
    q = [n for n, d in indeg.items() if d == 0]
    result = []
    while q:
        n = q.pop()
        result.append(n)
        # reduce indegree of nodes that depend on n
        for t, refs in deps.items():
            if n in refs:
                indeg[t] -= 1
                if indeg[t] == 0:
                    q.append(t)
    if len([n for n, d in indeg.items() if d >= 0]) != len(result):
        # cycle exists (detect cycles)
        visited = set()
        cycles = []

        def dfs(u, path):
            visited.add(u)
            path.append(u)
            for v in deps.get(u, []):
                if v in path:
                    idx = path.index(v)
                    cycles.append(path[idx:] + [v])
                elif v not in visited:
                    dfs(v, path.copy())

        for node in deps.keys():
            if node not in visited:
                dfs(node, [])
        return result, False, cycles
    return result, True, []
# --- Main pipeline function ---
def refine_sql(input_file: str, log_func: Optional[Callable[[str], None]] = None, output_folder: str = "results") -> Path:
    """
    Refine a MySQL dump file and generate cleaned SQL in the output folder.

    Changes:
    - Collect all CREATE TABLE blocks in memory, parse FK dependencies and reorder CREATE TABLE blocks
      so referenced tables appear before tables referencing them (topological order).
    - If cycles are detected, remove FK constraints from CREATEs and append equivalent ALTER TABLE ADD CONSTRAINT statements
      at the end of the final SQL file.
    - Rest of pipeline (extract INSERTs, refine values, etc.) follows your original process.

    Returns the final output SQL Path.
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

    # --- Step 1: Extract CREATE TABLEs and metadata (collect in memory first) ---
    step1_start = log_step_start("Step 1: Extract CREATE TABLEs and metadata", log_func, progress=5)
    column_metadata_map = {}
    create_blocks_by_table: Dict[str, str] = {}
    create_blocks_order: List[str] = []  # the order we encountered them (fallback)
    extra_blocks: List[str] = []
    inside_create = False
    current_table = None
    create_lines = []
    column_index = 0

    # Read file line-by-line and collect CREATE blocks (and other create-like objects to extra)
    with open(input_file, 'r', encoding='utf-8') as fin:
        for line in fin:
            stripped = line.strip()
            low = stripped.lower()

            # Start of CREATE TABLE
            if low.startswith("create table"):
                inside_create = True
                create_lines = [line]
                column_index = 0
                table_match = re.search(r"create\s+table\s+[`\"]?(\w+)[`\"]?", stripped, re.IGNORECASE)
                current_table = table_match.group(1) if table_match else None
                if current_table:
                    create_blocks_order.append(current_table)
                    column_metadata_map[current_table] = []
                continue

            # Other CREATEs that we want to stash into extra (views, triggers, procedures)
            if low.startswith(("create algorithm", "create view", "create trigger", "create procedure", "create function")):
                block = [line]
                while not stripped.endswith(";"):
                    line = next(fin)
                    stripped = line.strip()
                    block.append(line)
                extra_blocks.append("".join(block))
                continue

            if inside_create:
                # collect lines for the create block
                create_lines.append(line)
                # attempt to parse column definitions for metadata while collecting (we want column metadata for JSON)
                adjusted_line, varchar_length = adjust_varchar_and_key_length(line)
                if current_table:
                    col_meta = parse_column_definition(adjusted_line, varchar_length)
                    if col_meta:
                        col_meta["index"] = column_index
                        column_metadata_map[current_table].append(col_meta)
                        column_index += 1
                # end of create block
                if stripped.endswith(';'):
                    # join and store block
                    block_text = "".join(create_lines)
                    if current_table:
                        create_blocks_by_table[current_table] = block_text
                    inside_create = False
                    current_table = None
                continue

            # outside create: ignore for step1 (we'll extract INSERTs later from full file)
            # but keep scanning
            continue

    # === CHANGED ===
    # Now we have create_blocks_by_table mapping table_name -> full CREATE TEXT,
    # and column_metadata_map for JSON. Next: build dependency graph from FK references.
    deps = {}  # table -> list of referenced tables
    for tbl, create_sql in create_blocks_by_table.items():
        refs = extract_foreign_refs(create_sql)
        deps[tbl] = refs

    # Attempt topological sort
    sorted_tables, ok, cycles = topo_sort_tables(deps)

    fk_alter_statements: List[str] = []
    ordered_creates: List[Tuple[str, str]] = []

    if ok:
        # safe to write CREATEs in sorted order
        for tbl in sorted_tables:
            if tbl in create_blocks_by_table:
                ordered_creates.append((tbl, create_blocks_by_table[tbl]))
    else:
        # === CHANGED ===
        # Cycles detected. Strategy: remove FK constraint lines from all CREATEs involved in cycles
        # and convert them to ALTER TABLE ... ADD CONSTRAINT statements appended later.
        # We'll be conservative and strip FK definitions from any table that references another table if it participates in a cycle.
        # Build a set of tables in cycles
        cycle_tables: Set[str] = set()
        for cyc in cycles:
            for t in cyc:
                cycle_tables.add(t)
        # For all tables, if in cycle_tables -> strip FK defs and collect alters
        for tbl, create_sql in create_blocks_by_table.items():
            if tbl in cycle_tables:
                cleaned_create, alters = strip_fk_constraints_and_collect_alters(tbl, create_sql)
                fk_alter_statements.extend(alters)
                ordered_creates.append((tbl, cleaned_create))
            else:
                ordered_creates.append((tbl, create_sql))
        # We still should order non-cycle tables respecting deps as much as possible.
        # Do a simple sort: tables without dependencies first, then others (stable by original encounter order)
        # We'll attempt topo sort again on the modified deps with cycle edges removed.
        modified_deps = {}
        for tbl, refs in deps.items():
            # drop references to cycle tables (since we've removed their constraints)
            modified_refs = [r for r in refs if r not in cycle_tables]
            modified_deps[tbl] = modified_refs
        sorted_tables2, ok2, _ = topo_sort_tables(modified_deps)
        if ok2:
            # reorder ordered_creates according to sorted_tables2 where possible, keep rest appended
            create_map = {t: sql for (t, sql) in ordered_creates}
            reordered = []
            seen = set()
            for t in sorted_tables2:
                if t in create_map:
                    reordered.append((t, create_map[t])); seen.add(t)
            # append remaining in original order
            for t, sql in ordered_creates:
                if t not in seen:
                    reordered.append((t, sql))
            ordered_creates = reordered
        # else: leave ordered_creates as-is (we've already stripped FKs in cycle tables so execution won't fail)

    # Write the reordered CREATE TABLES to create_file
    with open(create_file, 'w', encoding='utf-8') as fcreate:
        for tbl, create_sql in ordered_creates:
            fcreate.write(create_sql)
            fcreate.write("\n")

    # Also write collected extra blocks (views, triggers, etc) to extra_file (these may include stored procs etc)
    with open(extra_file, 'w', encoding='utf-8') as fextra:
        for blk in extra_blocks:
            fextra.write(blk)
            fextra.write("\n")
        # If we have extracted FK alters due to cycles, write them into extra_file so they get applied after CREATEs.
        if fk_alter_statements:
            fextra.write("\n-- Foreign key constraints moved here due to cycles or ordering issues\n")
            for stmt in fk_alter_statements:
                fextra.write(stmt + "\n")

    # Save column metadata JSON (unchanged)
    with open(json_index_file, 'w', encoding='utf-8') as fjson:
        json.dump(column_metadata_map, fjson, indent=2)

    log_step_end("Step 1: Extract CREATE TABLEs and metadata", step1_start, log_func, progress=20)

    # --- Step 2: Extract INSERT statements (unchanged algorithm) ---
    step2_start = log_step_start("Step 2: Extract INSERT statements", log_func, progress=21)
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
    log_step_end("Step 2: Extract INSERT statements", step2_start, log_func, progress=40)

    # --- Step 3: Refine INSERT values (mostly unchanged) ---
    step3_start = log_step_start("Step 3: Refine INSERT values", log_func, progress=41)
    with open(json_index_file, 'r', encoding='utf-8') as f:
        table_metadata = json.load(f)

    def get_fallback(dtype, enum_values=None):
        dtype = dtype.lower()
        if enum_values:
            return f"'{enum_values[0]}'"
        if dtype.startswith('int') or dtype.startswith('bigint') or dtype.startswith('tinyint') or dtype.startswith('smallint') or dtype.startswith('mediumint'):
            return "'0'"
        if 'float' in dtype or 'double' in dtype or 'decimal' in dtype:
            return "'0.0'"
        if dtype.startswith('date') and 'time' not in dtype:
            return "'1970-01-01'"
        if dtype.startswith('datetime') or dtype.startswith('timestamp') or 'time' in dtype:
            return "'1970-01-01 00:00:00'"
        return "''"

    def sanitize_value(inner: str) -> str:
        return re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', inner)

    def truncate_value(value, col):
        max_len = col.get('varchar_length')
        enum_values = col.get('enum_values')
        value = value.strip()
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
        i = 0
        while i < len(row):
            char = row[i]
            if escape:
                current += "\\" + char
                escape = False
                i += 1
                continue
            if char == '\\':
                escape = True
                i += 1
                continue
            if char in ["'", '"']:
                current += char
                if not in_quotes:
                    in_quotes = True
                    quote_char = char
                elif char == quote_char:
                    in_quotes = False
                i += 1
                continue
            if char == ',' and not in_quotes:
                parts.append(current.strip())
                current = ''
                i += 1
                continue
            current += char
            i += 1
        parts.append(current.strip())
        for col in column_info:
            idx = col['index']
            if idx >= len(parts):
                continue
            val = parts[idx]
            if val in ["''", '""']:
                if col['nullable']:
                    parts[idx] = 'NULL'
                elif col.get('default') is not None:
                    parts[idx] = col['default']
                else:
                    parts[idx] = get_fallback(col['type'], col.get('enum_values'))
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

    log_step_end("Step 3: Refine INSERT values", step3_start, log_func, progress=70)

    # --- Step 4: Concatenate into final SQL ---
    step4_start = log_step_start("Step 4: Concatenate into final SQL", log_func, progress=71)
    with open(final_output_file, 'w', encoding='utf-8') as fout:
        # Disable foreign key checks at the beginning
        fout.write("SET FOREIGN_KEY_CHECKS=0;\n\n")
        # 1) CREATEs (we wrote them already in ordered manner to create_file)
        for fpath in [create_file, refined_file, extra_file]:
            if os.path.exists(fpath):
                with open(fpath, 'r', encoding='utf-8') as fin:
                    fout.write(f"-- Start of {os.path.basename(fpath)}\n")
                    fout.write(fin.read())
                    fout.write(f"\n-- End of {os.path.basename(fpath)}\n\n")
            # Re-enable foreign key checks at the end
        fout.write("SET FOREIGN_KEY_CHECKS=1;\n")
    log_func and log_func(f"✅ Final concatenated SQL → {final_output_file}")
    log_step_end("Step 4: Concatenate into final SQL", step4_start, log_func, progress=90)

    # --- Step 5: Cleanup ---
    step5_start = log_step_start("Step 5: Cleanup", log_func, progress=91)
    shutil.rmtree(output_dir, ignore_errors=True)
    log_func and log_func("🧹 Cleanup complete — only results folder kept.")
    log_step_end("Step 5: Cleanup", step5_start, log_func, progress=100)

    return final_output_file


# --- For command-line testing ---
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Refine large MySQL dump files (improved FK ordering)")
    parser.add_argument("input_file", help="Path to SQL dump file")
    parser.add_argument("--output", default="results", help="Output folder for refined SQL")
    args = parser.parse_args()
    out = refine_sql(args.input_file, log_func=print, output_folder=args.output)
    print("Output:", out)
