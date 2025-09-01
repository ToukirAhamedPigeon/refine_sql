import os
import re
import json
from pathlib import Path

# --- Configuration ---
input_file = 'chunks/building_database_temp.sql'
output_file = 'chunks/building_database_temp_refined.sql'
json_index_file = 'chunks/building_database_indexes.json'
final_output_dir = 'results'
Path(final_output_dir).mkdir(parents=True, exist_ok=True)
final_output_file = os.path.join(final_output_dir, 'building_database.sql')

create_file = 'chunks/building_database_0.sql'
extra_file = 'chunks/building_database_temp_extra.sql'


# --- Load metadata ---
with open(json_index_file, 'r', encoding='utf-8') as f:
    table_metadata = json.load(f)


def get_fallback(dtype, enum_values=None):
    """Fallback defaults when empty values are not allowed."""
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
    """Remove control characters but keep escape sequences intact."""
    return re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', inner)


def truncate_value(value, col):
    """Truncate oversized VARCHAR and enforce ENUM validity."""
    max_len = col.get('varchar_length')
    dtype = col.get('type', '')
    enum_values = col.get('enum_values')

    # Remove quotes for processing
    is_quoted = (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"'))
    if is_quoted:
        inner = value[1:-1]
        inner = sanitize_value(inner)

        if enum_values:
            if inner not in enum_values:
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
    """Fix a single INSERT row based on metadata."""
    row = row_line.strip()
    if not row.startswith('('):
        return row_line

    ends_with_comma = row.endswith(',') or row.endswith(',\n')
    ends_with_semicolon = row.endswith(');')

    if ends_with_comma:
        row = row[:-1]
    elif ends_with_semicolon:
        row = row[:-2]

    row = row.strip()
    if row.startswith('('):
        row = row[1:]
    if row.endswith(')'):
        row = row[:-1]

    # --- Split row respecting quotes ---
    parts = []
    current = ''
    in_quotes = False
    escape = False
    quote_char = ''

    for char in row:
        if escape:
            current += "\\" + char
            escape = False
        elif char == '\\':
            escape = True
        elif char in ["'", '"']:
            current += char
            if not in_quotes:
                in_quotes = True
                quote_char = char
            elif char == quote_char:
                in_quotes = False
        elif char == ',' and not in_quotes:
            parts.append(current.strip())
            current = ''
        else:
            current += char
    parts.append(current.strip())

    # --- Fix each value ---
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
    if ends_with_comma:
        fixed_row += '),'
    elif ends_with_semicolon:
        fixed_row += ');'
    else:
        fixed_row += ')'

    return fixed_row + '\n'


# --- Main loop: fix INSERT rows ---
with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8') as outfile:
    current_table = None
    current_columns = []
    row_buffer = []
    accumulating_row = False

    for line in infile:
        stripped = line.strip()

        # Detect new INSERT
        if stripped.lower().startswith('insert into'):
            # Flush previous buffered row
            if row_buffer:
                full_row = ''.join(row_buffer)
                fixed = fix_row(full_row, current_columns)
                outfile.write(fixed)
                row_buffer = []
                accumulating_row = False

            match = re.match(r'insert into [`"]?(\w+)[`"]?\s+values', stripped, re.IGNORECASE)
            if match:
                current_table = match.group(1)
                current_columns = table_metadata.get(current_table, [])
            outfile.write(line)
        elif stripped.startswith('(') or accumulating_row:
            row_buffer.append(line)
            accumulating_row = True
            if stripped.endswith('),') or stripped.endswith(');'):
                full_row = ''.join(row_buffer)
                fixed = fix_row(full_row, current_columns)
                outfile.write(fixed)
                row_buffer = []
                accumulating_row = False
        else:
            outfile.write(line)

    # Flush any remaining buffered row
    if row_buffer:
        full_row = ''.join(row_buffer)
        fixed = fix_row(full_row, current_columns)
        outfile.write(fixed)


# --- Step 3: Concatenate all chunks into final SQL ---
with open(final_output_file, 'w', encoding='utf-8') as fout:
    for fpath in [create_file, output_file, extra_file]:
        if os.path.exists(fpath):
            with open(fpath, 'r', encoding='utf-8') as fin:
                fout.write(f"-- Start of {os.path.basename(fpath)}\n")
                fout.write(fin.read())
                fout.write(f"\n-- End of {os.path.basename(fpath)}\n\n")

print(f"✅ Refined {input_file} → {output_file}")
print(f"✅ Final concatenated SQL → {final_output_file}")
