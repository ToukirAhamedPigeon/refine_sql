# SQL Refinement Project

This Python project processes raw SQL files to:

1. Extract `CREATE TABLE` statements and generate metadata.
2. Extract and clean `INSERT` statements.
3. Refine values (truncate `VARCHAR`, enforce `ENUM`, fill missing defaults).
4. Concatenate all SQL chunks into a single final `.sql` file.
5. Cleanup all temporary files, keeping only the final output.

---

## Project Structure

├── venv/ # Python virtual environment
├── sqls/ # Input raw SQL files
├── chunks/ # Temporary SQL chunks (auto-deleted)
├── results/ # Final refined SQL file
├── refine.py # Main Python script
├── .gitignore
├── requirements.txt
└── README.md

---

## Setup

1. Create virtual environment:

```bash
python -m venv venv
source venv/bin/activate   # Linux/macOS
venv\Scripts\activate      # Windows


2. Install dependencies (if any):

pip install -r requirements.txt


3. Place your raw SQL file in sqls/ (e.g., building_database.sql).


4. Run the script:

python refine.py


5. The final refined SQL will be in:

Temporary chunk files will be automatically deleted.


Notes

- Views or objects starting with v_ are commented out in the final SQL.

- The script automatically handles:

    - Truncating oversized VARCHARs.

    - Filling defaults for missing values.

    - Sanitizing control characters.

    - Handling ENUM restrictions.


- Only the final .sql file is kept after execution.
