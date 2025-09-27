# RefineSQLApp

**Version:** 1.0.1
**Status:** Open Source (Windows executable available)

## Overview

Large SQL dumps from legacy MySQL/MariaDB and even modern MySQL databases often contain errors that prevent smooth imports. These errors may include:

* Foreign key dependency issues
* Empty strings in numeric/date fields
* Nullability conflicts
* Oversized `VARCHAR` values
* Inconsistent quoting (`'` vs `"`)
* Newline and escape character issues
* Triggers, views, and stored procedures breaking import order

**RefineSQLApp** automatically analyzes, restructures, and cleans these SQL files so they can be executed with minimal errors, protecting against huge data loss.

## How It Works (Procedures)

1. **Splitting Phase**

   * Separates `CREATE TABLE`, `INSERT INTO`, and views/triggers into dedicated files.

2. **Dependency Resolution**

   * Moves foreign-key-dependent tables down in execution order.
   * Handles circular dependencies by shifting foreign key constraints into `ALTER TABLE` statements executed later.

3. **Schema Analysis**

   * Extracts column metadata from `CREATE TABLE` statements into a JSON file.
   * Identifies nullable columns, default values, and length constraints.

4. **Data Refinement**

   * Reads `INSERT INTO` statements and checks values against schema rules.
   * Fixes errors such as:

     * Invalid numeric/date values replaced with `NULL` when allowed
     * Oversized strings truncated to column max length
     * Empty strings corrected for proper type matching
     * Quote and newline inconsistencies normalized

5. **Rebuild & Output**

   * Produces a clean, import-ready SQL file inside the `results/` directory.
   * Generates logs of refinements performed.

## Usage

1. Download and run the provided `.exe` file (no setup required).
2. Upload your raw/dumped SQL file into the app.
3. The app will automatically start refining and show logs in real time.
4. After completion, the refined SQL file will be saved in the `results/` folder located in the same directory as the `.exe`.
5. You can manage older refined SQLs (delete/copy/move) directly within the app.

## Performance & Testing

* Tested on medium to very large SQL dumps (up to 1GB, ~3M lines).
* Successfully reduced one file from **10,000+ errors** to just **3–4 errors**.
* Works across both modern and legacy MySQL/MariaDB databases.

## Roadmap

* Enhanced UI/UX
* Cross-platform builds (Linux/macOS)
* Expanded error-handling coverage
* Community feedback-based improvements

## Downloads

* [GitHub Repository](#)
* [Direct Executable Link](#)

## License

Open source – free to use and improve. Contributions welcome.
