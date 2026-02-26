# CSV to PostgreSQL Importer

A simple C program to parse a CSV file and insert its contents into a PostgreSQL database using `libpq`. Designed to handle large CSV files, with support for Windows and Unix line endings and Unicode text.

---

## Features

- Reads CSV files row by row.
- Parses CSV fields into separate columns.
- Automatically creates a target table (`Telenor_404k`) if it doesn’t exist.
- Inserts each row into PostgreSQL using parameterized queries (`PQexecParams`) to avoid SQL injection.
- Supports environment-based database configuration.

---

## Requirements

- C compiler (gcc recommended)
- PostgreSQL server
- `libpq-dev` library
- Bash shell (for setup scripts)

---

## Setup

1. Install PostgreSQL client libraries:

```bash
./setup.sh
```
2. Ensure your PostgreSQL database exists and you have credentials ready.
3. Set environment variables for database connection:

``` bash
export DB_NAME=<your_database_name>
export DB_USER=<your_db_user>
export DB_PASS=<your_db_password>
export DB_HOST=<your_db_host>
```


