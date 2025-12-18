# MySQL to PostgreSQL Migration Tool

A command-line utility developed in Python to facilitate the migration of database schemas and data from MySQL to PostgreSQL.

## Features

- **Schema Comparison**: Compares the source (MySQL) and target (PostgreSQL) databases and lists which tables exist or are missing in the target.
- **Table Creation**: Automatically generates PostgreSQL `CREATE TABLE` statements based on the MySQL table structure, mapping data types.
- **Flexible Migration Control**:
    - `--recreate`: Drop and recreate tables in the target database even if they already exist.
    - `--truncate`: Truncate (empty) existing tables in the target database before migrating data.
- **Chunk-based Data Migration**: Transfers data in configurable chunks (`--chunk-size`) to handle large tables efficiently.
- **Live Progress Display**: Shows real-time progress of data migration for each table, including the number of records transferred.
- **Configuration File**: Database credentials and connection details are managed externally in a `config.ini` file, not hardcoded.
- **Python `uv` Environment**: Uses `uv` for fast and straightforward Python environment and package management.

## Prerequisites

- Python 3.8+
- [uv](https://github.com/astral-sh/uv) (for environment management, though `pip` can also be used)

## Setup and Installation

1.  **Clone the Repository** (or use the files already created):
    ```bash
    # git clone <repository_url>
    cd mysql2postgres
    ```

2.  **Create a Virtual Environment**:
    Use `uv` to create an isolated Python environment.
    ```bash
    uv venv
    ```

3.  **Activate the Virtual Environment**:
    - On macOS/Linux:
      ```bash
      source .venv/bin/activate
      ```
    - On Windows:
      ```bash
      .venv\Scripts\activate
      ```

4.  **Install Dependencies**:
    Install the required Python packages from `requirements.txt`.
    ```bash
    uv pip install -r requirements.txt
    ```

## Configuration

Before running the tool, you must provide your database connection details.

1.  Rename or copy `config.ini.template` to `config.ini`.
2.  Edit `config.ini` with your specific database credentials:

    ```ini
    [mysql]
    host = localhost
    user = your_mysql_user
    password = your_mysql_password
    database = source_database_name
    port = 3306

    [postgresql]
    host = localhost
    user = your_postgres_user
    password = your_postgres_password
    database = target_database_name
    port = 5432
    ```

## Usage

Ensure your virtual environment is activated before running any commands. All commands should be run from the `mysql2postgres` directory.

### 1. Show Help
To see all available commands and options:
```bash
python main.py --help
```

### 2. Schema Check (Dry Run)
To compare the databases without performing any migration. This is the recommended first step to understand the current state.
```bash
python main.py
```
This will output a list of tables found in the source and indicate whether they exist in the target.

### 3. Start a Basic Migration
This command will migrate only the tables that are missing in the target database. It will not affect existing tables.
```bash
python main.py
```

### 4. Truncate and Migrate
This will empty any existing tables in the target database before migrating new data into them.
```bash
python main.py --truncate
```

### 5. Recreate All Tables
This will **DROP** all corresponding tables in the target database and recreate them from the MySQL schema before migration.

**Warning**: Use this command with caution, as it will destroy any existing data and schema in the target tables.
```bash
python main.py --recreate
```

### 6. Adjust Chunk Size
To control memory usage and migration speed, you can set the number of records to process in each batch.
```bash
python main.py --chunk-size 5000
```

## Data Type Mapping

The script includes a simplified function (`map_mysql_to_postgres_type`) to convert MySQL data types to their PostgreSQL equivalents. This mapping covers common types but may not handle all edge cases or custom data types perfectly. If you have a complex schema, you may need to adjust this function in `main.py`.

## License

This project is licensed under the MIT License.
