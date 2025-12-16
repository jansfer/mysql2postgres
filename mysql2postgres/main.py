import argparse
import configparser
import sys
import time
import mysql.connector
import psycopg2
from psycopg2 import extras

def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description="Migrate data from MySQL to PostgreSQL.")
    parser.add_argument('--config', type=str, default='config.ini', help='Path to the configuration file.')
    parser.add_argument('--chunk-size', type=int, default=1000, help='Number of records to migrate at a time.')
    parser.add_argument('--recreate', action='store_true', help='Recreate all target tables even if they exist.')
    parser.add_argument('--truncate', action='store_true', help='Truncate target tables if they exist before migration.')
    return parser.parse_args()

def load_config(config_path):
    """Loads database configuration from an INI file."""
    config = configparser.ConfigParser()
    if not config.read(config_path):
        print(f"Error: Configuration file '{config_path}' not found or is empty.")
        sys.exit(1)
    return config

def connect_mysql(config):
    """Establishes a connection to the MySQL database."""
    try:
        return mysql.connector.connect(
            host=config['host'],
            user=config['user'],
            password=config['password'],
            database=config['database'],
            port=int(config.get('port', 3306))
        )
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        sys.exit(1)

def connect_postgres(config):
    """Establishes a connection to the PostgreSQL database."""
    try:
        return psycopg2.connect(
            host=config['host'],
            user=config['user'],
            password=config['password'],
            dbname=config['database'],
            port=config.get('port', 5432)
        )
    except psycopg2.Error as err:
        print(f"Error connecting to PostgreSQL: {err}")
        sys.exit(1)

def get_mysql_tables(my_conn):
    """Gets a list of all tables from the MySQL database."""
    with my_conn.cursor() as my_cursor:
        my_cursor.execute("SHOW TABLES")
        return [row[0] for row in my_cursor.fetchall()]

def get_postgres_tables(pg_conn):
    """Gets a list of all tables from the public schema in PostgreSQL."""
    with pg_conn.cursor() as pg_cursor:
        pg_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        return [row[0] for row in pg_cursor.fetchall()]

def map_mysql_to_postgres_type(mysql_type):
    """
    Maps MySQL data types to PostgreSQL data types.
    This is a simplified mapping and might need adjustments for specific needs.
    """
    mysql_type_lower = mysql_type.lower()
    if 'int' in mysql_type_lower:
        return 'INTEGER'
    if 'varchar' in mysql_type_lower:
        return mysql_type.replace('varchar', 'VARCHAR')
    if 'char' in mysql_type_lower:
        return mysql_type.replace('char', 'CHAR')
    if 'text' in mysql_type_lower or 'longtext' in mysql_type_lower:
        return 'TEXT'
    if 'datetime' in mysql_type_lower or 'timestamp' in mysql_type_lower:
        return 'TIMESTAMP'
    if 'date' in mysql_type_lower:
        return 'DATE'
    if 'decimal' in mysql_type_lower:
        return mysql_type.upper() # e.g., DECIMAL(10, 2)
    if 'float' in mysql_type_lower:
        return 'REAL'
    if 'double' in mysql_type_lower:
        return 'DOUBLE PRECISION'
    if 'blob' in mysql_type_lower or 'binary' in mysql_type_lower:
        return 'BYTEA'
    
    print(f"Warning: Unsupported MySQL type '{mysql_type}'. Defaulting to TEXT.")
    return 'TEXT'


def format_time(seconds):
    """Formats seconds into a human-readable string (MM:SS)."""
    if seconds is None or seconds < 0:
        return "N/A"
    minutes = int(seconds // 60)
    seconds = int(seconds % 60)
    return f"{minutes:02d}m {seconds:02d}s"

def migrate_table(table_name, my_conn, pg_conn, chunk_size, recreate, truncate, current_index, total_tables):
    """Migrates a single table from MySQL to PostgreSQL, including indexes."""
    print(f"\n----- Processing table: {table_name} ({current_index}/{total_tables}) -----")
    with my_conn.cursor() as my_cursor, pg_conn.cursor() as pg_cursor:
        # 1. Get MySQL table schema and find primary key
        my_cursor.execute(f"DESCRIBE `{table_name}`")
        columns_schema = my_cursor.fetchall()
        
        primary_key_column = None
        pk_candidates = []
        for col in columns_schema:
            if col[3] in (b'PRI', 'PRI'):
                pk_candidates.append(col[0])
        
        if len(pk_candidates) == 1:
            primary_key_column = pk_candidates[0]

        # 2. Handle table existence in PostgreSQL
        pg_cursor.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
        table_exists = pg_cursor.fetchone()[0] is not None

        if recreate and table_exists:
            print(f"Dropping table '{table_name}' in PostgreSQL as per --recreate flag.")
            pg_cursor.execute(f'DROP TABLE "{table_name}" CASCADE')
            table_exists = False
        
        if not table_exists:
            print(f"Creating table '{table_name}' in PostgreSQL.")
            column_defs = []
            for col in columns_schema:
                col_name = col[0]
                col_type = col[1].decode('utf-8') if isinstance(col[1], bytearray) else col[1]
                pg_type = map_mysql_to_postgres_type(col_type)
                nullable = "NULL" if col[2] == 'YES' else "NOT NULL"
                
                pk_constraint = ""
                if col_name == primary_key_column:
                    pk_constraint = " PRIMARY KEY"

                column_defs.append(f'"{col_name}" {pg_type} {nullable}{pk_constraint}')
            
            create_sql = f'CREATE TABLE "{table_name}" ({", ".join(column_defs)})'
            pg_cursor.execute(create_sql)
            print("Table created successfully.")

            # --- Migrate Indexes ---
            print("Migrating indexes...")
            my_cursor.execute(f"SHOW INDEX FROM `{table_name}`")
            mysql_indexes = my_cursor.fetchall()
            
            indexes_to_create = {}
            for index_row in mysql_indexes:
                key_name = index_row[2]
                if key_name == 'PRIMARY':
                    continue  # Already handled

                col_name = index_row[4]
                non_unique = index_row[1]
                
                if key_name not in indexes_to_create:
                    indexes_to_create[key_name] = {'columns': [], 'non_unique': bool(non_unique)}
                
                # Store columns in their correct order
                seq_in_index = index_row[3]
                indexes_to_create[key_name]['columns'].append((seq_in_index, col_name))

            for index_name, index_data in indexes_to_create.items():
                # Sort columns by their sequence in the index
                sorted_columns = [col[1] for col in sorted(index_data['columns'])]
                columns_sql = '", "'.join(sorted_columns)
                
                unique_str = "" if index_data['non_unique'] else "UNIQUE "
                # Using "IF NOT EXISTS" for safety (requires PostgreSQL 9.5+)
                index_sql = f'CREATE {unique_str}INDEX IF NOT EXISTS "{index_name}" ON "{table_name}" ("{columns_sql}")'
                
                print(f"  - Creating index '{index_name}' on column(s): {', '.join(sorted_columns)}")
                pg_cursor.execute(index_sql)
            # --- End Index Migration ---

        elif truncate:
            print(f"Truncating table '{table_name}' in PostgreSQL as per --truncate flag.")
            pg_cursor.execute(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY CASCADE')

        # 4. Migrate data in chunks
        my_cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        total_rows = my_cursor.fetchone()[0]
        
        if total_rows == 0:
            print("Table is empty. No data to migrate.")
            return

        print(f"Starting data migration for {total_rows} records...")
        migrated_rows = 0
        time_remaining_str = "Calculating..."

        column_names = [col[0] for col in columns_schema]
        insert_sql = f'INSERT INTO "{table_name}" ({", ".join([f'"{c}"' for c in column_names])}) VALUES %s'
        
        # --- Choose Pagination Strategy ---
        if primary_key_column:
            print(f"Using efficient keyset pagination on primary key: '{primary_key_column}'")
            last_id = 0
            while True:
                start_time = time.time()
                query = f"SELECT * FROM `{table_name}` WHERE `{primary_key_column}` > %s ORDER BY `{primary_key_column}` ASC LIMIT %s"
                my_cursor.execute(query, (last_id, chunk_size))
                rows_chunk = my_cursor.fetchall()

                if not rows_chunk:
                    break
                
                sanitized_rows = [tuple(c.replace('\x00', '') if isinstance(c, str) else c for c in row) for row in rows_chunk]
                extras.execute_values(pg_cursor, insert_sql, sanitized_rows)
                
                chunk_time = time.time() - start_time
                migrated_rows += len(rows_chunk)
                last_id = rows_chunk[-1][column_names.index(primary_key_column)]

                if chunk_time > 0:
                    rows_per_second = len(rows_chunk) / chunk_time
                    rows_remaining = total_rows - migrated_rows
                    if rows_per_second > 0:
                        time_remaining_seconds = rows_remaining / rows_per_second
                        time_remaining_str = format_time(time_remaining_seconds)
                    else:
                        time_remaining_str = "Infinite"
                
                progress = (migrated_rows / total_rows) * 100
                sys.stdout.write(f"\rProgress: {migrated_rows}/{total_rows} ({progress:.2f}%) | ETR: {time_remaining_str}   ")
                sys.stdout.flush()

        else:
            if len(pk_candidates) > 1:
                print("Warning: Composite primary key detected. Falling back to less efficient OFFSET pagination.")
            else:
                print("Warning: No single primary key found. Falling back to less efficient OFFSET pagination.")
            
            offset = 0
            while offset < total_rows:
                start_time = time.time()
                my_cursor.execute(f"SELECT * FROM `{table_name}` LIMIT %s OFFSET %s", (chunk_size, offset))
                rows_chunk = my_cursor.fetchall()
                if not rows_chunk:
                    break
                
                sanitized_rows = [tuple(c.replace('\x00', '') if isinstance(c, str) else c for c in row) for row in rows_chunk]
                extras.execute_values(pg_cursor, insert_sql, sanitized_rows)
                
                chunk_time = time.time() - start_time
                migrated_rows += len(rows_chunk)
                offset += chunk_size
                
                if chunk_time > 0:
                    rows_per_second = len(rows_chunk) / chunk_time
                    rows_remaining = total_rows - migrated_rows
                    if rows_per_second > 0:
                        time_remaining_seconds = rows_remaining / rows_per_second
                        time_remaining_str = format_time(time_remaining_seconds)
                    else:
                        time_remaining_str = "Infinite"

                progress = (migrated_rows / total_rows) * 100
                sys.stdout.write(f"\rProgress: {migrated_rows}/{total_rows} ({progress:.2f}%) | ETR: {time_remaining_str}   ")
                sys.stdout.flush()

        print("\nData migration completed for this table.")
        pg_conn.commit()


def main():
    """Main function to orchestrate the migration process."""
    overall_start_time = time.time()
    args = parse_arguments()
    config = load_config(args.config)
    
    my_conn = None
    pg_conn = None
    
    try:
        my_conn = connect_mysql(config['mysql'])
        pg_conn = connect_postgres(config['postgresql'])
        
        source_tables = get_mysql_tables(my_conn)
        target_tables = get_postgres_tables(pg_conn)
        
        print("--- Database Schema Comparison ---")
        print(f"Found {len(source_tables)} tables in the source MySQL database.")
        
        existing_in_target = []
        missing_in_target = []

        for table in source_tables:
            if table in target_tables:
                existing_in_target.append(table)
            else:
                missing_in_target.append(table)
        
        if existing_in_target:
            print("\nTables that already exist in the target PostgreSQL database:")
            for table in existing_in_target:
                print(f"  - {table}")
        
        if missing_in_target:
            print("\nTables that are missing in the target PostgreSQL database:")
            for table in missing_in_target:
                print(f"  - {table}")
        print("----------------------------------")

        # --- Add Confirmation Step ---
        print("\n--- Summary of Planned Actions ---")
        actions = []
        if missing_in_target:
            actions.append(f"  - CREATE {len(missing_in_target)} new table(s) in the target database.")
        
        if args.recreate and existing_in_target:
            actions.append(f"  - DROP and RECREATE {len(existing_in_target)} existing table(s) in the target database.")
        elif args.truncate and existing_in_target:
            actions.append(f"  - TRUNCATE {len(existing_in_target)} existing table(s) in the target database.")
        
        if not source_tables:
            print("No source tables found. Nothing to do.")
            return

        if not actions:
            print("No schema changes required. Data will be migrated into existing tables.")
        else:
            for action in actions:
                print(action)

        print(f"  - MIGRATE data for up to {len(source_tables)} table(s).")
        
        if args.recreate:
            print("\n\033[91mWARNING: The --recreate flag will cause IRREVERSIBLE DATA LOSS in target tables.\033[0m")
        elif args.truncate:
            print("\n\033[93mWARNING: The --truncate flag will delete all data from existing target tables.\033[0m")

        try:
            confirm = input("\n> Do you want to proceed? (y/n): ")
        except KeyboardInterrupt:
            print("\nOperation cancelled by user.")
            sys.exit(1)

        if confirm.lower() != 'y':
            print("Operation cancelled by user.")
            sys.exit(0)
        
        print("----------------------------------")
        # --- End Confirmation Step ---

        total_tables = len(source_tables)
        for i, table_name in enumerate(source_tables, 1):
            migrate_table(
                table_name, 
                my_conn, 
                pg_conn, 
                args.chunk_size,
                args.recreate,
                args.truncate,
                current_index=i,
                total_tables=total_tables
            )
        print("\nMigration finished for all tables.")
    except (mysql.connector.Error, psycopg2.Error) as err:
        print(f"\nAn error occurred during migration: {err}")
        if pg_conn:
            pg_conn.rollback()
    finally:
        if my_conn:
            my_conn.close()
        if pg_conn:
            pg_conn.close()
        print("Database connections closed.")
        
        overall_end_time = time.time()
        total_duration = overall_end_time - overall_start_time
        print(f"Total execution time: {format_time(total_duration)}")

if __name__ == "__main__":
    main()
