import argparse
import configparser
import sys
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


def migrate_table(table_name, my_conn, pg_conn, chunk_size, recreate, truncate):
    """Migrates a single table from MySQL to PostgreSQL."""
    print(f"\n----- Processing table: {table_name} ----- ")
    with my_conn.cursor() as my_cursor, pg_conn.cursor() as pg_cursor:
        # 1. Get MySQL table schema
        my_cursor.execute(f"DESCRIBE `{table_name}`")
        columns_schema = my_cursor.fetchall()

        # 2. Handle table existence in PostgreSQL
        pg_cursor.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
        table_exists = pg_cursor.fetchone()[0] is not None

        if recreate and table_exists:
            print(f"Dropping table '{table_name}' in PostgreSQL as per --recreate flag.")
            pg_cursor.execute(f'DROP TABLE "{table_name}" CASCADE')
            table_exists = False
        
        # 3. Create table in PostgreSQL if it doesn't exist
        if not table_exists:
            print(f"Creating table '{table_name}' in PostgreSQL.")
            column_defs = []
            for col in columns_schema:
                col_name = col[0]
                col_type = col[1].decode('utf-8') if isinstance(col[1], bytearray) else col[1]
                pg_type = map_mysql_to_postgres_type(col_type)
                nullable = "NULL" if col[2] == 'YES' else "NOT NULL"
                column_defs.append(f'"{col_name}" {pg_type} {nullable}')
            
            create_sql = f'CREATE TABLE "{table_name}" ({', '.join(column_defs)})'
            pg_cursor.execute(create_sql)
            print("Table created successfully.")
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
        offset = 0

        column_names = [col[0] for col in columns_schema]
        insert_sql = f'INSERT INTO "{table_name}" ({', '.join([f'"{c}"' for c in column_names])}) VALUES %s'

        while offset < total_rows:
            my_cursor.execute(f"SELECT * FROM `{table_name}` LIMIT %s OFFSET %s", (chunk_size, offset))
            rows_chunk = my_cursor.fetchall()
            if not rows_chunk:
                break
            
            extras.execute_values(pg_cursor, insert_sql, rows_chunk)

            migrated_rows += len(rows_chunk)
            offset += chunk_size
            progress = (migrated_rows / total_rows) * 100
            sys.stdout.write(f"\rProgress: {migrated_rows}/{total_rows} records migrated ({progress:.2f}%)")
            sys.stdout.flush()

        print("\nData migration completed for this table.")
        pg_conn.commit()


def main():
    """Main function to orchestrate the migration process."""
    args = parse_arguments()
    config = load_config(args.config)
    
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

    try:
        for table_name in source_tables:
            migrate_table(
                table_name, 
                my_conn, 
                pg_conn, 
                args.chunk_size,
                args.recreate,
                args.truncate
            )
        print("\nMigration finished for all tables.")
    except (mysql.connector.Error, psycopg2.Error) as err:
        print(f"\nAn error occurred during migration: {err}")
        pg_conn.rollback()
    finally:
        my_conn.close()
        pg_conn.close()
        print("Database connections closed.")

if __name__ == "__main__":
    main()
