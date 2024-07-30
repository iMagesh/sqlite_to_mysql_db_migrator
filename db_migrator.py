import sqlite3
import mysql.connector
import re
from mysql.connector import Error


def connect_sqlite(db_file):
    try:
        return sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(f"Error connecting to SQLite database: {e}")
        return None


def connect_mysql(host, user, password, database):
    try:
        return mysql.connector.connect(host=host, user=user, password=password, database=database)
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None


def get_sqlite_schema(sqlite_conn):
    cursor = sqlite_conn.cursor()
    cursor.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    return cursor.fetchall()


def sqlite_to_mysql_type(sqlite_type):
    type_mapping = {
        'INTEGER': 'INT',
        'REAL': 'DOUBLE',
        'TEXT': 'TEXT',
        'BLOB': 'BLOB',
        'BOOLEAN': 'TINYINT(1)',
        'DATETIME': 'DATETIME',
        'DATE': 'DATE',
        'TIME': 'TIME'
    }
    for sqlite, mysql in type_mapping.items():
        if sqlite in sqlite_type.upper():
            return mysql
    return sqlite_type


def convert_create_table_statement(sqlite_statement):
    # Ensure there's a space after CREATE TABLE
    mysql_statement = re.sub(
        r'CREATE TABLE', 'CREATE TABLE ', sqlite_statement, flags=re.IGNORECASE)

    # Remove SQLite-specific clauses and ensure spacing around AUTO_INCREMENT
    mysql_statement = re.sub(r'\s*AUTOINCREMENT\s*', r' AUTO_INCREMENT ',
                             mysql_statement.strip(), flags=re.IGNORECASE)

    # Convert data types
    def replace_type(match):
        column_name = match.group(1)
        data_type = sqlite_to_mysql_type(match.group(2))
        constraints = match.group(3) or ''

        # Handle NOT NULL constraint
        if 'not null' in constraints.lower():
            constraints = constraints.replace('not null', 'NOT NULL ')
            constraints = constraints.replace('NOT NULL NOT NULL', 'NOT NULL ')

        # Handle PRIMARY KEY
        if 'primary key' in constraints.lower():
            constraints = constraints.replace('primary key', 'PRIMARY KEY ')

        return f"{column_name} {data_type} {constraints}"

    mysql_statement = re.sub(
        r'(`?\w+`?)\s+(\w+(?:\(\d+\))?)\s*((?:(?:NOT)?\s*NULL|(?:PRIMARY)?\s*KEY|DEFAULT\s*[^,]+)?)',
        replace_type, mysql_statement, flags=re.IGNORECASE)

    # Ensure a space before AUTO_INCREMENT if following PRIMARY KEY
    mysql_statement = re.sub(r'PRIMARY KEY\s*AUTO_INCREMENT',
                             'PRIMARY KEY AUTO_INCREMENT', mysql_statement, flags=re.IGNORECASE)

    # Replace double quotes with backticks
    mysql_statement = mysql_statement.replace('"', '`')

    # Extract and remove foreign key constraints
    foreign_keys = re.findall(
        r',\s*(FOREIGN KEY\s*\([^)]+\)\s*REFERENCES\s*[^)]+\))', mysql_statement, flags=re.IGNORECASE)
    mysql_statement = re.sub(
        r',\s*FOREIGN KEY\s*\([^)]+\)\s*REFERENCES\s*[^)]+\)', '', mysql_statement, flags=re.IGNORECASE)

    # Remove any remaining SQLite-specific constraints
    mysql_statement = re.sub(
        r'CONSTRAINT .*? (UNIQUE|PRIMARY KEY) \(.*?\)', '', mysql_statement, flags=re.IGNORECASE)

    # Ensure there's a space before the opening parenthesis
    mysql_statement = re.sub(r'`\(', '` (', mysql_statement)

    return mysql_statement.strip(), foreign_keys


def get_foreign_keys(sqlite_conn, table_name):
    cursor = sqlite_conn.cursor()
    cursor.execute(f"PRAGMA foreign_key_list('{table_name}')")
    return cursor.fetchall()


def create_mysql_schema(mysql_conn, sqlite_conn, tables):
    cursor = mysql_conn.cursor()
    foreign_keys_info = {}

    for table_name, create_statement in tables:
        print(f"\nProcessing table: {table_name}")
        print("Original SQLite statement:")
        print(create_statement)
        print()

        mysql_create_statement, foreign_keys = convert_create_table_statement(
            create_statement)
        print("Converted MySQL statement:")
        print(mysql_create_statement)
        print()

        try:
            cursor.execute(mysql_create_statement)
            print(f"Table '{table_name}' created successfully in MySQL")

            # Collect foreign key information
            if foreign_keys:
                foreign_keys_info[table_name] = foreign_keys
        except mysql.connector.Error as e:
            print(f"Error creating table '{table_name}' in MySQL:")
            print(f"Error code: {e.errno}")
            print(f"Error message: {e.msg}")
            print(f"Problematic SQL: {mysql_create_statement}")
            mysql_conn.rollback()
            return False  # Stop processing on first error

    # Now add the foreign keys
    for table_name, foreign_keys in foreign_keys_info.items():
        for fk in foreign_keys:
            fk_sql = f"ALTER TABLE `{table_name}` ADD {fk}"
            print(f"Adding foreign key for table '{table_name}': {fk_sql}")
            try:
                cursor.execute(fk_sql)
            except mysql.connector.Error as e:
                print(f"Error adding foreign key for table "
                      f"'{table_name}': {e}")
                mysql_conn.rollback()
                return False

    mysql_conn.commit()
    return True  # All tables created successfully


def get_table_columns(conn, table_name, is_sqlite):
    cursor = conn.cursor()
    if is_sqlite:
        cursor.execute(f"PRAGMA table_info('{table_name}')")
        return [info[1] for info in cursor.fetchall()]
    else:
        cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
        return [column[0] for column in cursor.fetchall()]


def transfer_data(sqlite_conn, mysql_conn, tables):
    sqlite_cursor = sqlite_conn.cursor()
    mysql_cursor = mysql_conn.cursor()

    for table_name, _ in tables:
        sqlite_columns = get_table_columns(sqlite_conn, table_name, True)
        mysql_columns = get_table_columns(mysql_conn, table_name, False)

        common_columns = list(set(sqlite_columns) & set(mysql_columns))

        sqlite_cursor.execute(
            f"SELECT {', '.join(common_columns)} FROM {table_name}")
        rows = sqlite_cursor.fetchall()

        if rows:
            placeholders = ', '.join(['%s'] * len(common_columns))
            columns = ', '.join(f'`{col}`' for col in common_columns)
            insert_query = (
                f"INSERT INTO `{table_name}` ({columns}) "
                f"VALUES ({placeholders})"
            )

            try:
                mysql_cursor.executemany(insert_query, rows)
                mysql_conn.commit()
                print(
                    f"Transferred {len(rows)} rows to table "
                    f"'{table_name}' in MySQL"
                )
            except Error as e:
                print(f"Error inserting data into '{table_name}': {e}")
                mysql_conn.rollback()


def compare_databases(sqlite_conn, mysql_conn, tables):
    sqlite_cursor = sqlite_conn.cursor()
    mysql_cursor = mysql_conn.cursor()

    for table_name, _ in tables:
        sqlite_columns = get_table_columns(sqlite_conn, table_name, True)
        mysql_columns = get_table_columns(mysql_conn, table_name, False)

        common_columns = list(set(sqlite_columns) & set(mysql_columns))
        columns_str = ', '.join(common_columns)

        sqlite_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        mysql_cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")

        sqlite_count = sqlite_cursor.fetchone()[0]
        mysql_count = mysql_cursor.fetchone()[0]

        print(f"Table '{table_name}':")
        print(f"  Row count - SQLite: {sqlite_count}, MySQL: {mysql_count}")

        if sqlite_count == mysql_count:
            sqlite_cursor.execute(f"SELECT {columns_str} FROM "
                                  f" {table_name} ORDER BY 1")
            mysql_cursor.execute(f"SELECT {columns_str} FROM "
                                 f"`{table_name}` ORDER BY 1")

            sqlite_data = sqlite_cursor.fetchall()
            mysql_data = mysql_cursor.fetchall()

            if sqlite_data == mysql_data:
                print("  Data: Identical")
            else:
                print("  Data: Mismatch detected")
        else:
            print("  Data: Row count mismatch")
        print()


def main():
    sqlite_db = "./data-20240703190001.db"
    mysql_host = "localhost"
    mysql_user = "strapi"
    mysql_password = "strapi"
    mysql_database = "strapi"

    sqlite_conn = connect_sqlite(sqlite_db)
    mysql_conn = connect_mysql(
        mysql_host, mysql_user, mysql_password, mysql_database)

    if sqlite_conn and mysql_conn:
        tables = get_sqlite_schema(sqlite_conn)
        if create_mysql_schema(mysql_conn, sqlite_conn, tables):
            transfer_data(sqlite_conn, mysql_conn, tables)
            compare_databases(sqlite_conn, mysql_conn, tables)

        sqlite_conn.close()
        mysql_conn.close()
    else:
        print("Failed to connect to one or both databases.")


if __name__ == "__main__":
    main()
