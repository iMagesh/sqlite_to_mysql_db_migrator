import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
from collections import defaultdict, deque
import re


def connect_sqlite(db_file):
    try:
        return sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(f"Error connecting to SQLite database: {e}")
        return None


def connect_mysql(host, user, password, database):
    try:
        engine = create_engine(
            f"mysql+pymysql://{user}:{password}@{host}/{database}")
        return engine
    except Exception as e:
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
    mysql_statement = re.sub(
        r'CREATE TABLE', 'CREATE TABLE ', sqlite_statement, flags=re.IGNORECASE)
    mysql_statement = re.sub(r'\s*AUTOINCREMENT\s*', r' AUTO_INCREMENT ',
                             mysql_statement.strip(), flags=re.IGNORECASE)

    def replace_type(match):
        column_name = match.group(1)
        data_type = sqlite_to_mysql_type(match.group(2))
        constraints = match.group(3) or ''

        if 'not null' in constraints.lower():
            constraints = constraints.replace('not null', 'NOT NULL ')
            constraints = constraints.replace('NOT NULL NOT NULL', 'NOT NULL ')

        if 'primary key' in constraints.lower():
            constraints = constraints.replace('primary key', 'PRIMARY KEY ')

        return f"{column_name} {data_type} {constraints}"

    mysql_statement = re.sub(
        r'(`?\w+`?)\s+(\w+(?:\(\d+\))?)\s*((?:(?:NOT)?\s*NULL|(?:PRIMARY)?\s*KEY|DEFAULT\s*[^,]+)?)',
        replace_type, mysql_statement, flags=re.IGNORECASE)

    mysql_statement = re.sub(r'PRIMARY KEY\s*AUTO_INCREMENT',
                             'PRIMARY KEY AUTO_INCREMENT', mysql_statement, flags=re.IGNORECASE)
    mysql_statement = mysql_statement.replace('"', '`')

    foreign_keys = re.findall(
        r',\s*(FOREIGN KEY\s*\([^)]+\)\s*REFERENCES\s*[^)]+\))', mysql_statement, flags=re.IGNORECASE)
    mysql_statement = re.sub(
        r',\s*FOREIGN KEY\s*\([^)]+\)\s*REFERENCES\s*[^)]+\)', '', mysql_statement, flags=re.IGNORECASE)

    mysql_statement = re.sub(
        r'CONSTRAINT .*? (UNIQUE|PRIMARY KEY) \(.*?\)', '', mysql_statement, flags=re.IGNORECASE)
    mysql_statement = re.sub(r'`\(', '` (', mysql_statement)

    return mysql_statement.strip(), foreign_keys


def get_foreign_keys(sqlite_conn, table_name):
    cursor = sqlite_conn.cursor()
    cursor.execute(f"PRAGMA foreign_key_list('{table_name}')")
    return cursor.fetchall()


def sort_tables_by_dependency(tables, sqlite_conn):
    dependency_graph = defaultdict(list)
    in_degree = {table_name: 0 for table_name, _ in tables}

    for table_name, _ in tables:
        foreign_keys = get_foreign_keys(sqlite_conn, table_name)
        for fk in foreign_keys:
            dependency_graph[fk[2]].append(table_name)
            in_degree[table_name] += 1

    queue = deque([table for table in in_degree if in_degree[table] == 0])
    sorted_tables = []

    while queue:
        table = queue.popleft()
        sorted_tables.append(table)
        for dependent in dependency_graph[table]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    table_dict = dict(tables)
    return [(table, table_dict[table]) for table in sorted_tables]


def create_mysql_schema(mysql_conn, sqlite_conn, tables):
    connection = mysql_conn.connect()
    foreign_keys_info = {}

    tables = sort_tables_by_dependency(tables, sqlite_conn)

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
            connection.execute(text(mysql_create_statement))
            print(f"Table '{table_name}' created successfully in MySQL")

            if foreign_keys:
                foreign_keys_info[table_name] = foreign_keys
        except Exception as e:
            print(f"Error creating table '{table_name}' in MySQL:")
            print(f"Error message: {e}")
            print(f"Problematic SQL: {mysql_create_statement}")
            connection.rollback()
            return False

    for table_name, foreign_keys in foreign_keys_info.items():
        for fk in foreign_keys:
            fk_sql = f"ALTER TABLE `{table_name}` ADD {fk}"
            print(f"Adding foreign key for table '{table_name}': {fk_sql}")
            try:
                connection.execute(text(fk_sql))
            except Exception as e:
                print(f"Error adding foreign key for table "
                      f"'{table_name}': {e}")
                connection.rollback()
                return False

    connection.commit()
    connection.close()
    return True


def transfer_data_with_pandas(sqlite_conn, mysql_conn, tables):
    engine = mysql_conn
    for table_name, _ in tables:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", sqlite_conn)
        df.to_sql(name=table_name, con=engine,
                  if_exists='append', index=False, method='multi')
        print(f"Transferred {len(df)} rows to table '{table_name}' in MySQL")


def normalize_type(data_type):
    type_mapping = {
        'INT': 'int',
        'INTEGER': 'int',
        'BIGINT': 'int',
        'SMALLINT': 'int',
        'TINYINT': 'int',
        'FLOAT': 'float',
        'DOUBLE': 'float',
        'REAL': 'float',
        'NUMERIC': 'float',
        'DECIMAL': 'float',
        'BOOLEAN': 'tinyint',
        'TEXT': 'text',
        'BLOB': 'blob',
        'DATETIME': 'datetime',
        'DATE': 'date',
        'TIME': 'time',
        'VARCHAR': 'text',
        'CHAR': 'text',
        'CLOB': 'text',
        'NVARCHAR': 'text',
        'NCHAR': 'text'
    }
    normalized = data_type.upper()
    for key, value in type_mapping.items():
        if key in normalized:
            return value
    return normalized


def compare_schemas(sqlite_conn, mysql_conn):
    sqlite_cursor = sqlite_conn.cursor()
    mysql_connection = mysql_conn.connect()

    sqlite_tables = {table[0] for table in sqlite_cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall()}
    mysql_tables = {row[0] for row in mysql_connection.execute(
        text("SHOW TABLES")).fetchall()}

    if sqlite_tables != mysql_tables:
        print("Schema mismatch: Tables in SQLite and MySQL do not match.")
        missing_in_mysql = sqlite_tables - mysql_tables
        missing_in_sqlite = mysql_tables - sqlite_tables
        if missing_in_mysql:
            print(f"Tables missing in MySQL: {missing_in_mysql}")
        if missing_in_sqlite:
            print(f"Tables missing in SQLite: {missing_in_sqlite}")
        return False

    for table in sqlite_tables:
        sqlite_schema = sqlite_cursor.execute(
            f"PRAGMA table_info({table})").fetchall()
        mysql_schema = mysql_connection.execute(
            text(f"DESCRIBE {table}")).fetchall()

        sqlite_columns = {(col[1], normalize_type(col[2]))
                          for col in sqlite_schema}
        mysql_columns = {(col[0], normalize_type(col[1]))
                         for col in mysql_schema}

        if sqlite_columns != mysql_columns:
            print(f"Schema mismatch: Columns in table '{table}' do not match.")
            missing_in_mysql = sqlite_columns - mysql_columns
            missing_in_sqlite = mysql_columns - sqlite_columns
            if missing_in_mysql:
                print(f"Columns missing in MySQL: {missing_in_mysql}")
            if missing_in_sqlite:
                print(f"Columns missing in SQLite: {missing_in_sqlite}")
            return False

    print("Schemas match.")
    return True


def compare_data(sqlite_conn, mysql_conn, tables):
    engine = mysql_conn
    for table_name, _ in tables:
        sqlite_df = pd.read_sql_query(
            f"SELECT * FROM {table_name}", sqlite_conn)
        mysql_df = pd.read_sql_table(table_name, con=engine)

        if not sqlite_df.equals(mysql_df):
            print(f"Data mismatch in table '{table_name}'.")
            sqlite_count = len(sqlite_df)
            mysql_count = len(mysql_df)
            print(f"Row count - SQLite: {sqlite_count}, MySQL: {mysql_count}")
            if sqlite_count != mysql_count:
                print(f"Row count mismatch in table '{table_name}'.")
            else:
                mismatched_rows = sqlite_df.compare(mysql_df)
                print(f"Mismatched rows in table '{table_name}':")
                print(mismatched_rows)
            return False

    print("Data matches for all tables.")
    return True


def main():
    sqlite_db = "./dev-strapi-database-march-292024.db"
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
            transfer_data_with_pandas(sqlite_conn, mysql_conn, tables)
            if compare_schemas(sqlite_conn, mysql_conn) and compare_data(sqlite_conn, mysql_conn, tables):
                print("Migration successful and validated.")
            else:
                print("Migration validation failed.")
        else:
            print("Schema creation failed.")

        sqlite_conn.close()
    else:
        print("Failed to connect to one or both databases.")


if __name__ == "__main__":
    main()
