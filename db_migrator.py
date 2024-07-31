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
            f"mysql+pymysql://{user}:{password}@{host}/{database}?charset=utf8mb4")
        return engine
    except Exception as e:
        print(f"Error connecting to MySQL database: {e}")
        return None


def get_sqlite_schema(sqlite_conn):
    cursor = sqlite_conn.cursor()
    cursor.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    return cursor.fetchall()


def get_sqlite_indexes(sqlite_conn, table_name):
    cursor = sqlite_conn.cursor()
    cursor.execute(f"PRAGMA index_list('{table_name}')")
    indexes = cursor.fetchall()
    index_info = {}
    for index in indexes:
        index_name = index[1]
        cursor.execute(f"PRAGMA index_info('{index_name}')")
        index_columns = [col[2] for col in cursor.fetchall()]
        index_info[index_name] = index_columns
    return index_info


def sqlite_to_mysql_type(sqlite_type):
    type_mapping = {
        'INTEGER': 'BIGINT',
        'REAL': 'DOUBLE',
        'TEXT': 'TEXT',
        'BLOB': 'BLOB',
        'BOOLEAN': 'TINYINT(1)',
        'DATETIME': 'DATETIME',
        'DATE': 'DATE',
        'TIME': 'TIME',
        'BIGINT': 'BIGINT'
    }
    for sqlite, mysql in type_mapping.items():
        if sqlite in sqlite_type.upper():
            return mysql
    return sqlite_type


def normalize_type(data_type):
    type_mapping = {
        'INT': 'BIGINT',
        'INTEGER': 'BIGINT',
        'BIGINT': 'BIGINT',
        'SMALLINT': 'INT',
        'TINYINT': 'INT',
        'FLOAT': 'DOUBLE',
        'DOUBLE': 'DOUBLE',
        'REAL': 'DOUBLE',
        'NUMERIC': 'DOUBLE',
        'DECIMAL': 'DOUBLE',
        'BOOLEAN': 'TINYINT(1)',
        'TEXT': 'TEXT',
        'BLOB': 'BLOB',
        'DATETIME': 'DATETIME',
        'DATE': 'DATE',
        'TIME': 'TIME',
        'VARCHAR': 'TEXT',
        'CHAR': 'TEXT',
        'CLOB': 'TEXT',
        'NVARCHAR': 'TEXT',
        'NCHAR': 'TEXT'
    }
    normalized = data_type.upper()
    for key, value in type_mapping.items():
        if key in normalized:
            return value
    return normalized


def convert_create_table_statement(sqlite_statement, foreign_key_data_types):
    mysql_statement = re.sub(
        r'CREATE TABLE', 'CREATE TABLE ', sqlite_statement, flags=re.IGNORECASE)
    mysql_statement = re.sub(r'\s*AUTOINCREMENT\s*', r' AUTO_INCREMENT ',
                             mysql_statement.strip(), flags=re.IGNORECASE)

    def replace_type(match):
        column_name = match.group(1)
        sqlite_data_type = match.group(2)
        data_type = sqlite_to_mysql_type(sqlite_data_type)
        constraints = match.group(3) or ''

        if 'not null' in constraints.lower():
            constraints = constraints.replace('not null', 'NOT NULL ')
            constraints = constraints.replace('NOT NULL NOT NULL', 'NOT NULL ')

        if 'primary key' in constraints.lower():
            constraints = constraints.replace('primary key', 'PRIMARY KEY ')

        # Ensure foreign key columns have the correct data type
        if column_name in foreign_key_data_types:
            data_type = foreign_key_data_types[column_name]

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

    # Set default character set and collation for the table
    mysql_statement += ' DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'

    return mysql_statement.strip(), foreign_keys


def get_foreign_keys(sqlite_conn, table_name):
    cursor = sqlite_conn.cursor()
    cursor.execute(f"PRAGMA foreign_key_list('{table_name}')")
    return cursor.fetchall()


def get_sqlite_column_type(sqlite_conn, table_name, column_name):
    cursor = sqlite_conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    for row in cursor.fetchall():
        if row[1] == column_name:
            return row[2]
    return None


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


def create_mysql_schema_without_foreign_keys(mysql_conn, sqlite_conn, tables):
    connection = mysql_conn.connect()
    foreign_keys_info = {}

    tables = sort_tables_by_dependency(tables, sqlite_conn)

    for table_name, create_statement in tables:
        print(f"\nProcessing table: {table_name}")
        print("Original SQLite statement:")
        print(create_statement)
        print()

        mysql_create_statement, foreign_keys = convert_create_table_statement(
            create_statement, {})
        print("Converted MySQL statement:")
        print(mysql_create_statement)
        print()

        try:
            print(f"Executing SQL: {mysql_create_statement}")
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

    # Create indexes
    for table_name, _ in tables:
        indexes = get_sqlite_indexes(sqlite_conn, table_name)
        for index_name, index_columns in indexes.items():
            index_columns_str = ", ".join(
                [f"`{col}`" for col in index_columns])
            create_index_sql = f"CREATE INDEX `{index_name}` ON "
            f"`{table_name}` ({index_columns_str})"
            print(f"Creating index for table "
                  f"'{table_name}': {create_index_sql}")
            try:
                connection.execute(text(create_index_sql))
            except Exception as e:
                print(f"Error creating index "
                      f"'{index_name}' for table '{table_name}': {e}")
                print(f"Problematic SQL: {create_index_sql}")
                connection.rollback()
                return False

    connection.commit()
    connection.close()
    return foreign_keys_info


def match_foreign_key_data_types(mysql_conn, foreign_keys_info):
    connection = mysql_conn.connect()

    for table_name, foreign_keys in foreign_keys_info.items():
        for fk in foreign_keys:
            referenced_table = re.search(
                r'REFERENCES\s+`?(\w+)`?', fk).group(1)
            fk_column = re.search(r'FOREIGN KEY\s+\(`?(\w+)`?\)', fk).group(1)
            ref_column = re.search(
                r'REFERENCES\s+`?\w+`?\s+\(`?(\w+)`?\)', fk).group(1)

            # Get the data type of the referenced column from MySQL
            ref_column_type = connection.execute(
                text(f"DESCRIBE {referenced_table}")).fetchall()
            ref_column_type_dict = {col[0]: col[1] for col in ref_column_type}
            ref_column_type_mysql = ref_column_type_dict[ref_column]

            # Alter the foreign key column type to match the referenced column type
            alter_fk_column_sql = f"ALTER TABLE `{table_name}` MODIFY "
            f"`{fk_column}` {ref_column_type_mysql}"
            print(f"Executing SQL to modify foreign key column type: "
                  f"{alter_fk_column_sql}")
            connection.execute(text(alter_fk_column_sql))

    connection.commit()
    connection.close()


def add_foreign_keys(mysql_conn, foreign_keys_info):
    connection = mysql_conn.connect()

    for table_name, foreign_keys in foreign_keys_info.items():
        for fk in foreign_keys:
            fk_sql = f"ALTER TABLE `{table_name}` ADD {fk}"
            print(f"Adding foreign key for table '{table_name}': {fk_sql}")
            try:
                print(f"Executing SQL: {fk_sql}")
                connection.execute(text(fk_sql))
            except Exception as e:
                print(f"Error adding foreign key for table "
                      f"'{table_name}': {e}")
                print(f"Problematic SQL: {fk_sql}")
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
        foreign_keys_info = create_mysql_schema_without_foreign_keys(
            mysql_conn, sqlite_conn, tables)
        match_foreign_key_data_types(mysql_conn, foreign_keys_info)
        if add_foreign_keys(mysql_conn, foreign_keys_info):
            transfer_data_with_pandas(sqlite_conn, mysql_conn, tables)
            if compare_schemas(sqlite_conn, mysql_conn) and compare_data(sqlite_conn, mysql_conn, tables):
                print("Migration successful and validated.")
            else:
                print("Migration validation failed.")
        else:
            print("Adding foreign keys failed.")

        sqlite_conn.close()
    else:
        print("Failed to connect to one or both databases.")


if __name__ == "__main__":
    main()
