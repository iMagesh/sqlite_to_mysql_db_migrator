import re
from collections import defaultdict, deque
from sqlalchemy import text
from logger_setup import logger


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

    base_type = sqlite_type.split('(')[0].upper()

    for sqlite, mysql in type_mapping.items():
        if base_type.startswith(sqlite):
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


def convert_create_table_statement(sqlite_statement):
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

        return f"{column_name} {data_type} {constraints}"

    mysql_statement = re.sub(
        r'(`?\w+`?)\s+(\w+(?:\(\d+\))?)\s*((?:(?:NOT)?\s*NULL|(?:PRIMARY)?\s*KEY|DEFAULT\s*[^,]+)?)',
        replace_type, mysql_statement, flags=re.IGNORECASE)

    mysql_statement = re.sub(r'PRIMARY KEY\s*AUTO_INCREMENT',
                             'PRIMARY KEY AUTO_INCREMENT', mysql_statement, flags=re.IGNORECASE)
    mysql_statement = mysql_statement.replace('"', '`')

    foreign_keys = re.findall(
        r'CONSTRAINT\s+`?\w+`?\s+FOREIGN KEY\s*\([^)]+\)\s*REFERENCES\s*[^)]+\)[^,]*', mysql_statement, flags=re.IGNORECASE)

    for fk in foreign_keys:
        mysql_statement = mysql_statement.replace(f", {fk}", "")

    mysql_statement = re.sub(r',\s*\)', ')', mysql_statement)
    mysql_statement = re.sub(r'`\(', '` (', mysql_statement)
    mysql_statement += ' DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'

    return mysql_statement.strip(), foreign_keys


def get_foreign_keys(sqlite_conn, table_name):
    cursor = sqlite_conn.cursor()
    cursor.execute(f"PRAGMA foreign_key_list('{table_name}')")
    return cursor.fetchall()


def sort_tables_by_dependency(tables, sqlite_conn):
    dependency_graph = defaultdict(set)
    in_degree = {table_name: 0 for table_name, _ in tables}

    for table_name, _ in tables:
        foreign_keys = get_foreign_keys(sqlite_conn, table_name)
        for fk in foreign_keys:
            referenced_table = fk[2]
            if referenced_table != table_name:  # Avoid self-references
                dependency_graph[referenced_table].add(table_name)
                in_degree[table_name] += 1

    sorted_tables = []
    no_dependencies = deque(
        [table for table, degree in in_degree.items() if degree == 0])

    while no_dependencies:
        table = no_dependencies.popleft()
        sorted_tables.append(table)
        for dependent in dependency_graph[table]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                no_dependencies.append(dependent)

    if len(sorted_tables) != len(tables):
        logger.warning(
            "Circular dependencies detected. Some tables may not be properly sorted.")
        sorted_tables.extend(set(dict(tables).keys()) - set(sorted_tables))

    table_dict = dict(tables)
    return [(table, table_dict[table]) for table in sorted_tables]


def create_mysql_schema(mysql_conn, sqlite_conn, tables):
    connection = mysql_conn.connect()
    created_tables = set()
    failed_tables = set()
    all_foreign_keys = {}

    try:
        tables = sort_tables_by_dependency(tables, sqlite_conn)

        for table_name, create_statement in tables:
            logger.info("Processing table: %s", table_name)
            mysql_create_statement, foreign_keys = convert_create_table_statement(
                create_statement)
            all_foreign_keys[table_name] = foreign_keys

            try:
                logger.info("Executing SQL: %s", mysql_create_statement)
                connection.execute(text(mysql_create_statement))
                logger.info(
                    "Table '%s' created successfully in MySQL", table_name)
                created_tables.add(table_name)
            except Exception as e:
                logger.error("Error creating table '%s' in MySQL:", table_name)
                logger.error("Error message: %s", e)
                logger.error("Problematic SQL: %s", mysql_create_statement)
                failed_tables.add(table_name)

        connection.commit()
        return created_tables, failed_tables, all_foreign_keys
    except Exception as e:
        logger.exception(
            "An error occurred during MySQL schema creation: %s", e)
        connection.rollback()
        return created_tables, failed_tables, all_foreign_keys
    finally:
        connection.close()


def get_sqlite_foreign_keys(sqlite_conn):
    cursor = sqlite_conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    foreign_keys_info = {}
    for (table_name,) in tables:
        cursor.execute(f"PRAGMA foreign_key_list('{table_name}')")
        fks = cursor.fetchall()
        if fks:
            foreign_keys_info[table_name] = fks

    return foreign_keys_info
