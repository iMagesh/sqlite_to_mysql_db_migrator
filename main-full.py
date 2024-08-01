import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
from collections import defaultdict, deque
import re
import logging
from logging.handlers import RotatingFileHandler
import sys
from datetime import datetime
import os
import time
from functools import wraps

# Configuration
SQLITE_DB = os.getenv('SQLITE_DB', './data-20240703190001.db')
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_USER = os.getenv('MYSQL_USER', 'strapi')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'strapi')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'strapi')


def setup_logger(log_file):
    logger = logging.getLogger('migration_logger')
    logger.setLevel(logging.DEBUG)

    file_handler = RotatingFileHandler(
        log_file, maxBytes=10*1024*1024, backupCount=5)
    file_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# Create logger
log_file = f"migration_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logger = setup_logger(log_file)


def retry(max_attempts=3, delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    logger.warning("Attempt %d failed: %s", attempts, str(e))
                    if attempts == max_attempts:
                        logger.error("All %d attempts failed.", max_attempts)
                        raise
                    time.sleep(delay)
        return wrapper
    return decorator


@retry(max_attempts=3, delay=2)
def connect_sqlite(db_file):
    """
    Connect to a SQLite database.

    Args:
        db_file (str): Path to the SQLite database file.

    Returns:
        sqlite3.Connection: A connection object or None if connection failed.
    """
    try:
        return sqlite3.connect(db_file)
    except sqlite3.Error as e:
        logger.error("Error connecting to SQLite database: %s", e)
        return None


@retry(max_attempts=3, delay=2)
def connect_mysql(host, user, password, database):
    """
    Connect to a MySQL database.

    Args:
        host (str): MySQL host address.
        user (str): MySQL username.
        password (str): MySQL password.
        database (str): MySQL database name.

    Returns:
        sqlalchemy.engine.base.Engine: A SQLAlchemy engine object or None if connection failed.
    """
    try:
        engine = create_engine(
            f"mysql+pymysql://{user}:{password}@{host}/{database}?charset=utf8mb4")
        return engine
    except Exception as e:
        logger.error("Error connecting to MySQL database: %s", e)
        return None


def get_sqlite_schema(sqlite_conn):
    """
    Get the schema of all tables in the SQLite database.

    Args:
        sqlite_conn (sqlite3.Connection): SQLite database connection.

    Returns:
        list: A list of tuples containing table names and their CREATE TABLE statements.
    """
    cursor = sqlite_conn.cursor()
    cursor.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    return cursor.fetchall()


def get_sqlite_indexes(sqlite_conn, table_name):
    """
    Get the indexes of a specific table in the SQLite database.

    Args:
        sqlite_conn (sqlite3.Connection): SQLite database connection.
        table_name (str): Name of the table.

    Returns:
        dict: A dictionary of index names and their column names.
    """
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
    """
    Convert SQLite data type to MySQL data type.

    Args:
        sqlite_type (str): SQLite data type.

    Returns:
        str: Corresponding MySQL data type.
    """
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
    """
    Normalize data type for comparison between SQLite and MySQL.

    Args:
        data_type (str): Data type to normalize.

    Returns:
        str: Normalized data type.
    """
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
    """
    Convert a SQLite CREATE TABLE statement to MySQL syntax.

    Args:
        sqlite_statement (str): The SQLite CREATE TABLE statement.

    Returns:
        tuple: A tuple containing the MySQL CREATE TABLE statement and a list of foreign key constraints.
    """
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
    """
    Get foreign keys for a specific table in the SQLite database.

    Args:
        sqlite_conn (sqlite3.Connection): SQLite database connection.
        table_name (str): Name of the table.

    Returns:
        list: A list of foreign key information.
    """
    cursor = sqlite_conn.cursor()
    cursor.execute(f"PRAGMA foreign_key_list('{table_name}')")
    return cursor.fetchall()


def sort_tables_by_dependency(tables, sqlite_conn):
    """
    Sort tables by their dependencies based on foreign key relationships.

    Args:
        tables (list): List of tables to sort.
        sqlite_conn (sqlite3.Connection): SQLite database connection.

    Returns:
        list: Sorted list of tables.
    """
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
    """
    Create MySQL schema based on SQLite schema.

    Args:
        mysql_conn (sqlalchemy.engine.base.Engine): MySQL database connection.
        sqlite_conn (sqlite3.Connection): SQLite database connection.
        tables (list): List of tables to create.

    Returns:
        tuple: A tuple containing sets of created and failed tables, and a dictionary of foreign keys.
    """
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


def transfer_data_with_pandas(sqlite_conn, mysql_conn, tables):
    """
    Transfer data from SQLite to MySQL using pandas.

    Args:
        sqlite_conn (sqlite3.Connection): SQLite database connection.
        mysql_conn (sqlalchemy.engine.base.Engine): MySQL database connection.
        tables (list): List of tables to transfer data from.
    """
    engine = mysql_conn
    for table_name, _ in tables:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", sqlite_conn)
        df.to_sql(name=table_name, con=engine,
                  if_exists='append', index=False, method='multi')
        logger.info("Transferred %d rows to table '%s' in MySQL",
                    len(df), table_name)


def add_foreign_keys(mysql_conn, foreign_keys_info):
    """
    Add foreign key constraints to MySQL tables.

    Args:
        mysql_conn (sqlalchemy.engine.base.Engine): MySQL database connection.
        foreign_keys_info (dict): Dictionary containing foreign key information for each table.

    Returns:
        bool: True if all foreign keys were added successfully, False otherwise.
    """
    connection = mysql_conn.connect()

    try:
        for table_name, foreign_keys in foreign_keys_info.items():
            for fk in foreign_keys:
                match = re.match(
                    r'CONSTRAINT\s+`?(\w+)`?\s+FOREIGN KEY\s*\(`?(\w+)`?\)\s*REFERENCES\s*`?(\w+)`?\s*\(`?(\w+)`?\)(\s+ON DELETE (\w+(\s+\w+)?)?)?(\s+ON UPDATE (\w+(\s+\w+)?)?)?', fk, re.IGNORECASE)

                if match:
                    constraint_name = match.group(1)
                    fk_column = match.group(2)
                    referenced_table = match.group(3)
                    referenced_column = match.group(4)
                    on_delete = match.group(
                        6) if match.group(6) else 'RESTRICT'
                    on_update = match.group(
                        9) if match.group(9) else 'RESTRICT'

                    fk_sql = f"""
                    ALTER TABLE `{table_name}`
                    ADD CONSTRAINT `{constraint_name}`
                    FOREIGN KEY (`{fk_column}`)
                    REFERENCES `{referenced_table}`(`{referenced_column}`)
                    ON DELETE {on_delete} ON UPDATE {on_update}
                    """
                    logger.info(
                        "Adding foreign key for table '%s': %s", table_name, fk_sql)
                    try:
                        connection.execute(text(fk_sql))
                    except Exception as e:
                        logger.error(
                            "Error adding foreign key for table '%s': %s", table_name, e)
                        logger.error("Problematic SQL: %s", fk_sql)
                        continue
                else:
                    logger.warning(
                        "Could not parse foreign key constraint: %s", fk)

        connection.commit()
        return True
    except Exception as e:
        logger.exception("An error occurred while adding foreign keys: %s", e)
        connection.rollback()
        return False
    finally:
        connection.close()


def compare_schemas(sqlite_conn, mysql_conn):
    """
    Compare schemas between SQLite and MySQL databases.

    Args:
        sqlite_conn (sqlite3.Connection): SQLite database connection.
        mysql_conn (sqlalchemy.engine.base.Engine): MySQL database connection.

    Returns:
        bool: True if schemas match, False otherwise.
    """
    sqlite_cursor = sqlite_conn.cursor()
    mysql_connection = mysql_conn.connect()

    sqlite_tables = {table[0] for table in sqlite_cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall()}
    mysql_tables = {row[0] for row in mysql_connection.execute(
        text("SHOW TABLES")).fetchall()}

    if sqlite_tables != mysql_tables:
        logger.warning(
            "Schema mismatch: Tables in SQLite and MySQL do not match.")
        missing_in_mysql = sqlite_tables - mysql_tables
        missing_in_sqlite = mysql_tables - sqlite_tables
        if missing_in_mysql:
            logger.warning("Tables missing in MySQL: %s", missing_in_mysql)
        if missing_in_sqlite:
            logger.warning("Tables missing in SQLite: %s", missing_in_sqlite)
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
            logger.warning(
                "Schema mismatch: Columns in table '%s' do not match.", table)
            missing_in_mysql = sqlite_columns - mysql_columns
            missing_in_sqlite = mysql_columns - sqlite_columns
            if missing_in_mysql:
                logger.warning("Columns missing in MySQL: %s",
                               missing_in_mysql)
            if missing_in_sqlite:
                logger.warning("Columns missing in SQLite: %s",
                               missing_in_sqlite)
            return False

    logger.info("Schemas match.")
    return True


def compare_data(sqlite_conn, mysql_conn, tables):
    """
    Compare data between SQLite and MySQL databases.

    Args:
        sqlite_conn (sqlite3.Connection): SQLite database connection.
        mysql_conn (sqlalchemy.engine.base.Engine): MySQL database connection.
        tables (list): List of tables to compare.

    Returns:
        bool: True if data matches, False otherwise.
    """
    engine = mysql_conn
    for table_name, _ in tables:
        sqlite_df = pd.read_sql_query(
            f"SELECT * FROM {table_name}", sqlite_conn)
        mysql_df = pd.read_sql_table(table_name, con=engine)

        if not sqlite_df.equals(mysql_df):
            logger.warning("Data mismatch in table '%s'.", table_name)
            sqlite_count = len(sqlite_df)
            mysql_count = len(mysql_df)
            logger.warning("Row count - SQLite: %d, MySQL: %d",
                           sqlite_count, mysql_count)
            if sqlite_count != mysql_count:
                logger.warning("Row count mismatch in table '%s'.", table_name)
            else:
                mismatched_rows = sqlite_df.compare(mysql_df)
                logger.warning("Mismatched rows in table '%s':", table_name)
                logger.warning("%s", mismatched_rows)
            return False

    logger.info("Data matches for all tables.")
    return True


def get_sqlite_foreign_keys(sqlite_conn):
    """
    Get all foreign keys from the SQLite database.

    Args:
        sqlite_conn (sqlite3.Connection): SQLite database connection.

    Returns:
        dict: A dictionary containing foreign key information for each table.
    """
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


def main():
    """
    Main function to orchestrate the database migration process.
    """
    logger.info("Starting database migration process")

    sqlite_conn = connect_sqlite(SQLITE_DB)
    mysql_conn = connect_mysql(
        MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE)

    if sqlite_conn and mysql_conn:
        try:
            tables = get_sqlite_schema(sqlite_conn)

            # Step 1: Create all tables with primary keys
            created_tables, failed_tables, foreign_keys_info = create_mysql_schema(
                mysql_conn, sqlite_conn, tables)

            if failed_tables:
                logger.warning(
                    "Failed to create the following tables: %s", ', '.join(failed_tables))
                logger.warning(
                    "Continuing with the successfully created tables.")

            logger.info("Successfully created tables: %s",
                        ', '.join(created_tables))

            # Step 2: Transfer data
            transfer_data_with_pandas(sqlite_conn, mysql_conn, [
                                      (table, '') for table in created_tables])
            logger.info("Finished transferring data.")

            # Step 3: Add foreign keys for all tables
            if add_foreign_keys(mysql_conn, foreign_keys_info):
                logger.info("Successfully added foreign keys for all tables.")
            else:
                logger.warning(
                    "Some issues occurred while adding foreign keys.")

            # Step 4: Validate migration
            if compare_schemas(sqlite_conn, mysql_conn) and compare_data(sqlite_conn, mysql_conn, [(table, '') for table in created_tables]):
                logger.info("Migration successful and validated.")
            else:
                logger.warning("Migration completed, but validation failed.")
        except Exception as e:
            logger.exception(
                "An unexpected error occurred during migration: %s", e)
        finally:
            sqlite_conn.close()
            logger.info("Migration process completed")
    else:
        logger.error("Failed to connect to one or both databases.")


if __name__ == "__main__":
    main()
