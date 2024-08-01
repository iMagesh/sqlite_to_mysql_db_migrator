import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
from collections import defaultdict, deque
import re
import logging
import sys
from datetime import datetime

# Set up logging


def setup_logger(log_file):
    logger = logging.getLogger('migration_logger')
    logger.setLevel(logging.DEBUG)

    # Create file handler which logs even debug messages
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)

    # Create console handler with a higher log level
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)

    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


# Create logger
log_file = f"migration_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logger = setup_logger(log_file)


def connect_sqlite(db_file):
    try:
        return sqlite3.connect(db_file)
    except sqlite3.Error as e:
        logger.error(f"Error connecting to SQLite database: {e}")
        return None


def connect_mysql(host, user, password, database):
    try:
        engine = create_engine(
            f"mysql+pymysql://{user}:{password}@{host}/{database}?charset=utf8mb4")
        return engine
    except Exception as e:
        logger.error(f"Error connecting to MySQL database: {e}")
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

    # Remove any size specifications or additional attributes
    base_type = sqlite_type.split('(')[0].upper()

    for sqlite, mysql in type_mapping.items():
        if base_type.startswith(sqlite):
            return mysql

    # If no match found, return the original type
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

    # Extract foreign key constraints
    foreign_keys = re.findall(
        r'CONSTRAINT\s+`?\w+`?\s+FOREIGN KEY\s*\([^)]+\)\s*REFERENCES\s*[^)]+\)[^,]*', mysql_statement, flags=re.IGNORECASE)

    # Remove the extracted foreign keys from the main statement
    for fk in foreign_keys:
        mysql_statement = mysql_statement.replace(f", {fk}", "")

    # Remove any trailing commas and whitespace
    mysql_statement = re.sub(r',\s*\)', ')', mysql_statement)

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
        # Add remaining tables to the end
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
            logger.info(f"\nProcessing table: {table_name}")
            mysql_create_statement, foreign_keys = convert_create_table_statement(
                create_statement)
            all_foreign_keys[table_name] = foreign_keys

            try:
                logger.info(f"Executing SQL: {mysql_create_statement}")
                connection.execute(text(mysql_create_statement))
                logger.info(
                    f"Table '{table_name}' created successfully in MySQL")
                created_tables.add(table_name)
            except Exception as e:
                logger.error(f"Error creating table '{table_name}' in MySQL:")
                logger.error(f"Error message: {e}")
                logger.error(f"Problematic SQL: {mysql_create_statement}")
                failed_tables.add(table_name)
                # Continue with the next table instead of raising an exception

        connection.commit()
        return created_tables, failed_tables, all_foreign_keys
    except Exception as e:
        logger.exception(
            f"An error occurred during MySQL schema creation: {e}")
        connection.rollback()
        return created_tables, failed_tables, all_foreign_keys
    finally:
        connection.close()


def set_primary_keys(mysql_conn, sqlite_conn, tables):
    mysql_connection = mysql_conn.connect()
    sqlite_cursor = sqlite_conn.cursor()

    try:
        for table_name, _ in tables:
            sqlite_cursor.execute(f"PRAGMA table_info('{table_name}')")
            columns = sqlite_cursor.fetchall()
            # col[5] is the pk flag
            primary_keys = [col[1] for col in columns if col[5] != 0]

            if primary_keys:
                pk_columns = ", ".join(f"`{pk}`" for pk in primary_keys)
                alter_sql = f"""
                    ALTER TABLE `{
                    table_name}` ADD PRIMARY KEY ({pk_columns})
                    """
                logger.info(f"""
                            Setting primary key for table '{
                            table_name}': {alter_sql}
                            """
                            )
                try:
                    mysql_connection.execute(text(alter_sql))
                    logger.info(
                        f"Successfully set primary key for table '{table_name}'")
                except Exception as e:
                    logger.error(f"""
                                 Error setting primary key for table '{
                                 table_name}': {e}"""
                                 )
                    logger.error(f"Problematic SQL: {alter_sql}")
                    return False
            else:
                logger.warning(f"""
                               No primary key defined for table '{
                               table_name}' in SQLite.""")

        mysql_connection.commit()
        return True
    except Exception as e:
        logger.exception(f"An error occurred while setting primary keys: {e}")
        mysql_connection.rollback()
        return False
    finally:
        mysql_connection.close()


def update_foreign_key_column_types(mysql_conn, sqlite_conn, foreign_keys_info):
    mysql_connection = mysql_conn.connect()
    sqlite_cursor = sqlite_conn.cursor()

    try:
        for table_name, foreign_keys in foreign_keys_info.items():
            for fk in foreign_keys:
                fk_column = fk[3]
                referenced_table = fk[2]
                referenced_column = fk[4]

                sqlite_cursor.execute(
                    f"PRAGMA table_info('{referenced_table}')")
                ref_columns = sqlite_cursor.fetchall()
                ref_column_info = next(
                    (col for col in ref_columns if col[1] == referenced_column), None)

                if ref_column_info:
                    ref_column_type = ref_column_info[2]
                    mysql_type = sqlite_to_mysql_type(ref_column_type)
                    is_nullable = "NULL" if ref_column_info[3] == 0 else "NOT NULL"

                    alter_fk_column_sql = f"""
                    ALTER TABLE `{table_name}`
                    MODIFY COLUMN `{fk_column}` {mysql_type} {is_nullable}
                    """
                    logger.info(f"""
                                Executing SQL to modify foreign key column type: {
                                alter_fk_column_sql}"""
                                )
                    try:
                        mysql_connection.execute(text(alter_fk_column_sql))
                        logger.info(f"""
                                    Successfully modified column {
                                    fk_column} in table {table_name}"""
                                    )
                    except Exception as e:
                        logger.error(
                            f"Error modifying foreign key column type: {e}")
                        logger.error(f"Problematic SQL: {alter_fk_column_sql}")
                        return False
                else:
                    logger.warning(f"""
                                   Could not find referenced column type for {
                                   referenced_table}.{referenced_column}""")

        mysql_connection.commit()
        return True
    except Exception as e:
        logger.exception(
            f"An error occurred while updating foreign key column types: {e}")
        mysql_connection.rollback()
        return False
    finally:
        mysql_connection.close()


def add_foreign_keys(mysql_conn, foreign_keys_info):
    connection = mysql_conn.connect()

    try:
        for table_name, foreign_keys in foreign_keys_info.items():
            for fk in foreign_keys:
                # Parse the foreign key constraint
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
                    logger.info(f"""Adding foreign key for table '{
                                table_name}': {fk_sql}""")
                    try:
                        connection.execute(text(fk_sql))
                    except Exception as e:
                        logger.error(f"""Error adding foreign key for table '{
                                     table_name}': {e}""")
                        logger.error(f"Problematic SQL: {fk_sql}")
                        # Continue with other foreign keys instead of raising an exception
                        continue
                else:
                    logger.warning(
                        f"Could not parse foreign key constraint: {fk}")

        connection.commit()
        return True
    except Exception as e:
        logger.exception(f"An error occurred while adding foreign keys: {e}")
        connection.rollback()
        return False
    finally:
        connection.close()


def transfer_data_with_pandas(sqlite_conn, mysql_conn, tables):
    engine = mysql_conn
    for table_name, _ in tables:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", sqlite_conn)
        df.to_sql(name=table_name, con=engine,
                  if_exists='append', index=False, method='multi')
        logger.info(f"""Transferred {len(df)} rows to table '{
                    table_name}' in MySQL""")


def compare_schemas(sqlite_conn, mysql_conn):
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
            logger.warning(f"Tables missing in MySQL: {missing_in_mysql}")
        if missing_in_sqlite:
            logger.warning(f"Tables missing in SQLite: {missing_in_sqlite}")
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
            logger.warning(f"""Schema mismatch: Columns in table '{
                           table}' do not match.""")
            missing_in_mysql = sqlite_columns - mysql_columns
            missing_in_sqlite = mysql_columns - sqlite_columns
            if missing_in_mysql:
                logger.warning(f"Columns missing in MySQL: {missing_in_mysql}")
            if missing_in_sqlite:
                logger.warning(f"""Columns missing in SQLite: {
                               missing_in_sqlite}""")
            return False

    logger.info("Schemas match.")
    return True


def compare_data(sqlite_conn, mysql_conn, tables):
    engine = mysql_conn
    for table_name, _ in tables:
        sqlite_df = pd.read_sql_query(
            f"SELECT * FROM {table_name}", sqlite_conn)
        mysql_df = pd.read_sql_table(table_name, con=engine)

        if not sqlite_df.equals(mysql_df):
            logger.warning(f"Data mismatch in table '{table_name}'.")
            sqlite_count = len(sqlite_df)
            mysql_count = len(mysql_df)
            logger.warning(
                f"Row count - SQLite: {sqlite_count}, MySQL: {mysql_count}")
            if sqlite_count != mysql_count:
                logger.warning(f"Row count mismatch in table '{table_name}'.")
            else:
                mismatched_rows = sqlite_df.compare(mysql_df)
                logger.warning(f"Mismatched rows in table '{table_name}':")
                logger.warning(mismatched_rows)
            return False

    logger.info("Data matches for all tables.")
    return True


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


def main():
    sqlite_db = "./data-20240703190001.db"
    mysql_host = "localhost"
    mysql_user = "strapi"
    mysql_password = "strapi"
    mysql_database = "strapi"

    logger.info("Starting database migration process")

    sqlite_conn = connect_sqlite(sqlite_db)
    mysql_conn = connect_mysql(
        mysql_host, mysql_user, mysql_password, mysql_database)

    if sqlite_conn and mysql_conn:
        try:
            tables = get_sqlite_schema(sqlite_conn)

            # Step 1: Create all tables with primary keys
            created_tables, failed_tables, foreign_keys_info = create_mysql_schema(
                mysql_conn, sqlite_conn, tables)

            if failed_tables:
                logger.warning(f"""Failed to create the following tables: {
                               ', '.join(failed_tables)}""")
                logger.warning(
                    "Continuing with the successfully created tables.")

            logger.info(f"""Successfully created tables: {
                        ', '.join(created_tables)}""")

            # # Step 2: Update column types for all tables
            # update_foreign_key_column_types(
            #     mysql_conn, sqlite_conn, foreign_keys_info)
            # logger.info("Finished updating column types.")

            # Step 4: Transfer data
            transfer_data_with_pandas(sqlite_conn, mysql_conn, [
                                      (table, '') for table in created_tables])
            logger.info("Finished transferring data.")

            # Step 3: Add foreign keys for all tables
            add_foreign_keys(mysql_conn, foreign_keys_info)
            logger.info("Finished adding foreign keys.")

            # # Step 5: Validate migration
            # if compare_schemas(sqlite_conn, mysql_conn) and compare_data(sqlite_conn, mysql_conn, [(table, '') for table in created_tables]):
            #     logger.info("Migration successful and validated.")
            # else:
            #     logger.warning("Migration completed, but validation failed.")
        except Exception as e:
            logger.exception(
                f"An unexpected error occurred during migration: {e}")
        finally:
            sqlite_conn.close()
            logger.info("Migration process completed")
    else:
        logger.error("Failed to connect to one or both databases.")


if __name__ == "__main__":
    main()
