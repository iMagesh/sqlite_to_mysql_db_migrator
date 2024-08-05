import pandas as pd
from sqlalchemy import text
from logger_setup import logger
from schema_conversion import normalize_type


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
            logger.warning("Tables missing in MySQL: %s", missing_in_mysql)
        if missing_in_sqlite:
            logger.warning("Tables missing in SQLite: %s", missing_in_sqlite)
        return False

    for table in sqlite_tables:
        sqlite_schema = sqlite_cursor.execute(
            f"PRAGMA table_info({table})").fetchall()
        mysql_schema = mysql_connection.execute(
            text(f"DESCRIBE `{table}`")).fetchall()

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
