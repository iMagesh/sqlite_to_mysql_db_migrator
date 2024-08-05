from config import SQLITE_DB, MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
from logger_setup import logger
from db_connections import connect_sqlite, connect_mysql
from schema_conversion import get_sqlite_schema, create_mysql_schema, get_sqlite_foreign_keys
from data_transfer import transfer_data_with_pandas, add_foreign_keys
from validation import compare_schemas, compare_data


def migrate_database(sqlite_conn, mysql_conn):

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


def main():
    logger.info("Starting database migration process")

    try:
        sqlite_conn = connect_sqlite(SQLITE_DB)
        mysql_conn = connect_mysql(
            MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE)

        if sqlite_conn and mysql_conn:
            migrate_database(sqlite_conn, mysql_conn)
        else:
            logger.error("Failed to connect to one or both databases.")
    except Exception as e:
        logger.exception(f"An unexpected error occurred during migration: {e}")
    finally:
        sqlite_conn.close()

    logger.info("Migration process completed")


if __name__ == "__main__":
    main()
