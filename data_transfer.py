import pandas as pd
from logger_setup import logger
from sqlalchemy import text
import re


def convert_datetime_format(df):
    """
    Convert specified timestamp columns from milliseconds to datetime.

    Parameters:
    df (pd.DataFrame): DataFrame containing the data.
    timestamp_columns (list): List of column names to convert from milliseconds to datetime.

    Returns:
    pd.DataFrame: DataFrame with converted timestamp columns.
    """
    # List of columns to convert from milliseconds to datetime
    timestamp_columns = ['created_at', 'updated_at', 'published_at']

    # Convert the specified columns if they exist in the DataFrame
    for col in timestamp_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], unit='ms')
            print(f"Converted {col} to datetime.")

    print("Converted DataFrame:\n", df)

    return df


def transfer_data_with_pandas(sqlite_conn, mysql_conn, tables):
    engine = mysql_conn
    for table_name, _ in tables:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", sqlite_conn)

        df = convert_datetime_format(df)

        df.to_sql(name=table_name, con=engine,
                  if_exists='append', index=False, method='multi')
        logger.info("Transferred %d rows to table '%s' in MySQL",
                    len(df), table_name)


def add_foreign_keys(mysql_conn, foreign_keys_info):
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
