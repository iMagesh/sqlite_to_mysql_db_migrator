import mysql.connector

# Database connection details
config = {
    'user': 'strapi',
    'password': 'strapi',
    'host': 'localhost',
    'database': 'strapi',
}

# Connect to the database
try:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor(dictionary=True)

    # Get all tables in the database
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()

    for table in tables:
        table_name = list(table.values())[0]

        # Get the primary key column for the table
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{config['database']}'
              AND TABLE_NAME = '{table_name}'
              AND COLUMN_KEY = 'PRI'
        """)
        primary_key = cursor.fetchone()

        if primary_key:
            column_name = primary_key['COLUMN_NAME']
            data_type = primary_key['DATA_TYPE']

            # Check if the primary key is not already INT UNSIGNED
            if data_type.upper() != 'INT' or 'unsigned' not in primary_key.get('COLUMN_TYPE', '').lower():
                # Generate and execute the ALTER TABLE statement
                alter_statement = f"""
                    ALTER TABLE `{table_name}`
                    MODIFY COLUMN `{column_name}` INT UNSIGNED NOT NULL AUTO_INCREMENT;
                """
                print(f"Executing: {alter_statement}")
                cursor.execute(alter_statement)
                conn.commit()
                print(f"Modified primary key for table {table_name}")
            else:
                print(f"""Primary key for table {
                      table_name} is already INT UNSIGNED""")
        else:
            print(f"No primary key found for table {table_name}")

except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("Database connection closed.")
