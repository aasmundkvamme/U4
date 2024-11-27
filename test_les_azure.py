import pyodbc
import os

conn_str = os.environ["Connection_SQL"]


def get_latest_update_time(table_name):
    """
    Return the latest update time for the given table from the akv_sist_oppdatert table.
    """
    with pyodbc.connect(conn_str) as connection:
        cursor = connection.cursor()
        try:
            query = """
            SELECT [sist_oppdatert] FROM [dbo].[akv_sist_oppdatert]
            WHERE [tabell] = ?
            """
            cursor.execute(query, (table_name,))
            row = cursor.fetchone()
            if row:
                return row[0]
        except pyodbc.Error as exc:
            logging.error(f"Error: {exc}")

tabell = "enrollments"
sist_oppdatert = get_latest_update_time(tabell)

print(sist_oppdatert)