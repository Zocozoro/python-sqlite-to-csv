"""Extract a Sqlite DB file into .csv files.

Usage:

    python sqlite_to_csv.py <sqlite_file_location> <csv_save_location>
"""

import sqlite3
import csv
import sys


def _get_table_names(cursor):
    cursor.execute("SELECT * FROM sqlite_master WHERE type='table'")
    return cursor.fetchall()


def _get_column_names(cursor, table_name_to_pragma):
    cursor.execute('PRAGMA table_info({0})'.format(table_name_to_pragma))  # PRAGMA returns data about each column
    return cursor.fetchall()


def _get_rows_from_table(cursor, table_name_for_rows):
    cursor.execute('SELECT * FROM {0}'.format(table_name_for_rows))
    return cursor.fetchall()


def extract(sqlite_file_location, csv_save_location):
    """Extract a Sqlite DB into .csv files.

    Args:
        sqlite_file_location: The file location of the Sqlite DB file.
        csv_save_location: The file location where the .csv's will be saved to.
    """
    try:
        connection = sqlite3.connect(sqlite_file_location)
        cursor = connection.cursor()
        rows = _get_table_names(cursor)
        for row in rows:
            table_name = row[1]  # second item is table name
            with open('{0}/{1}.csv'.format(csv_save_location, table_name), 'w', newline='') as f:
                # Get a .csv writer to write rows to file (as this will handle escaping characters)
                csv_writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)

                # Adding table column names to start of file.
                column_names = _get_column_names(cursor, table_name)
                csv_writer.writerow(c[1] for c in column_names)

                # Adding each row to file.
                table_rows = _get_rows_from_table(cursor, table_name)
                csv_writer.writerows(table_rows)

    except Exception as e:
        with open('tables/errors.txt', 'wt') as error:
            type, value, traceback = sys.exc_info()
            error.write("{0}\n{1}\n{2}\n".format(type, value, traceback))
    finally:
        connection.close()


def main():
    sqlite_file_location = sys.argv[1]
    csv_save_location = sys.argv[2]
    extract(sqlite_file_location, csv_save_location)


if __name__ == '__main__':
    main()
