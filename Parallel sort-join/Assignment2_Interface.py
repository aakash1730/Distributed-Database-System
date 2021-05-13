#
# Assignment2 Interface
#

import psycopg2
import psycopg2.extras
import os
import sys
import threading
import concurrent.futures
import time

RANGE_TABLE_PREFIX = 'range_part'
ROUND_ROBIN_1_TABLE_PREFIX = 'round_robin1_part'
ROUND_ROBIN_2_TABLE_PREFIX = 'round_robin2_part'
ROUND_ROBIN_OUTPUT_TABLE_PREFIX = 'round_robin_output_part'
NUMBER_OF_THREAD = 5


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort(InputTable, SortingColumnName, OutputTable, openconnection):
    # Implement ParallelSort Here.
    try:
        start_time = time.time()
        cursor = openconnection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("Select count(*) as \"row\" from " + InputTable)
        total_number_of_rows_in_input_table = cursor.fetchone()['row']

        columns_datatype = get_schema_for_input_table(InputTable=InputTable, openconnection=openconnection)

        create_temporary_table_for_sort_and_join(table_name=RANGE_TABLE_PREFIX, attributes=columns_datatype,
                                                 openconnection=openconnection)

        range_min, range_max = get_min_max_value_for_partition(InputTable=InputTable,
                                                               SortingColumnName=SortingColumnName,
                                                               openconnection=openconnection)

        columns = get_column_names(attributes=columns_datatype)
        lower_bound = range_min - 0.000000001
        partition_value = (range_max - range_min) / NUMBER_OF_THREAD
        upper_bound = range_min + partition_value
        futures = []
        # Default 5 threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=NUMBER_OF_THREAD) as executor:
            for i in range(NUMBER_OF_THREAD):
                # noinspection PyTypeChecker
                futures.append(
                    executor.submit(range_insert_and_sort, InputTable=InputTable, SortingColumnName=SortingColumnName,
                                    index=i, min_value=lower_bound, max_value=upper_bound, columns=columns,
                                    openconnection=openconnection))

                lower_bound = upper_bound
                upper_bound = upper_bound + partition_value

        total_rows = 0
        for future in concurrent.futures.as_completed(futures):
            total_rows += future.result()

        if total_number_of_rows_in_input_table != total_rows:
            print('Some problem with threading and logic')
        else:
            print(f'Working perfectly, count matched, total number of rows in input table : '
                  f'{total_number_of_rows_in_input_table}, and Total number of rows in output table {total_rows}')

        stored_result(InputTable=RANGE_TABLE_PREFIX, OutputTable=OutputTable, attributes=columns_datatype,
                      columns=columns,
                      openconnection=openconnection)

        print(f"Time to complete parallel sort: {time.time() - start_time}")
        cursor.close()
        openconnection.commit()

    except Exception as detail:
        print("Exception occurred: ", detail)

    finally:
        cursor = openconnection.cursor()
        for i in range(NUMBER_OF_THREAD):
            cursor.execute("DROP TABLE IF EXISTS " + RANGE_TABLE_PREFIX + str(i))


def stored_result(InputTable, OutputTable, attributes, columns, openconnection, attributes1=None, columns1=None):
    cursor = openconnection.cursor()
    cursor.execute("DROP TABLE IF EXISTS " + OutputTable)
    cursor.execute(
        "CREATE TABLE " + OutputTable + " (" + attributes[0][0] + " " + attributes[0][1] + ")")

    for j in range(1, len(attributes)):
        cursor.execute(
            "ALTER TABLE " + OutputTable + " ADD COLUMN " + attributes[j][0] + " " + attributes[j][1])

    if attributes1 is not None:
        for j in range(0, len(attributes1)):
            cursor.execute(
                "ALTER TABLE " + OutputTable + " ADD COLUMN " + attributes1[j][0] + " " + attributes1[j][1])

    if columns1 is not None:
        columns = columns + ", " + columns1

    for i in range(NUMBER_OF_THREAD):
        cursor.execute("Insert into " + OutputTable + "(" + columns + ") (Select * from " + InputTable + str(
            i) + ")")

    cursor.close()
    openconnection.commit()


def range_insert_and_sort(InputTable, SortingColumnName, index, min_value, max_value, columns,
                          openconnection) -> object:
    cursor = openconnection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(
        "Insert into " + RANGE_TABLE_PREFIX + str(index) + "(" + columns + ") (Select " + columns + " from " +
        InputTable + " Where " + SortingColumnName + " > " + str(min_value) + " And " + \
        SortingColumnName + " <= " + str(max_value) + " Order by " + SortingColumnName + ")")

    if index == NUMBER_OF_THREAD - 1:
        cursor.execute("Insert into " + RANGE_TABLE_PREFIX + str(
            index) + "(" + columns + ") (Select " + columns + " from " + InputTable + " Where " + SortingColumnName + " ISNULL)")

    cursor.execute("Select count(*) as \"row\"  from " + RANGE_TABLE_PREFIX + str(index))
    result = cursor.fetchone()['row']
    cursor.close()
    openconnection.commit()
    return result


def get_min_max_value_for_partition(InputTable, SortingColumnName, openconnection):
    cursor = openconnection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    min_query = "Select MIN(" + SortingColumnName + ") as \"row\" From " + InputTable
    cursor.execute(min_query)
    min_value = cursor.fetchone()['row']
    max_query = "Select MAX(" + SortingColumnName + ") as \"row\" From " + InputTable
    cursor.execute(max_query)
    max_value = cursor.fetchone()['row']
    cursor.close()
    openconnection.commit()
    return min_value, max_value


def get_schema_for_input_table(InputTable, openconnection):
    cursor = openconnection.cursor()
    schema_query = "SELECT COLUMN_NAME, DATA_TYPE  FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s"
    cursor.execute(schema_query, (InputTable,))
    columns = cursor.fetchall()
    cursor.close()
    openconnection.commit()
    return columns


def create_temporary_table_for_sort_and_join(table_name, attributes, openconnection, attributes1=None):
    cursor = openconnection.cursor()

    for i in range(NUMBER_OF_THREAD):
        cursor.execute("DROP TABLE IF EXISTS " + table_name + str(i))
        cursor.execute(
            "CREATE TABLE " + table_name + str(i) + " (" + attributes[0][0] + " " +
            attributes[0][1] + ")")

        for j in range(1, len(attributes)):
            cursor.execute(
                "ALTER TABLE " + table_name + str(i) + " ADD COLUMN " + attributes[j][0] + " " +
                attributes[j][1])

        if attributes1 is not None:
            for j in range(len(attributes1)):
                cursor.execute(
                    "ALTER TABLE " + table_name + str(i) + " ADD COLUMN " + attributes1[j][0] + " " +
                    attributes1[j][1])
    cursor.close()
    openconnection.commit()


def get_column_names(attributes):
    column_name = ''
    for i in range(len(attributes)):
        if i != len(attributes) - 1:
            column_name += attributes[i][0] + ', '
        else:
            column_name += attributes[i][0]

    return column_name


def ParallelJoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    # Implement ParallelJoin Here.
    try:
        start_time = time.time()
        cursor = openconnection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        input_query = "SELECT count(*) as \"row\" FROM " + InputTable1 + " INNER JOIN " + \
                      InputTable2 + " ON " + InputTable1 + "." + \
                      Table1JoinColumn + "=" + InputTable2 + "." + Table2JoinColumn

        cursor.execute(input_query)
        total_number_of_rows_in_input_table = cursor.fetchone()['row']

        columns_datatype_1 = get_schema_for_input_table(InputTable=InputTable1, openconnection=openconnection)

        columns_datatype_2 = get_schema_for_input_table(InputTable=InputTable2, openconnection=openconnection)

        create_temporary_table_for_sort_and_join(table_name=ROUND_ROBIN_1_TABLE_PREFIX, attributes=columns_datatype_1,
                                                 openconnection=openconnection)

        create_temporary_table_for_sort_and_join(table_name=ROUND_ROBIN_2_TABLE_PREFIX, attributes=columns_datatype_2,
                                                 openconnection=openconnection)

        create_temporary_table_for_sort_and_join(table_name=ROUND_ROBIN_OUTPUT_TABLE_PREFIX,
                                                 attributes=columns_datatype_1,
                                                 openconnection=openconnection, attributes1=columns_datatype_2)

        columns_1 = get_column_names(attributes=columns_datatype_1)

        columns_2 = get_column_names(attributes=columns_datatype_2)

        futures = []
        # Default 5 threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=NUMBER_OF_THREAD) as executor:
            for i in range(NUMBER_OF_THREAD):
                # noinspection PyTypeChecker
                futures.append(
                    executor.submit(round_robin_insert_and_join, InputTable1=InputTable1, InputTable2=InputTable2,
                                    Table1JoinColumn=Table1JoinColumn, Table2JoinColumn=Table2JoinColumn,
                                    Table1Attributes=columns_1,
                                    Table2Attributes=columns_2, openconnection=openconnection, index=i))

        total_rows = 0
        for future in concurrent.futures.as_completed(futures):
            total_rows += future.result()

        if total_number_of_rows_in_input_table != total_rows:
            print('Some problem with threading and logic')
        else:
            print(f'Working perfectly, count matched, total number of rows in input table : '
                  f'{total_number_of_rows_in_input_table}, and Total number of rows in output table {total_rows}')

        stored_result(InputTable=ROUND_ROBIN_OUTPUT_TABLE_PREFIX, OutputTable=OutputTable,
                      attributes=columns_datatype_1,
                      columns=columns_1,
                      openconnection=openconnection, attributes1=columns_datatype_2, columns1=columns_2)

        print(f"Time to complete parallel join: {time.time() - start_time}")
        openconnection.commit()
        cursor.close()

    except Exception as detail:
        print("Exception occurred: ", detail)

    finally:
        cursor = openconnection.cursor()
        for i in range(NUMBER_OF_THREAD):
            cursor.execute("DROP TABLE IF EXISTS " + RANGE_TABLE_PREFIX + str(i))

        cursor.close()
        openconnection.commit()


def round_robin_insert_and_join(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, Table1Attributes,
                                Table2Attributes, openconnection, index):
    round_robin_insert(table_name=InputTable1, insert_table_name=ROUND_ROBIN_1_TABLE_PREFIX,
                       attributes=Table1Attributes, index=index, joining_column=Table1JoinColumn,
                       openconnection=openconnection)

    round_robin_insert(table_name=InputTable2, insert_table_name=ROUND_ROBIN_2_TABLE_PREFIX,
                       attributes=Table2Attributes, index=index, joining_column=Table2JoinColumn,
                       openconnection=openconnection)

    return round_robin_insert_and_join_in_output_table(Table1Attributes=Table1Attributes,
                                                       Table2Attributes=Table2Attributes,
                                                       Table1JoinColumn=Table1JoinColumn,
                                                       Table2JoinColumn=Table2JoinColumn,
                                                       index=index, openconnection=openconnection)


def round_robin_insert(table_name, insert_table_name, attributes, index, joining_column, openconnection):
    cursor = openconnection.cursor()

    partition_query = 'Insert into ' + insert_table_name + str(index) + ' (' + attributes + ')'
    select_query = 'Select ' + attributes + ' from ' + table_name + ' Where ' + joining_column + ' % ' + \
                   str(NUMBER_OF_THREAD) + ' = (' + str(index + 1) + ' % ' + str(NUMBER_OF_THREAD) + ')'

    cursor.execute(partition_query + ' ' + select_query)

    cursor.close()
    openconnection.commit()


def round_robin_insert_and_join_in_output_table(Table1Attributes, Table2Attributes, Table1JoinColumn, Table2JoinColumn,
                                                index, openconnection):
    cursor = openconnection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    insert_query = "INSERT INTO " + ROUND_ROBIN_OUTPUT_TABLE_PREFIX + str(index) + " (" + Table1Attributes + ", " + \
                   Table2Attributes + ") SELECT * FROM " + ROUND_ROBIN_1_TABLE_PREFIX + str(index) + " INNER JOIN " + \
                   ROUND_ROBIN_2_TABLE_PREFIX + str(index) + " ON " + ROUND_ROBIN_1_TABLE_PREFIX + str(index) + "." + \
                   Table1JoinColumn + "=" + ROUND_ROBIN_2_TABLE_PREFIX + str(index) + "." + Table2JoinColumn

    cursor.execute(insert_query)

    cursor.execute("Select count(*) as \"row\"  from " + ROUND_ROBIN_OUTPUT_TABLE_PREFIX + str(index))
    result = cursor.fetchone()['row']

    cursor.close()
    openconnection.commit()
    return result


################### DO NOT CHANGE ANYTHING BELOW THIS #############################

# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


# Donot change this function
def createDB(dbname='dds_assignment2'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()


# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
