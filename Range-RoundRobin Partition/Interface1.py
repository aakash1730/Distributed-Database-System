import psycopg2
import psycopg2.extras
import os
from os import path

RANGE_TABLE_PREFIX = 'range_ratings_part'
RROBIN_TABLE_PREFIX = 'round_robin_ratings_part'


class MetadataConstant:
    METADATA_TABLE_NAME = 'fragmentation'
    ATTRIBUTE1_ALGORITHM = 'algorithm'
    ATTRIBUTE2_PARTITION = 'numberofpartition'
    TABLE_EXIST_KEY = 'table_exist'
    METADATA_EXIST_QUERY = ''' Select EXISTS(SELECT FROM information_schema.tables WHERE table_name = ''' + \
                           "'" + METADATA_TABLE_NAME + "') as " + TABLE_EXIST_KEY
    CREATE_METADATA_TABLE_QUERY = ' Create Table ' + METADATA_TABLE_NAME + ' (' + ATTRIBUTE1_ALGORITHM \
                                  + ' Varchar Primary Key, ' + ATTRIBUTE2_PARTITION + ' Integer' + ')'

    RANGE = 'Range'
    ROUND_ROBIN = 'Round_Robin'
    NUMBER_OF_PARTITION = 'Number_Of_Partition'
    METADATA_INSERT_QUERY = 'Insert into ' + METADATA_TABLE_NAME + ' (' + ATTRIBUTE1_ALGORITHM + ', ' + \
                            ATTRIBUTE2_PARTITION + ') Values(%s, %s)'


def getOpenConnection(user='postgres', password='1234', dbname='dds_assignment1'):  # change dbname='postgres'
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    create_sql = 'CREATE TABLE ' + ratingstablename + \
                 ' (ID serial, UserID Integer, MovieID Integer, Rating Float)'

    cursor = openconnection.cursor()
    cursor.execute("DROP TABLE IF EXISTS " + ratingstablename)
    cursor.execute(create_sql)
    separator = '::'
    with open(ratingsfilepath, 'r') as f, open('temp.csv', 'w') as t:
        for line in f:
            print(','.join(line.split(separator)[:3]), file=t)

    table_data_file = open('temp.csv', 'r')
    cursor.copy_from(table_data_file, ratingstablename, sep=',', columns=('UserID', 'MovieID', 'Rating'))

    table_data_file.close()
    os.remove('temp.csv')
    cursor.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cursor = openconnection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    rating = 5 / numberofpartitions
    lower_bound = -0.0000001
    upper_bound = rating
    for i in range(numberofpartitions):
        table_name = RANGE_TABLE_PREFIX + str(i)
        cursor.execute('DROP TABLE IF EXISTS ' + table_name)
        cursor.execute('CREATE TABLE ' + table_name + ' (UserID Integer, MovieID Integer, Rating Float)')

        partition_query = 'Insert into ' + table_name + ' (UserID, MovieID, Rating)'
        select_query = 'Select UserID, MovieID, Rating from ' + ratingstablename + ' Where Rating > ' + \
                       str(lower_bound) + ' and  Rating <= ' + str(upper_bound)

        cursor.execute(partition_query + ' ' + select_query)

        lower_bound = upper_bound
        upper_bound = rating * (i + 2)

    cursor.execute(MetadataConstant.METADATA_EXIST_QUERY)
    table_exist = cursor.fetchone()[MetadataConstant.TABLE_EXIST_KEY]

    if not table_exist:
        cursor.execute(MetadataConstant.CREATE_METADATA_TABLE_QUERY)
    else:
        cursor.execute('Delete From ' + MetadataConstant.METADATA_TABLE_NAME + ' Where %s = %s',
                       (MetadataConstant.ATTRIBUTE1_ALGORITHM, MetadataConstant.RANGE,))

    cursor.execute(MetadataConstant.METADATA_INSERT_QUERY, (MetadataConstant.RANGE, numberofpartitions,))

    cursor.close()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cursor = openconnection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    for i in range(numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        cursor.execute('DROP TABLE IF EXISTS ' + table_name)
        cursor.execute('CREATE TABLE ' + table_name + ' (UserID Integer, MovieID Integer, Rating Float)')

        partition_query = 'Insert into ' + table_name + ' (UserID, MovieID, Rating)'
        select_query = 'Select UserID, MovieID, Rating from ' + ratingstablename + ' Where ID % ' + \
                       str(numberofpartitions) + ' = (' + str(i + 1) + ' % ' + str(numberofpartitions) + ')'

        cursor.execute(partition_query + ' ' + select_query)

    cursor.execute(MetadataConstant.METADATA_EXIST_QUERY)
    table_exist = cursor.fetchone()[MetadataConstant.TABLE_EXIST_KEY]

    if not table_exist:
        cursor.execute(MetadataConstant.CREATE_METADATA_TABLE_QUERY)
    else:
        cursor.execute('Delete From ' + MetadataConstant.METADATA_TABLE_NAME + ' Where %s = %s',
                       (MetadataConstant.ATTRIBUTE1_ALGORITHM, MetadataConstant.ROUND_ROBIN,))

    cursor.execute(MetadataConstant.METADATA_INSERT_QUERY, (MetadataConstant.ROUND_ROBIN, numberofpartitions,))

    cursor.close()


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    rating_query = 'Insert into ' + ratingstablename + ' (UserID, MovieID, Rating) Values (' + \
                   str(userid) + ', ' + str(itemid) + ', ' + str(rating) + ' )'

    cursor.execute(rating_query)

    cursor.execute('Select * From ' + ratingstablename + ' Where UserId = ' +
                   str(userid) + ' and MovieId = ' + str(itemid) + ' and Rating = ' + str(rating))

    row_id = cursor.fetchone()['id']

    select_query = 'Select * From ' + MetadataConstant.METADATA_TABLE_NAME + ' Where ' + \
                   str(MetadataConstant.ATTRIBUTE1_ALGORITHM) + ' = %s'

    cursor.execute(select_query, (MetadataConstant.ROUND_ROBIN,))
    number_of_partition = cursor.fetchone()[MetadataConstant.ATTRIBUTE2_PARTITION]

    if row_id % number_of_partition == 0:
        insertion_table_suffix = number_of_partition - 1
    else:
        insertion_table_suffix = (row_id % number_of_partition) - 1

    table_name = RROBIN_TABLE_PREFIX + str(insertion_table_suffix)
    cursor.execute('Insert into ' + table_name + ' (UserID, MovieID, Rating) Values(%s, %s, %s)',
                   (userid, itemid, rating))

    cursor.close()


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    rating_query = 'Insert into ' + ratingstablename + ' (UserID, MovieID, Rating) Values (' + \
                   str(userid) + ', ' + str(itemid) + ', ' + str(rating) + ' )'

    cursor.execute(rating_query)
    select_query = 'Select * From ' + MetadataConstant.METADATA_TABLE_NAME + ' Where ' + \
                   str(MetadataConstant.ATTRIBUTE1_ALGORITHM) + ' = %s'

    cursor.execute(select_query, (MetadataConstant.RANGE,))
    number_of_partition = cursor.fetchone()[MetadataConstant.ATTRIBUTE2_PARTITION]
    insertion_table_suffix = -1
    for i in range(number_of_partition):
        if rating <= (5 / number_of_partition) * (i + 1):
            insertion_table_suffix = i
            break

    table_name = RANGE_TABLE_PREFIX + str(insertion_table_suffix)
    if insertion_table_suffix != -1:
        cursor.execute('Insert into ' + table_name + ' (UserID, MovieID, Rating) Values(%s, %s, %s)',
                       (userid, itemid, rating))
    else:
        print('Could not find partition for given insertion rating' + rating)

    cursor.close()


def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    if ratingMaxValue < ratingMinValue:
        print('Incorrect Input, please enter the correct input')
        return

    if path.exists(outputPath):
        os.remove(outputPath)

    cursor = openconnection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    select_query = 'Select * From ' + MetadataConstant.METADATA_TABLE_NAME + ' Where ' + \
                   str(MetadataConstant.ATTRIBUTE1_ALGORITHM) + ' = %s'

    cursor.execute(select_query, (MetadataConstant.ROUND_ROBIN,))
    number_of_partition_round_robin = cursor.fetchone()[MetadataConstant.ATTRIBUTE2_PARTITION]

    cursor.execute(select_query, (MetadataConstant.RANGE,))
    number_of_partition_range = cursor.fetchone()[MetadataConstant.ATTRIBUTE2_PARTITION]
    f = open(outputPath, 'a')

    for i in range(number_of_partition_round_robin):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        select_query = 'Select * from ' + table_name + ' Where Rating >= ' + \
                       str(ratingMinValue) + ' And Rating <= ' + str(ratingMaxValue)

        cursor.execute(select_query)
        row_value = cursor.fetchall()
        for col in row_value:
            if col:
                print(table_name + ',' + str(col['userid']) + ',' + str(col['movieid']) + ',' + str(col['rating']),
                      file=f)

    j = start_index = end_index = 0
    while j < number_of_partition_range:
        if ratingMinValue <= ((5 / number_of_partition_range) * (j + 1)):
            start_index = j
            break
        j += 1

    while j < number_of_partition_range:
        if ratingMaxValue <= ((5 / number_of_partition_range) * (j + 1)):
            end_index = j
            break
        j += 1

    while start_index <= end_index:
        table_name = RANGE_TABLE_PREFIX + str(start_index)
        select_query = 'Select * from ' + table_name + ' Where Rating >= ' + \
                       str(ratingMinValue) + ' And Rating <= ' + str(ratingMaxValue)

        cursor.execute(select_query)
        row_value = cursor.fetchall()

        for col in row_value:
            if col:
                print(table_name + ',' + str(col['userid']) + ',' + str(col['movieid']) + ',' + str(col['rating']),
                      file=f)
        start_index += 1

    f.close()
    cursor.close()


def pointQuery(ratingValue, openconnection, outputPath):
    if ratingValue < 0 or ratingValue > 5:
        print('Incorrect Input pleas enter the correct input')
        return

    if path.exists(outputPath):
        os.remove(outputPath)

    cursor = openconnection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    select_query = 'Select * From ' + MetadataConstant.METADATA_TABLE_NAME + ' Where ' + \
                   str(MetadataConstant.ATTRIBUTE1_ALGORITHM) + ' = %s'

    cursor.execute(select_query, (MetadataConstant.ROUND_ROBIN,))
    number_of_partition_round_robin = cursor.fetchone()[MetadataConstant.ATTRIBUTE2_PARTITION]

    cursor.execute(select_query, (MetadataConstant.RANGE,))
    number_of_partition_range = cursor.fetchone()[MetadataConstant.ATTRIBUTE2_PARTITION]

    f = open(outputPath, 'a')
    for i in range(number_of_partition_round_robin):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        select_query = 'Select * from ' + table_name + ' Where Rating = ' + \
                       str(ratingValue)

        cursor.execute(select_query)
        row_value = cursor.fetchall()
        for col in row_value:
            if col:
                print(table_name + ',' + str(col['userid']) + ',' + str(col['movieid']) + ',' + str(col['rating']),
                      file=f)
    j = 0
    while j < number_of_partition_range:
        if ratingValue <= ((5 / number_of_partition_range) * (j + 1)):
            break
        j += 1

    table_name = RANGE_TABLE_PREFIX + str(j)
    select_query = 'Select * from ' + table_name + ' Where Rating = ' + str(ratingValue)
    cursor.execute(select_query)
    row_value = cursor.fetchall()

    for col in row_value:
        if col:
            print(table_name + ',' + str(col['userid']) + ',' + str(col['movieid']) + ',' + str(col['rating']),
                  file=f)

    f.close()
    cursor.close()


def createDB(dbname='dds_assignment1'):
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
    con.close()


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
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
