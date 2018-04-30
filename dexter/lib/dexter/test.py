import psycopg2
import json
import pprint

# retrieve and parse tables json files from dexter
# then query the db to retrieve all attribute names
def get_attributes():
    # parse tables json file to get db tables from dexter
    tables = json.load(open('tables.json'))

    attributes = []

    # connection info
    conn_string = "host='localhost' port=15432 dbname='tpch' user='postgres' password='postgres'"

    # connect to db
    conn = psycopg2.connect(conn_string)
    # connection cursor to perform queries
    cursor = conn.cursor()

    print("Connected!\n")

    # execute query
    for table in tables:
        query = "SELECT * FROM %s WHERE FALSE" % table
        cursor.execute(query)

        # get all attribute names in table
        for i in range(len(cursor.description)):
            attributes.append(cursor.description[i][0])

    conn.close()

    print(attributes)
    return attributes

get_attributes()