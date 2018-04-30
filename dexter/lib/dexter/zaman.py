from sklearn.cluster import KMeans
import numpy as np
import psycopg2
import json
import pprint

"""
Re-implemented version of Zaman's auto-indexing clustering algorithm in Python.
Originally written in Java.
Authors: Emily Black, Dragon Tran
Resource: http://ieeexplore.ieee.org/abstract/document/1333569/
"""


# retrieve and parse queries json files from dexter
def get_queries():
    queries = json.load(open('queries.json'))

    # new list for relevent select queries
    relevent_queries = []

    for query in queries:
        # parse through queries list for only select queries
        if query.split(' ', 1)[0] == "select":
            relevent_queries.append(query)

    return relevent_queries


# retrieve and parse tables json files from dexter
# then query the db to retrieve all attribute names
def get_attributes():
    ## hardcoded the attributes to test locally
    # hardcode = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
    #             'o_clerk', 'o_shippriority', 'o_comment', 'l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber',
    #             'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus',
    #             'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment',
    #             'ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment', 'c_custkey', 'c_name',
    #             'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment', 's_suppkey',
    #             's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment', 'n_nationkey', 'n_name',
    #             'n_regionkey', 'n_comment', 'r_regionkey', 'r_name', 'r_comment', 'p_partkey', 'p_name', 'p_mfgr',
    #             'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice', 'p_comment']

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

    # close db connection
    conn.close()

    return attributes


# Main method
def main():
    """Initialized variables"""
    # parse data from json files exported from dexter
    logData = get_queries()
    attributes = get_attributes()

    numOfAttrs = len(attributes)

    numOfQueries = len(logData)

    # other computational variables
    rowIndex = 0
    columnIndex = 0
    newIndexset = []
    # define thresholds to determine indexable candidate attributes
    thresholdOne = numOfQueries / 2
    # thresholdTwo = numOfQueries / 4

    if numOfAttrs is None or numOfQueries is None:
        print("ERROR: Unable to retrieve all unique attributes or cannot query input is none. Aborting.")
        raise SystemError

    # query-attribute matrix
    queryAttrMatrix = np.ndarray(shape=(numOfQueries, numOfAttrs), dtype=int)

    # query-frequency matrix, 2 extra rows for freq, freq*T
    queryFreqMatrix = np.ndarray(shape=(numOfQueries + 1, numOfAttrs), dtype=int)

    # populate query-attr matrix for clustering (will contain 1's and 0's)
    for query in logData:
        for attr in attributes:
            # found occurrence
            if query.find(attr) != -1:
                np.insert(queryAttrMatrix, [rowIndex, columnIndex], '1')
            # didn't find occurrence
            else:
                np.insert(queryAttrMatrix, [rowIndex, columnIndex], '0')
            columnIndex += 1
        rowIndex += 1

        # reset our index for query-freq matrix insertion
    rowIndex = 0
    columnIndex = 0

    # populate query-freq matrix for freq calculations
    for query in logData:
        for attr in attributes:
            # found occurrence
            if query.find(attr) != -1:
                np.insert(queryFreqMatrix, [rowIndex, columnIndex], query.count(attr))
            # didn't find occurrence
            else:
                np.insert(queryFreqMatrix, [rowIndex, columnIndex], '0')
            columnIndex += 1
        rowIndex += 1

    rowIndex = 0
    columnIndex = 0

    # add frequency totals to query-freq matrix
    while columnIndex != numOfAttrs - 1:
        np.insert(queryFreqMatrix, [numOfQueries + 1, columnIndex], queryFreqMatrix.sum(axis=columnIndex))
        columnIndex += 1

    # reset our index for query-freq matrix insertion
    # columnIndex = 0

    # Can't do this atm, can get number of rows for each table
    # add frequency*T totals to query-freq matrix
    # while columnIndex != numOfAttrs-1:
    # numOfRows = attributes[columnIndex].getNumOfRowsInTable
    # np.insert(queryFreqMatrix, queryFreqMatrix.size-1, queryFreqMatrix.sum(axis=columnIndex) * numOfRows, axis=columnIndex)
    # columnIndex += 1

    # reset to use as random counter
    columnIndex = 0

    # compare frequencies to thresholds
    while columnIndex != numOfAttrs and columnIndex != numOfQueries:
        if queryFreqMatrix[numOfQueries + 1, columnIndex] < thresholdOne:
            np.delete(queryAttrMatrix, axix=columnIndex)
            columnIndex += 1

    # cluster queries together based on relatively similar mentioned attributes - Artificial intelligence part
    # number of clusters = k = n_clusters
    # Zaman's research concluded that 3 was optimal k value
    # example: [(Q1,Q4), Q3, (Q2, Q5)], array of tuples
    clusterResults = KMeans(n_clusters=3).fit(queryAttrMatrix)

    # Finds the most common candidate indexable attributes across all clusters/queries
    for i in clusterResults:
        mostFreqValue = np.bincount(clusterResults[i]).argmax()
    newIndexset.append(mostFreqValue)
    i += 1

    # return new index set by converting list to json and printing to console
    # dexter will retrieve console output as a string where it will be able to parse the json output
    print(json.dumps(newIndexset))

    # return newIndexset


if __name__ == "__main__":
    main()
