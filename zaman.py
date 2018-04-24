from sklearn.cluster import KMeans
import numpy as np

"""
Re-implemented version of Zaman's auto-indexing clustering algorithm in Python.
Originally written in Java.
Authors: Emily Black
Thesis: http://ieeexplore.ieee.org/abstract/document/1333569/
"""

"""Initialized variables"""
inputParameters = str(sys.argv) #reads parameter input from Ruby

logData = inputParameters[0]
numOfAttrs = 0
numOfQueries = 0
rowIndex = 0
columnIndex = 0
numOfRows = 0
thresholdOne = 0
thresholdTwo = 0
attributes = []
currentIndexSet = []
newIndexset = []
indexesToAdd = []
indexesToRemove = []



#currentIndexSet = DB.getIndexSet()		    #get current index set
	#wlSize = getCurrentWorkload()              #workload size
	#logData = log.in() 						#read input from log on queries in the form of an ARRAY

	#numOfAttrs = DB.countAllUniqueAttributes() 		#number of total attributes from all tables
	#numOfQueries = logData.countAllUniqueQueries()		#number of total unique queries read from log
	#attributes = DB.getAllUniqueAttributes()			#list of all unique attributes
	if numOfAttrs != len(attributes.length):
		print("ERROR: Number of attributes and actual attributes size mismatched. Aborting.")
		raise SystemError
	if numOfAttrs is None or numOfQueries is None:
		print("ERROR: Unable to retrieve all unique attributes/queries. Aborting.")
		raise SystemError

#define thresholds to determine indexable candidate attributes
thresholdOne = wlSize/2
thresholdTwo = wlSize/4
		
#query-attribute matrix, index of QUERIES starts at 1
queryAttrMatrix = np.ndarray(shape=(numOfQueries,numOfAttrs), dtype=int)

#query-frequency matrix, 2 extra rows for freq, freq*T, index of QUERIES starts at 1
queryFreqMatrix = np.ndarray(shape=(numOfQueries+2,numOfAttrs), dtype=int)

#populate query-attr matrix for clustering (will contain 1's and 0's)
for query in logData:
	for attr in attributes:
		#found occurrence
		if query.find(attr) != -1:
			np.insert(queryAttrMatrix, rowIndex, '1', axis=rowIndex)
		#didn't find occurrence
		else:
			np.insert(queryAttrMatrix, rowIndex, '0', axis=rowIndex)
		rowIndex += 1

#reset our index for query-freq matrix insertion
rowIndex = 0

#populate query-freq matrix for freq calculations
for query in logData:
	for attr in attributes:
		#found occurrence
		if query.find(attr) != -1:
			np.insert(queryFreqMatrix, rowIndex, query.count(attr), axis=rowIndex)
		#didn't find occurrence
		else:
			np.insert(queryAttrMatrix, rowIndex, '0', axis=rowIndex)
		rowIndex += 1

#reset our index for next workload change
rowIndex = 0

#cluster queries together based on relatively similar mentioned attributes - Artificial intelligence part
#number of clusters = k = n_clusters
#example: [(Q1,Q4), Q3, (Q2, Q5)], array of tuples
clusterResults = KMeans(n_clusters=3).fit(queryAttrMatrix)

#add frequency totals to query-freq matrix
while columnIndex != numOfAttrs-1:
	np.insert(queryFreqMatrix, queryFreqMatrix.size-2, queryFreqMatrix.sum(axis=columnIndex), axis=columnIndex)
	columnIndex += 1

#reset our index for query-freq matrix insertion
columnIndex = 0

#add frequency*T totals to query-freq matrix
while columnIndex != numOfAttrs-1:
	#numOfRows = attributes[columnIndex].getNumOfRowsInTable
	np.insert(queryFreqMatrix, queryFreqMatrix.size-1, queryFreqMatrix.sum(axis=columnIndex) * numOfRows, axis=columnIndex)
	columnIndex += 1

#use thresholds here
#newIndexSet = Find common candidate indexable attributes across all clusters/queries

#filter out index sets
#create new indexes which aren’t in old index set
for index in newIndexSet:
    if index not in currentIndexSet:
   		#indexesToAdd.append(new index i)

#delete old indexes which aren’t in new index set
for index in currentIndexSet:
    if index not in newIndexSet:
   		indexesToRemove.append(index)

#postgresql planner/optimizer 43.5
#Pass newIndexSet to PostgreSQL optimizer to have it choose from set of hypothetical indexes, outputs choice/cost estimate for each one

#from what the optimizer recommends, create indexes from newIndexSet 
