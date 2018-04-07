from sklearn.cluster import KMeans
import numpy as np

"""
Re-implemented version of Zaman's auto-indexing clustering algorithm in Python.
Originally written in Java.
Thesis: http://ieeexplore.ieee.org/abstract/document/1333569/
"""

"""Initialized variables"""
int numOfAttrs, numOfQueries

#determined from the relationship between increase in table scan cost & performance gain due to re-indexing
#use integrated postgresql workload
#if(workload pattern has changed enough to hit our threshold):
	#currentIndexSet = DB.getIndexSet()		#get current index set
	#data = log.in() 						#read input from log on queries

	#numOfAttrs = data.getAllUniqueAttributes() 	#number of total attributes from all tables
	#numOfQueries = data.getAllUniqueQueries()		#number of total unique queries read from log
	if numOfAttrs is None or numOfQueries is None:
		print("ERROR: Unable to retrieve all unique attributes/queries. Aborting.")
		raise SystemError

#query-attribute matrix
queryAttrMatrix = np.ndarray(shape=(numOfQueries,numOfAttrs), dtype=int)

#query-frequency matrix, 2 extra rows for freq, freq*T
queryFreqMatrix = np.ndarray(shape=(numOfQueries+2,numOfAttrs), dtype=int)

#populate query-attr matrix for clustering

#populate query-freq matrix
For each row i in matrix M:
	For each column j in matrix M:
		If attribute is present in query i:
			Set M[i][j] = 1 #indexable attribute
		Else:
			Set M[i][j] = 0

#finds which queries should be clustered together - AI PART
#explain in presentation
Clusters = kmeans(m) 				#example entries: [(Q1,Q4), Q3, (Q2, Q5)], array of tuples

#add frequency totals to query-freq matrix
m.append(numOfQueries+1, 0) #assumes indexing at 0
For each column j in matrix M:
	Sum the rest of the values from the array M[j]
	Store sum value in m[0]

#add frequency*T totals to query-freq matrix
m.append(numOfQueries+2, 0) #assumes indexing at 0
For each column j in matrix M:
	Sum the rest of the values from the array M[j] * M[j].getTable().getNumOfRows()
	Store sum*T value in m[0]

newIndexSet = Find common candidate indexable attributes across all clusters/queries

#filter out index sets
#create new indexes which aren’t in old index set
For index i in newIndexSet:
    if i not in currentIndexSet:
   	 create new index i

#delete old indexes which aren’t in new index set
For index j in currentIndexSet:
    if j not in newIndexSet:
   	 drop old index j

#postgresql planner/optimizer 43.5
Pass newIndexSet to PostgreSQL optimizer to have it choose from set of hypothetical indexes, outputs choice/cost estimate for each one

#from what the optimizer recommends, create indexes from newIndexSet 
