#determined from the relationship between increase in table scan cost & performance gain due to re-indexing
#postgresql workload
if(workload pattern has changed enough to hit our threshold):
currentIndexSet = DB.getIndexSet()
data = log.in() 						#read input from log on queries

Int numOfAttrs = data.getAllUniqueAttributes() 	#number of total attributes from all tables

Int numOfQueries = data.getAllUniqueQueries()	#number of total unique queries read from log

Matrix m [numOfQueries] [numOfAttrs]		#query-freq matrix

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
