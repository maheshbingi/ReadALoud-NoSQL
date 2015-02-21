import uuid,json, csv,uuid
from random import randint
from cassandra.cluster import Cluster

stringFilePath = "E:/Semester II/CMPE226/Project 2/data/randomCommets.csv"
userIdFile = "E:/Semester II/CMPE226/Project 2/data/userIds.txt"
categoryList = ["Autobiography","Adventure","Classics","Comic","Crime Fiction","Fantasy", "Fiction", "Horror", "History", "Poetry", "Politics", "Travel", "Vampires"]

RECORD_COUNT = 10

stringData = []
userIds = []
groupIds = []

cluster = Cluster()
session = cluster.connect('readaloud')

def populateGroups():
	recordCounter = 0
	count =0
	while recordCounter < RECORD_COUNT:
		idx = randint(0,len(categoryList)-1)
		stringIdx = randint(0,len(stringData) - 1)
		usrIdx = randint(0,len(userIds)-1)
		
		groupId = uuid.uuid1()
		groupIds.append(groupId)
		
		size = randint(1,20)
		queryUserIds = ""
		while count < size:
			if queryUserIds == "" :
				queryUserIds = userIds[randint(0,len(userIds)-1)]
			else:
				queryUserIds  = queryUserIds + "," + userIds[randint(0,len(userIds)-1)]
			count +=1
		csql = "insert into groups (id, name,owner_id,category,description,user_id) values (" + str(groupId) +",'" + categoryList[idx] + "'," +str(userIds[usrIdx])+ ",'" + categoryList[idx] + "','" + str(stringData[stringIdx]) + "',{" + queryUserIds + "}) "
		print csql
		session.execute(csql)
		recordCounter +=1

	file = open('groupId.txt', 'w')
	file.write(str(groupId))
	

data = open(stringFilePath)
input_file = csv.DictReader(data,delimiter=",",quotechar='"')	
for row in input_file:
	stringData.append(row["comments"])
print "comments loaded"
	
data = open(userIdFile)
input_file = csv.DictReader(data,delimiter=",",quotechar="'")	
for row in input_file:
	userIds.append(row["user_uuid"])
	
print "userIds loaded"

	
populateGroups()	



