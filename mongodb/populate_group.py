import csv,uuid
from random import randint
from pymongo import MongoClient

stringFilePath = "E:/Semester II/CMPE226/Project 2/data/randomCommets.csv"
userIdFile = "E:/Semester II/CMPE226/Project 2/scripts/mongodb/userIds.txt"
categoryList = ["Autobiography","Adventure","Classics","Comic","Crime Fiction","Fantasy", "Fiction", "Horror", "History", "Poetry", "Politics", "Travel", "Vampires"]

RECORD_COUNT = 10

stringData = []
userIds = []
groupIds = []
connection = MongoClient("mongodb://localhost:27017")


def populateGroups():
	recordCounter = 0
	count =0
	while recordCounter < RECORD_COUNT:
		idx = randint(0,len(categoryList)-1)
		stringIdx = randint(0,len(stringData) - 1)
		usrIdx = randint(0,len(userIds)-1)
		
		groupId = str(uuid.uuid1())
		groupIds.append(groupId)
		
		size = randint(1,20)
		queryUserIds = ""
		while count < size:
			if queryUserIds == "" :
				queryUserIds = userIds[randint(0,len(userIds)-1)]
			else:
				queryUserIds  = queryUserIds + "," + userIds[randint(0,len(userIds)-1)]
			count +=1
		db = connection.readaloud.groups
		group_record = {"_id" : str(groupId), "name":categoryList[idx],"owner_id":str(userIds[usrIdx]),"category":categoryList[idx],"description":stringData[stringIdx],"user_id":[queryUserIds]}
		db.insert(group_record)
	
		recordCounter +=1
	
	for userId in userIds:
		size = randint(1,20)
		print size
		usersInGroupIds = ""
		count =0
		while count < size:
			if usersInGroupIds == "" :
				usersInGroupIds = groupIds[randint(0,len(groupIds)-1)]
			else:
				usersInGroupIds  = usersInGroupIds + "," + groupIds[randint(0,len(groupIds)-1)]
			count +=1
		print usersInGroupIds
		db = connection.readaloud.users
		print usersInGroupIds
		db.update({"_id":userId},{"$set":{"groups":[usersInGroupIds]}})

	

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



