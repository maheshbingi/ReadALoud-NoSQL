import json, csv
from random import randint
from pymongo import MongoClient


stringFilePath = "E:/Semester II/CMPE226/Project 2/data/randomCommets.csv"
bookIsbnFilepath = "E:/Semester II/CMPE226/Project 2/data/books_metadata.csv"
userIdFile = "E:/Semester II/CMPE226/Project 2/scripts/mongodb/userIds.txt"


stringData = []
userIds = []
groupIds = []
bookIds = []

RECORD_COUNT = 10
connection = MongoClient("mongodb://localhost:27017")

def populateRecommend():
	counter = 0;
	for userId in userIds:
		print userId
		#if counter > RECORD_COUNT:
		#	break
		db = connection.readaloud.users
		db.update({ "_id" : userId}, {"$set":{"recommendations" : {"to" : [ {"userId" : userIds[randint(0,len(userIds) - 1)],"bookId" : bookIds[randint(0,len(bookIds)-1)], "comments" : stringData[randint(0,len(stringData) - 1)]}],"from" : [ {"userId" : userIds[randint(0,len(userIds) - 1)], "bookId" : bookIds[randint(0,len(bookIds)-1)], "comments" : stringData[randint(0,len(stringData) - 1)]}]}}},upsert=True)
		counter += 1

data = open(userIdFile)
input_file = csv.DictReader(data,delimiter=",",quotechar='"')	
for row in input_file:
	userIds.append(row["user_uuid"])
print "user ids loaded"

data = open(bookIsbnFilepath)
input_file = csv.DictReader(data, delimiter=",")
for row in input_file:
	bookIds.append(row["isbn"])
print "books ids loaded"
	
data = open(stringFilePath)
input_file = csv.DictReader(data,delimiter=",",quotechar='"')	
for row in input_file:
	stringData.append(row["comments"])
print "comments loaded"

populateRecommend()
