import csv
from pymongo import MongoClient
from random import randint 	


userIdFile = "E:/Semester II/CMPE226/Project 2/scripts/mongodb/userIds.txt"
stringFilePath = "E:/Semester II/CMPE226/Project 2/data/randomCommets.csv"
bookIsbnFilepath = "E:/Semester II/CMPE226/Project 2/data/books_metadata.csv"
connection = MongoClient("mongodb://localhost:27017")


stringData = [] 
bookIds = []
userIds = []

def populateComments():
	db = connection.readaloud.books_metadata
	
	for bookId in bookIds:
		print bookId
		comments_group = []
		size = randint(1,10)
		count =0
		while count < size:
			comments = {"userId" : userIds[randint(0,len(userIds)-1)],"comment" : stringData[randint(0,len(stringData)-1)]}
			comments_group.append(comments)
			count +=1
		db.update({"ISBN":bookId},{"$set":{"comments":comments_group}})


data = open(stringFilePath)
input_file = csv.DictReader(data,delimiter=",",quotechar='"')	
for row in input_file:
	stringData.append(row["comments"])
print "Comments loaded"



data = open(bookIsbnFilepath)
input_file = csv.DictReader(data,delimiter=",",quotechar='"')	
for row in input_file:
	bookIds.append(row["isbn"])
print "Books Ids loaded"

data = open(userIdFile)
input_file = csv.DictReader(data,delimiter=",",quotechar="'")	
for row in input_file:
	userIds.append(row["user_uuid"])

populateComments()

