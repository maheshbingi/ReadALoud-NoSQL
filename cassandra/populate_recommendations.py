import uuid,json, csv,uuid
from random import randint
from cassandra.cluster import Cluster


stringFilePath = "E:/Semester II/CMPE226/Project 2/data/randomCommets.csv"
userIdFile = "E:/Semester II/CMPE226/Project 2/data/userIds.txt"
file = "E:/Semester II/CMPE226/Project 2/data/books_metadata.csv"

RECORD_COUNT = 10

stringData = []
userIds = []
bookIds = []

cluster = Cluster()
session = cluster.connect('readaloud')

def populateRecommendations():
	recordCounter = 0
	count =0
	while recordCounter < RECORD_COUNT:
		stringIdx = randint(0,len(stringData) - 1)
		
		recommendationId = uuid.uuid1()
		usrIdx = randint(0,len(userIds)-1)
		from_user_id = userIds[usrIdx]
		
		usrIdx = randint(0,len(userIds)-1)
		to_user_id = userIds[usrIdx]
		
		bookIdIdx = randint(0,len(bookIds)-1)
		
		csql = "insert into recommendations (id, from_user_id, to_user_id,book_id,comments) values (" + str(recommendationId) +"," + str(from_user_id) + "," +str(to_user_id)+ ",'" + bookIds[bookIdIdx] + "','" + str(stringData[stringIdx]) + "') "
		print csql
		session.execute(csql)
		recordCounter +=1
		
		
data = open(file)
input_file = csv.DictReader(data, delimiter=",")
for row in input_file:
	bookIds.append(row["isbn"])
	
		
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


populateRecommendations()

