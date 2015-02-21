import os, sys
import csv, time
import json
from cassandra.cluster import Cluster
from random import randint

RECORD_COUNT = 10
filenames = []

contentWriteTime =0 
book_record={}

file = "E:/Semester II/CMPE226/Project 2/data/Books.csv"
path = "E:/Semester II/CMPE226/Project 2/data/test"

genreList = ["Autobiography","Adventure","Classics","Comic","Crime Fiction","Fantasy", "Fiction", "Horror", "History", "Poetry", "Politics", "Travel", "Vampires"]
langList = ["English (US)","French","Spanish","Hindi","Japanese","English (UK)"]
keyList = []

#KEYSPACE initialization
cluster = Cluster()
session = cluster.connect('readaloud')


def readCSV():
	metaWriteTime =0 
	counter = 1 
	failCount = 0 
	data = open(file)
	input_file = csv.DictReader(data,delimiter=";",quotechar='"')
	bound_statement= session.prepare("INSERT INTO books_meta (ISBN, publisher, author, language,title,genre,year_of_publication) VALUES (?,?,?,?,?,?,?)")
	for row in input_file:
		if counter > RECORD_COUNT:
			break
		genreIndex = randint(0,12)
		langIndex = randint(0,5)
		
		try:
			keys = {"ISBN": row["ISBN"], "genre" : genreList[genreIndex], "author" : row["Book-Author"]}
			keyList.append(keys)
			
			startTime = int(round(time.time() * 1000))
			session.execute(bound_statement.bind((row["ISBN"],row["Publisher"],row["Book-Author"],langList[langIndex],row["Book-Title"],genreList[genreIndex],row["Year-Of-Publication"])))
			metaWriteTime += (int(round(time.time() * 1000)) - startTime)
			counter +=1
			
		except :
			failCount +=1
	with open('keyList.txt', 'w') as outfile:
		json.dump(keyList,outfile)
	#keyFile = open("keyList.js",'w')
	#keyFile.write(str(keyList))
	
	#ff = open("E:/Semester II/CMPE226/Project 2/scripts/keyList.txt","r")
	#aa = json.load(ff)
	#print aa[0]["genre"]
	print "Time for metadata  :" + str(metaWriteTime)

def populateContent():
	failureCounter = 0 
	counter = 0 
	failCount = 0
	contentWriteTime=0
	#while counter < RECORD_COUNT :
	data = open(file)
	input_file = csv.DictReader(data,delimiter=";",quotechar='"')
	for row in input_file:
		if counter > RECORD_COUNT :
			break
		filename = path + "/"+ filenames[randint(0,1300)]
		try:
			input_file = open(filename,'r')
			counter +=1
			startTime = int(round(time.time() * 1000))
			bound_statement= session.prepare("insert into books_content(ISBN, content) values (?,?)")
			session.execute(bound_statement.bind((row["ISBN"],input_file.read())))
			contentWriteTime = contentWriteTime + (int(round(time.time() * 1000)) - startTime)
		except:
			failCount +=1
			#print "Ignored" + str(sys.exc_info()[0])
	print "Time for content   :" + str(contentWriteTime)

dirs = os.listdir(path)
for f in dirs:
	filenames.append(f)



print "******************Cassandra*********************"
print "Total Records      :" + str(RECORD_COUNT)
readCSV()
populateContent()
print "************************************************"
