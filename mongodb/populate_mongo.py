import os, sys
import csv, time
from pymongo import MongoClient
from random import randint

RECORD_COUNT = 10

file = "E:/Semester II/CMPE226/Project 2/data/Books.csv"
path = "E:/Semester II/CMPE226/Project 2/data/test"

connection = MongoClient("mongodb://localhost:27017")

genreList = ["Autobiography","Adventure","Classics","Comic","Crime Fiction","Fantasy", "Fiction", "Horror", "History", "Poetry", "Politics", "Travel", "Vampires"]
langList = ["English (US)","French","Spanish","Hindi","Japanese","English (UK)"]
filenames = []

book_record={}

def populateBooksMongoMeta(book_record):
	db = connection.readaloud.books_metadata
	db.insert(book_record)
	
def populateBooksMongoContent(book_record):
	db = connection.readaloud.books_content
	db.insert(book_record)
	

def readCSV():
	failureCounter = 0 
	counter = 1 
	failCount = 0
	metaWriteTime =0 
	data = open(file)
	input_file = csv.DictReader(data,delimiter=";",quotechar='"')
	for row in input_file:
		if counter > RECORD_COUNT:
			break
		#print row
		genreIndex = randint(0,12)
		langIndex = randint(0,5)
		
		try:
			book_record={'_id':counter,'ISBN':row["ISBN"], 'title': row["Book-Title"] , 'author' : row["Book-Author"] , 'year_of_publication': row["Year-Of-Publication"] , 'publisher': row["Publisher"], 'genre':genreList[genreIndex],'language':langList[langIndex]}
			startTime = int(round(time.time() * 1000))
			populateBooksMongoMeta(book_record)
			metaWriteTime += (int(round(time.time() * 1000)) - startTime)
			counter +=1
		except : 
			failCount +=1
			#print "Row Ignored"
	print "Time for metadata  :" + str(metaWriteTime)
				
def populateContent():
	failureCounter = 0 
	counter = 0 
	failCount = 0
	contentWriteTime=0
	
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
			book_record={"ISBN":row["ISBN"],"content":input_file.read()}
			populateBooksMongoContent(book_record)
			contentWriteTime = contentWriteTime + (int(round(time.time() * 1000)) - startTime)
		except:
			failCount +=1
	print "Time for content   :" + str(contentWriteTime)

dirs = os.listdir(path)
for f in dirs:
	filenames.append(f)


print "******************Mongo*********************"
print "Total Records      :" + str(RECORD_COUNT)
#readCSV()
populateComments()
populateContent()
print "************************************************"

connection.close()



