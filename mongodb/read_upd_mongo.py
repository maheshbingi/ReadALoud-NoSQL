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
isbnList = []
book_record={}

data = open(file)
input_file = csv.DictReader(data,delimiter=";",quotechar='"')
counter = 0
for row in input_file:
	if counter > RECORD_COUNT:
		break
	counter += 1
	isbnList.append(row["ISBN"])


def readBookMeta():
	genreIndex = randint(0,12)
	startmillis = int(round(time.time() * 1000))
	db = connection.readaloud.books_metadata
	db.find({ "genre" : str(genreList[genreIndex])})
	totalmillis = (int(round(time.time() * 1000)) - startmillis)
	#print("Read done.." + str(totalmillis))


def updateBookMeta():
	isbnIndex = randint(0,RECORD_COUNT -1)
	langIndex = randint(0,5)
	db = connection.readaloud.books_metadata
	db.update({ "ISBN" : str(isbnList[isbnIndex])},{"language" : langList[langIndex]})
	#updateCount += 1
#	print("Updated::")

def mainMethod():	
	opCount = 0
	readTime =0 
	writeTime =0
	while opCount < RECORD_COUNT:
		startTime = int(round(time.time() * 1000))
		readBookMeta()
		readTime += (int(round(time.time() * 1000)) - startTime)
		'''if opCount%2 ==0:
			startTime = int(round(time.time() * 1000))
			readBookMeta()
			readTime += (int(round(time.time() * 1000)) - startTime)
		else:
			startTime = int(round(time.time() * 1000))
			updateBookMeta()
			writeTime += (int(round(time.time() * 1000)) - startTime)
		'''
		opCount+=1
	print "ReadTime === " + str(readTime)
	print "WriteTime === " + str(writeTime)

mainMethod()


