import os, sys
import csv, time, json
from cassandra.cluster import Cluster
from random import randint

RECORD_COUNT = 100000
genreList = ["Autobiography","Adventure","Classics","Comic","Crime Fiction","Fantasy", "Fiction", "Horror", "History", "Poetry", "Politics", "Travel", "Vampires"]
langList = ["English (US)","French","Spanish","Hindi","Japanese","English (UK)"]
isbnList = []
updateKeyList = []
updateCount = 0

cluster = Cluster()
session = cluster.connect('readaloud')

def readBookMeta():
	langIndex = randint(0,5)
	csql = ("select * from books_metadata where language = '"+ str(langList[langIndex]) +"' ALLOW FILTERING")
	session.execute(csql)

	
def updateBookMeta():
	randIndex = randint(0,700)
	bound_statement = session.prepare("update books_metadata set language = ? where ISBN = ? and genre = ? and author = ?")
	genreIndex = randint(0,12)
	langIndex = randint(0,5)
	resultset = session.execute(bound_statement.bind((langList[langIndex],keyListData[randIndex]["ISBN"],keyListData[randIndex]["genre"],keyListData[randIndex]["author"])))
	
def mainMethod():	
	opCount = 0
	readTime =0 
	writeTime =0
	while opCount < RECORD_COUNT:
		startTime = int(round(time.time() * 1000))
		updateBookMeta()
		writeTime += (int(round(time.time() * 1000)) - startTime)
		'''if opCount%2 ==0:
			startTime = int(round(time.time() * 1000))
			readBookMeta()
			readTime += (int(round(time.time() * 1000)) - startTime)
		else:
			startTime = int(round(time.time() * 1000))
			updateBookMeta()
			writeTime += (int(round(time.time() * 1000)) - startTime)'''
		opCount+=1
	#print "readTime === " + str(readTime)
	print "writeTime === " + str(writeTime)

	
keyListFile = open("E:/Semester II/CMPE226/Project 2/scripts/keyList.txt","r")
keyListData = json.load(keyListFile)	
mainMethod()
	
#updateBookMeta()
#readBookMeta()