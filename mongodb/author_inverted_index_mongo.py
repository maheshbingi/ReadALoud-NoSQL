import os, sys
import csv, time
from pymongo import MongoClient
from random import randint

RECORD_COUNT = 10

file = "E:/Semester II/CMPE226/Project 2/data/books_metadata.csv"

connection = MongoClient("mongodb://localhost:27017")

genreList = ["Autobiography","Adventure","Classics","Comic","Crime Fiction","Fantasy", "Fiction", "Horror", "History", "Poetry", "Politics", "Travel", "Vampires"]
langList = ["English (US)","French","Spanish","Hindi","Japanese","English (UK)"]
book_record={}

def populateAuthorInvertedIndex(row):
	db = connection.readaloud.author_inverted_index
	db.update({ "Author" : row["author"]}, { "$push": { "ISBN": row["isbn"] } }, upsert=True)

def populateGenreInvertedIndex(row):
	db = connection.readaloud.genre_inverted_index
	db.update({ "Genre" : row["genre"] }, { "$push": { "ISBN": row["isbn"] } }, upsert=True)

def populateLangInvertedIndex(row):
	db = connection.readaloud.language_inverted_index
	db.update({ "Language" : row["language"] }, { "$push": { "ISBN": row["isbn"] } }, upsert=True)


def readCSV():
	counter = 1 
	failCount = 0
	data = open(file)
	input_file = csv.DictReader(data, delimiter=",")
	for row in input_file:
		if counter > RECORD_COUNT:
			break
		try:
			populateAuthorInvertedIndex(row)
			populateGenreInvertedIndex(row)
			populateLangInvertedIndex(row)
			counter +=1
		except : 
			failCount +=1


		
startmillis = int(round(time.time() * 1000))
readCSV()
totalTimeMeta = int(round(time.time() * 1000)) - startmillis
print(totalTimeMeta)

connection.close()



