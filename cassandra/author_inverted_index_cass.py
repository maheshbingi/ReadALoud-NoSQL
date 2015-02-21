import os, sys
import csv, time
import json
from cassandra.cluster import Cluster
from random import randint

RECORD_COUNT = 10

file = "E:/Semester II/CMPE226/Project 2/data/books_metadata.csv"

authorList = []
genreList = []
languageList = []
cluster = Cluster()
session = cluster.connect('readaloud')

def readCSV():
	metaWriteTime =0 
	counter = 1 
	failCount = 0 
	data = open(file)
	
	input_file = csv.DictReader(data, delimiter=",")
	for row in input_file:
		if counter > RECORD_COUNT:
			break
		author = row["author"]
		genre = row["genre"]
		language = row["language"]
		try :
			if author not in authorList:
				authorList.append(author)
				csql = "insert into author_inverted_index (author, isbn_genre) values ('" + author + "',{'" + str(row["isbn"]) +"'})"
				session.execute(csql)
			else :
				csql = "update author_inverted_index set isbn_genre = isbn_genre + {'" + str(row["isbn"]) +"'} where author = '" + author + "'"
				session.execute(csql)
			if genre not in genreList:
				genreList.append(genre)
				csql = "insert into genre_inverted_index (genre, isbn_genre) values ('" + genre + "',{'" + str(row["isbn"]) +"'})"
				session.execute(csql)
			else :
				csql = "update genre_inverted_index set isbn_genre = isbn_genre + {'" + str(row["isbn"]) +"'} where genre = '" + genre + "'"
				session.execute(csql)
			if language not in languageList:
				languageList.append(language)
				csql = "insert into language_inverted_index (language, isbn_genre) values ('" + language + "',{'" + str(row["isbn"]) +"'})"
				session.execute(csql)
			else :
				csql = "update language_inverted_index set isbn_genre = isbn_genre + {'" + str(row["isbn"]) +"'} where language = '" + language + "'"
				session.execute(csql)
			counter +=1
		except:
			failCount +=1

readCSV()