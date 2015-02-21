import uuid,json, csv
from random import randint
from cassandra.cluster import Cluster
path = "E:/Semester II/CMPE226/Project 2/data/randomCommets.csv"
bookIsbnFilepath = "E:/Semester II/CMPE226/Project 2/data/books_metadata.csv"
firstNames = ["Armand","Brock","Hakeem","Kristen","Hiroko","Shoshana","Beatrice","Herrod","Diana","Hollee","Irene","Davis","Aphrodite","Reed","Dieter","Sigourney","Myra","Thaddeus","Bevis","Zachery","Byron","Colby","Theodore","Nita","Ryan","Amethyst","Shelly","Moana","Veronica","Paloma","Gannon","Fuller","Calvin","Nevada","Yvette","William","Gareth","Jeanette","Colby","Xena","Emery","Kylee","Clark","Jakeem","Signe","Belle","Jonah","Breanna","Beau","Troy","Burke","Anjolie","Mercedes","Beck","Geoffrey","Brooke","Hunter","Orson","Kelly","Jared","Perry","Sebastian","Darrel","Malcolm","Hashim","Naomi","Zelda","Caldwell","Zeus","Xandra","Julie","Summer","Cameron","Zeus","Susan","Cleo","Quintessa","MacKensie","Elmo","Breanna","Ila","Tamekah","Germaine","Kibo""Sylvester","Kristen","Kiayada","Destiny","Lucy","Zena","Coby","Alexa","Zephr","Brennan","Orli","Stephanie","Gabriel","Guinevere","Charity","Branden"]

lastNames = ["Robertson","Good","Hurley","Ortiz","Fowler","Hatfield","Salas","Armstrong","Talley","Blake","Vazquez","Ingram","Ray","Cole","Thompson","Potter","Bird","Mcfarland","Herman","Mathis","Baron","Soto","Gilbert","Rojas","Gibbs","Tucker","Goff","Crosby","Weiss","Preston","Beach","Raymond","Simon","Blackwell","Cabrera","Lancaster","Nielsen","Santana","Chan","Mitchell","Gardnr","Harmon","Haynes","Hurst","Fuentes","Ramsey","Copeland","Murray","York","Torres","Ware","Carver","Collins","Hickman","Bush","Griffin","Sawyer","Hendrix","Spence","Dillard","Hamilton""Blackwell","Fernandez","Gill","Santos","Morse","Fuller","Lopez","Rodriguez","Robles","Huff","Chang","Hull","Hinton","Mueller","Delgado","Christensen","Stein","Hopper","Gonzales","Cunningham","hite","Franco","Graves","Pickett","Langley","Aguirre","Castro","May","Lindsey","Hester","Flores","Cantu","Edwards","York","Horn","Ramirez","Gamble","Kim","Russell"]

stringData =[]

userType = ["reader","author"]
userIds = []
groupIds = []
bookIds = []

RECORD_COUNT = 10

cluster = Cluster()
session = cluster.connect('readaloud')

def populateUsers():
	counter = 0;
	while counter < RECORD_COUNT:
		firstNameIdx = randint(0,len(firstNames)-1)
		lastNameIdx = randint(0,len(lastNames)-1)
		userTypeIndex = randint(0,len(userType)-1)

		userId = str(uuid.uuid1())
		userIds.append(userId)
		emailId = str(firstNames[firstNameIdx]) + "." + str(lastNames[lastNameIdx]) + "@gmail.com"
		
		#randomly create set of book isbns
		booksRead = ""
		booksWritten = ""
		count =0
		readBookCount=randint(1,15)
		
		while count < readBookCount:
			if booksRead == "":
				booksRead = "'" +bookIds[randint(0,len(bookIds)-1)]+ "'"
			else:
				booksRead = booksRead + ",'" + bookIds[randint(0,len(bookIds)-1)]+ "'"
			count+=1
			
		count =0 
		writtenBookCount = randint(1,15)
		while count < writtenBookCount:
			if booksWritten == "":
				booksWritten = "'" + bookIds[randint(0,len(bookIds)-1)] + "'"
			else:
				booksWritten = booksWritten + ",'" + bookIds[randint(0,len(bookIds)-1)] + "'"
			count+=1
		
		#insert data in database
		csql = "insert into users (id, email,password,type, books_read,books_written) values (" + str(userId) + ",'" + emailId + "','pass','" + userType[userTypeIndex] + "',{" + booksRead + "},{" + booksWritten + "})"
		session.execute(csql)
		counter +=1
		
	file = open('userIds.txt', 'w')
	file.write(str(userIds))
	
data = open(bookIsbnFilepath)
input_file = csv.DictReader(data,delimiter=",",quotechar='"')	
for row in input_file:
	bookIds.append(row["isbn"])
print "books ids loaded"

populateUsers()
