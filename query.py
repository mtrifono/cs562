import psycopg2 
import os.path

#connect to the database
try:
	connection = psycopg2.connect(user = "postgres",
                                  password = "",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "postgres")
	print("connected to postgres")
except psycopg2.DatabaseError as error:
	print(error);

input = input("Input a query or a file name that contains a query\n")

is_file = os.path.isfile(input)

query = ""

if is_file:
	print("file exists")
	with open(input, 'r') as file:
		query = file.read().replace('\n', '')
else:
	query = input

print("The input query is:", query)