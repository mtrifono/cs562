import psycopg2 
import os.path

##### Flow:
# read input from terminal/file
# organize input and format it into 6 vectors/attributes feeding into phi operator
# generate another file, where takes the formatted input and compute the final output using phi MF algo 

# read input
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

# format the input
#################### TODO ###########################

# connect to the database
try:
	connection = psycopg2.connect(user = "postgres",
								  password = "",
								  host = "127.0.0.1",
								  port = "5432",
								  database = "postgres")

	cursor = connection.cursor()

	###### create table
	# create_table_query = '''CREATE TABLE sales
	# 	(cust VARCHAR(20) NOT NULL,
	# 	prod VARCHAR(20) NOT NULL,
	# 	day INT NOT NULL,
	# 	month INT NOT NULL,
	# 	year INT NOT NULL,
	# 	state CHAR(2) NOT NULL,
	# 	quant INT NOT NULL); '''
	# cursor.execute('''DROP TABLE IF EXISTS sales''');
	# cursor.execute(create_table_query)
	# connection.commit()
	# print("Table created successfully in PostgreSQL ")

	###### read data from sql file and add to table
	# fd = open('sdap.sql', 'r')
	# sqlFile = fd.read()
	# fd.close()
	# sqlCommands = sqlFile.split(';')
	# for command in sqlCommands:
	# 	try:
	# 		print(command)
	################ below line doesn't work ##################
	# 		cursor.execute(command)
	# 	except Error as error:
	# 		print (error)
	# connection.commit()
	# count = cursor.rowcount
	# print (count, "Record inserted successfully into mobile table")

except psycopg2.DatabaseError as error:
	print(error);
finally:
	# close database connection.
		if(connection):
			cursor.close()
			connection.close()
			print("PostgreSQL connection is closed")

