import psycopg2 
import os.path
import json

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
print("\n\n")

# format the input
S = [] #select attributes
n = 0 #number of grouping variables
V = [] #grouping attributes
F = [] #F-VECT
Sigma = [] #select condition-vect
G = "" #having condition

with open("input.json", "w") as input:
#GROUP BY CLAUSE
	query = query.replace("\n", ' ')
	group = query.split("group by")[1].split('such that')[0].split(':');
	gb_attr = group[0].replace(" ", "").split(",")
	gb_var = group[1].replace(" ", "").split(",")
	V = gb_attr
	n = len(gb_var)
	print("Grouping attributes: ", V)
	print("Number of group by variables: ", n)

	varDict = {} #eg. {'x': 1, 'y': 2, 'z': 3}
	i = 0
	for v in gb_var:
		i=i+1
		varDict[v] = i

	print("Groub by variables: ", varDict, '\n')

	select = query.split("select")[1].split('from')[0].replace(" ", "")
	for key in varDict:
			select = select.replace(key + '.', str(varDict[key])+'_').replace('(', '_').replace(')','')
	S = select.split(',')

#for item in select:
#	index = item.find('(') + 1
#	if index > 0:
#		num = str(varDict[item[index]])
#		item = num+'_'+item.replace(item[index-1:index+2], '_').replace(')','')
#	S = S + [item]	

	print("Select attributes S: ", S, '\n') 

	select_condition = query.split("such that")[1].split("having")[0].replace('and', '|').replace('or', '|').replace(' ', '').split('|')
	for item in select_condition:
		temp=item.split('.')[0]
		if temp in varDict:
			item = item.replace(temp + '.', str(varDict[temp]) + '_')
		Sigma = Sigma + [item]

	print("Select condition Sigma: ", Sigma, '\n') 

	having_index = query.find('having')
	if having_index > 0:
		having = query.split('having')[1]
		for key in varDict:
			having = having.replace(key + '.', str(varDict[key])+'_').replace('(', '_').replace(')','')
		G = having
		having_vars = having.replace('>','|').replace('<','|').replace('*','|').replace('/','|').replace('=','|').replace('+','|').replace('-','|').replace('not', '|').replace('or','|').replace('and','|').replace(' ','').split('|')
		for item in having_vars:
			if not item.isnumeric():
				F = F + [item]

	print("Having condition G: ", G,'\n') 

	print("F-vector F: ", F, '\n') 

	inp = {
		'S': S,
		'n': n,
		'V': V,
		'F': F,
		'Sigma': Sigma,
		'G': G,
	}

	json.dump(inp, input)

# connect to the database
try:
	connection = psycopg2.connect(user = "postgres",
								  password = "masha270896",
								  host = "127.0.0.1",
								  port = "5432",
								  database = "postgres")
	print("\nconnected to postgres\n");

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

