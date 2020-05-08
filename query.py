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
		query = file.read()
else:
	query = input

print("The input query is:", query)
print("\n\n")

query = query.replace('‘', '').replace('’','').replace("'",'').replace('"','').replace('\t','').replace('\n', '')

# format the input
S = [] #select attributes
n = 0 #number of grouping variables
V = [] #grouping attributes
F = [] #F-VECT
Sigma = [] #select condition-vect
G = "" #having condition

with open("input.json", "w") as input:
#GROUP BY CLAUSEs
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

	if query.find('where') != -1: 
		where = query.split("where")[1].split('group by')[0].replace(" ", "")
		print("where: ", where)
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

	obj = {
		"cust": 0,
		"prod": 1,
		"day" : 2,
		"month": 3,
		"year" : 4,
		"state": 5,
		"quant": 6,
	}
	repr(obj)

	F = F + list(filter(lambda x: (x not in obj) and (x not in F), S)) 

	print("F-vector F: ", F, '\n') 


	inp = {
		'S': S,
		'n': n,
		'V': V,
		'F': F,
		'Sigma': Sigma,
		'G': G,
	}
	# print("inp: ", inp)
	# print("type of inp", type(inp))
	repr(inp)
	json.dump(inp, input)


outstr = """  
S = [] #select attributes
n = 0 #number of grouping variables
V = [] #grouping attributes
F = [] #F-VECT
Sigma = [] #select condition-vect
G = ""

S = inp['S']
n = inp['n']
V = inp['V']
F = inp['F']
Sigma = inp['Sigma']
G = inp['G']

# print(S)
# print(n)
# print(V)
# print(F)
# print(Sigma)
# print(G)

# connect to the database
try:
	connection = psycopg2.connect(user = "postgres",
								  password = "",
								  host = "127.0.0.1",
								  port = "5432",
								  database = "postgres")
	print("connected to postgres");

	cursor = connection.cursor()
	cursor.execute("select * from sales");
	data = cursor.fetchall()

	#a = ['cust','prod']
	
	gb = list(map(lambda x : obj[x],V))
	#print(gb)
	partition = {}

	for row in data:
		l = tuple(map(lambda x: row[x], gb))
		if l not in partition.keys():
			partition[l] = [row]
		else:
			partition[l] = partition[l] + [row]

	#Checks if a row satisfies a certain condition
	def satisfies(row, condition): 
		#condition passed in the following format eg. [state, = , NJ]
		sat = False 
		if (condition[1] == '='):
			if row[obj[condition[0]]] == condition[2]:
				sat = True 
		elif (condition[1] == '>'):
			if row[obj[condition[0]]] > condition[2]:
				sat = True  
		elif (condition[1] == '<'):
			if row[obj[condition[0]]] < condition[2]:
				sat = True  
		return sat

	#Computes sum based on the rows and an attribute passed in
	def sum(rows, attr, conditions):
		sum = 0
		for row in rows:
			sat = True
			for c in conditions:
				if not satisfies(row, c):
					sat = False
			if sat:
				sum = sum + int(row[obj[attr]])
		return sum

	#Counts rows
	def count(rows, conditions):
		if conditions == []:
			return len(rows)
		else:
			count = 0
			for row in rows:
				sat = True
				for c in conditions:
					if not satisfies(row, c):
						sat = False
				if sat:
					count = count + 1
			return count

	#Computes avg based on the rows and an attribute passed in
	def avg(rows,attr, conditions):
		c = count(rows, conditions)
		if c == 0:
			return 0
		s = sum(rows,attr, conditions)
		return s/c


	#Computes min 
	def min(rows, attr, conditions):
		min = 0
		for row in rows: 
			sat = True
			for c in conditions:
				if not satisfies(row, c):
					sat = False
			min = row[obj[attr]]
			break

		for row in rows:
			sat = True
			for c in conditions:
				if not satisfies(row, c):
					sat = False
			if sat: 		
				if row[obj[attr]] < min:
					min = row[obj[attr]]
		return min

	#Computes max
	def max(rows, attr, conditions):
		max = 0
		for row in rows: 
			sat = True
			for c in conditions:
				if not satisfies(row, c):
					sat = False
			max = row[obj[attr]]
			break

		for row in rows:
			sat = True
			for c in conditions:
				if not satisfies(row, c):
					sat = False
			if sat: 		
				if row[obj[attr]] > max:
					max = row[obj[attr]]
		return max

	def compute_aggr(rows, attr, func, conditions):
		if func == 'sum':
			return sum(rows, attr, conditions)
		elif func == 'count':
			return count(rows, conditions)
		elif func == 'avg':
			return avg(rows, attr, conditions)
		elif func == 'min':
			return min(rows, attr, conditions)
		elif func == 'max':
			return max(rows, attr, conditions)


	def parse_condition(condition): #passing string in the following format: satate
		equals = condition.find('=')
		greater = condition.find('>')
		less = condition.find('<')
		if equals > -1:
			condition = [(condition.split('=')[0])] + ['='] + [(condition.split('=')[1])]
		elif greater > -1:
			condition = [(condition.split('>')[0])] + ['>'] + [(condition.split('>')[1])]
		elif less > -1:	
			condition = [(condition.split('<')[0])] + ['<'] + [(condition.split('<')[1])]
		return condition

	#dictionary that contains 
	computed_aggregates = {}

	aggregate_functions = ['sum', 'count', 'avg', 'min', 'max']
	#compute aggregate functions for each group in partition based on F and Sigma
	for key in partition:
		rows = partition[key]
		computed = []
		for agr in F: 					#computing each aggregate function in F
			agr = agr.split('_') 		#spliting it so we get the following format ['sum', '1', 'quant']
			func = agr[0] 				#'sum'
			gb_var = agr[1] 			#'1'
			attr = agr[2] 				#'quant'
			select_conditions = []
			for s in Sigma: 			#parsing condition in sigma if it has the same variable as an aggregate function in F
				s = s.split('_')		#['1', 'state = NJ']
				if s[0] == gb_var:		
					condition = parse_condition(s[1])
					a = condition[0]
					operator = condition[1]
					val = condition[2]  #evaluate this val depending on wheather it is an aggregate function of something or a string or a number
					if val.isnumeric():
						val = int(val)
					else:
						for fun in aggregate_functions:
							if fun in val:
								val_attr = val.replace(fun +'(','').replace(')', '')
								val = compute_aggr(rows, val_attr, fun, [])
								break
					select_conditions = select_conditions + [[a,operator, val]]
					#select_conditions = select_conditions.append([a,operator,val])
			computed = computed + [compute_aggr(rows, attr, func, select_conditions)]
			computed_aggregates[key] = computed
			#partition[key] = partition[key] + tuple(computed)

	#print(computed_aggregates) 
	#print(partition)

	#output for filtering out partition
	filtered_partition = {}
	
	for key in computed_aggregates:
		hav_condition = G
		for i in range(len(F)):
			hav_condition = hav_condition.replace(F[i], str(computed_aggregates[key][i]))
		hav_condition.replace('=', '==')
		if eval(hav_condition):
			filtered_partition[key] = partition[key]


	print(computed_aggregates)

	output = [] #Final Output
	for key in filtered_partition:
		select_values = []
		for attribute in S: 
			value = None
			if attribute in V:
				index = V.index(attribute) 
				value = key[index]
			if attribute in F:
				index = F.index(attribute)
				value = computed_aggregates[key][index]
			select_values = select_values  + [value]
		output = output + [tuple(select_values)]

	print(output)

	table = PrettyTable(S)
	for rec in output:
		table.add_row(rec)

	print(table)
	
	result = open('output.txt', 'w+')
	table_txt = table.get_string()
	result.write(table_txt)
	result.close()

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
"""

engine = open('engine.py', 'w+')
engine.write("import psycopg2\nfrom prettytable import PrettyTable\n\n")
engine.write("obj = " + repr(obj) + '\n')
engine.write("inp = " + repr(inp) + '\n')
engine.write(outstr)
engine.close()

