import sys
import pandas
import re
import numpy
import os

####################################################################
#functions

def get_attributes(table_name):


	with open("metadata.txt", "r+") as fp1:
		list_of_atts = fp1.readlines()

	list_of_attributes = []
	for i in list_of_atts:
		list_of_attributes.append(i.rstrip("\r\n"))

	required_list_of_attributes = []
	index_of_tablename = list_of_attributes.index(table_name) + 1

	while list_of_attributes[index_of_tablename] != '<end_table>':
		required_list_of_attributes.append(table_name + "." + list_of_attributes[index_of_tablename])
		index_of_tablename = index_of_tablename + 1

	return required_list_of_attributes
	#print required_list_of_attributes

def get_functions(attr):
	all_functions = {'func_flag': 0, 'func':{'max':[], 'min':[], 'avg':[], 'sum':[], 'distinct':[]}}

	for i in attr:
		if re.match(r'max\((.+)\)', i, re.I) is not None:
			all_functions['func_flag'] = 1
			all_functions['func']['max'].append((re.match(r'max\((.+)\)', i, re.I)).group(1))

		if re.match(r'min\((.+)\)', i, re.I) is not None:
			all_functions['func_flag'] = 1
			all_functions['func']['min'].append((re.match(r'min\((.+)\)', i, re.I)).group(1))

		if re.match(r'avg\((.+)\)', i, re.I) is not None:
			all_functions['func_flag'] = 1
			all_functions['func']['avg'].append((re.match(r'avg\((.+)\)', i, re.I)).group(1))

		if re.match(r'sum\((.+)\)', i, re.I) is not None:
			all_functions['func_flag'] = 1
			all_functions['func']['sum'].append((re.match(r'sum\((.+)\)', i, re.I)).group(1))

		if re.match(r'distinct\((.+)\)', i, re.I) is not None:
			all_functions['func_flag'] = 1
			all_functions['func']['distinct'].append((re.match(r'distinct\((.+)\)', i, re.I)).group(1)) 

	return all_functions


def evaluate_csv(tabs):
	evaluated = []
	for i in tabs:
		i = i + ".csv"
		m = []
		with open(i, "r+") as fp2:
			j = fp2.readlines()		
		for k in j:
			m.append(k.rstrip("\r\n"))
		evaluated.append(m)
	return evaluated


def project(a, b):
	ans = []
	for i in a:
		for j in b:
			t = str(j) + ',' + str(i)
			ans.append(t)
	return ans

def error_print():
	print ""
	print "################ SOMW ERROR OCCURED<--->READ ONCE #################"
	print "----->  Only select queries allowed"
	print "----->  select column_name(s) from table_name(s)"
	print "----->  select column_names(s) from table_names(s) where condition(s)"
	print "----->  ATMOST 'TWO' conditions can be given"
	print "----->  Only operator used in a condition is '='"
	print "----->  In case of two conditions only Operators to combine them are 'AND' and 'OR'"
	print "----->  SELECT, FROM, WHERE, AND, OR are case insensitive"
	print "##################################################################"


def get_list_of_contents(tabs, colus):
	ans = []
	headers = []

	for i in colus:
		m = i.split('.')
		#print m
		if('.' in i):
			t_name = m[0] + ".csv"
			c_name = m[1]
			headers.append(".".join(m))

			df = pandas.read_csv(t_name, names = get_attributes(m[0]))
			ans.append(list(df[i]))


		elif('.' not in i):
			c_name = m[0]

			for i in tables:
				if i + '.' + c_name in get_attributes(i):
					t_name = i + ".csv"
					df = pandas.read_csv(t_name, names = get_attributes(i))
					ans.append(list(df[i + '.' + c_name]))
					headers.append(i + '.' + c_name)

	return headers


def get_table_names(conds, tabs):

	if '.' in conds[0] and '.' in conds[1]:
		return conds

	if '.' not in conds[0]:
		ind = -1
		for i in tabs:
			for j in get_attributes(i):
				if i + '.' + conds[0] == j:
					conds[0] = i+'.'+conds[0]
					ind = 1
			#if ind == -1:
			#	error_print()
			#	sys.exit("\nERROR: The conditions are invalid\n")
					

	if '.' not in conds[1]:
		for i in tabs:
			for j in get_attributes(i):
				if i + '.' + conds[1] == j:
					conds[1] = i + '.' + conds[1]

	return conds

def is_file(tab):
	tab = tab + ".csv"
	return os.path.isfile(tab)

def check_errors(query, cols, tables):
	error = {'flag':0, 'msg':""}

	attr = cols[:]

	funcs = get_functions(attr)
	#print funcs

	if len(query) < 4 or query[0] != 'select' or 'from' not in query :
		error['flag'] = 1
		error['msg'] = "Invalid query\n"
		return error

	elif  'where' in query and len(query) < 6:
		error['flag'] = 1
		error['msg'] = "Invalid query\n"
		return error

	elif len(cols) == 0 or len(tables) == 0:
		error['flag'] = 1
		error['msg'] = "Invalid query\n"
		return error

	#error for tables presence
	if funcs['func_flag'] == 0 and cols != ['*']:

		for i in tables:
			#print "entered"
			if not is_file(i):
				error['flag'] = 1
				error['msg'] = "One of the tables '" + i +"' is missing\n"
				return error

		#error for columns whether they all in tables
		for i in cols:
			if '.' not in i:
				ind = -1
				for j in tables:
					if (j+'.' +i) in get_attributes(j):
						ind = 1
				if ind == -1:
					error['flag'] = 1
					error['msg'] = "the column " + i + " is not in any table\n"
					return error

			elif '.' in i:
				x = i.split(".")
				if i not in get_attributes(x[0]):
					error['flag'] = 1
					error['msg'] = "the column " + x[1] + " is not in table: " + x[0]
					return error

	elif funcs['func_flag'] == 1 and cols != ['*']:
		print ""

		exist_funcs = {}
		for key in all_funcs['func']:
			if funcs['func'][key] != []:
				exist_funcs[key] = funcs['func'][key]

		f = []
		for i in exist_funcs:
			f.extend(exist_funcs[i])

		for j in  f:
			ind = -1
			for i in tables:
				if i + '.' + j in get_attributes(i) or j in get_attributes(i):
					ind = 1
			if ind == -1:
				error['flag'] = 1
				error['msg'] = "The column name " + j + " is not in any table\n"
				return error



	#error for conditions
	if 'where' in query:
		cond_1 = []
		cond_2 = []

		conditions = []
		where_index = query.index('where')
		conditions = query[where_index+1 : ]

		if conditions == []:
			error['flag'] = 1
			error['msg'] = "The conditions after 'where' are missing"
		and_flag = 0
		or_flag = 0
		no_flag = 1

		for i in conditions:
			if i.lower() == 'and':
				#print 'and is there\n'
				and_flag = 1
				cond_1 = query[where_index+1 : query.index(i)]
				cond_2 = query[query.index(i)+1 : ]
				cond_1 = "".join(cond_1)
				cond_2 = "".join(cond_2)
				no_flag = 0
				cond_1 = cond_1.split("=")
				cond_2 = cond_2.split("=")
				break

			elif i.lower() == 'or':
				or_flag = 1
				cond_1 = query[where_index+1 : query.index(i)]
				cond_2 = query[query.index(i)+1 : ]
				cond_1 = "".join(cond_1)
				cond_2 = "".join(cond_2)	
				no_flag = 0
				cond_1 = cond_1.split("=")
				cond_2 = cond_2.split("=")
				break

		if no_flag == 1:
			conditions = "".join(conditions)
			conditions = conditions.split("=")


		if no_flag == 1:
			#no and/or
			if len(conditions) > 3:
				error['flag'] = 1
				error['msg'] = " Please check the conditions after where\n"
				return error

		elif or_flag == 1:
			if len(cond_1) + len(cond_2) > 4:
				error['flag'] = 1
				error['msg'] = " Please check the conditions after where\n"
				return error

			elif len(cond_1) == 0 or len(cond_2) == 0:
				error['flag'] = 1
				error['msg'] = " Please check the conditions after where\n"
				return error


		elif and_flag == 1:
			if len(cond_1) + len(cond_2) > 4:
				error['flag'] = 1
				error['msg'] = " Please check the conditions after where\n"
				return error

			elif len(cond_1) == 0 or len(cond_2) == 0:
				error['flag'] = 1
				error['msg'] = " Please check the conditions after where\n"
				return error

	return error


#def check_repetitions(ans, header):
#	all_cols = []
#	for i in ans:



#################### main program ####################################
#list of strings of query
query = (sys.argv[1]).split(" ")

i = query.count('')
while i!=0:
	query.remove('')
	i = i - 1

for i in query:
	if i.lower() == 'select' or i.lower() == 'from' or i.lower() == 'where':
		z = i.lower()
		query[query.index(i)] = z

select_index = query.index('select') + 1
from_index = query.index('from')

###############################################################
cols = []
#cols is the list of attributes between 'select and from'
while select_index != from_index:
	cols.extend(query[select_index].split(","))			
	select_index += 1

i = cols.count('')
while i!=0:
	cols.remove('')
	i = i - 1

##############################################################

where_flag = 0
if 'where' in query:
	where_flag = 1

#all_funcs is dictionary of all aggregate functions
attr = cols[:]#shallow copy
all_funcs = get_functions(attr)

############################################################

#conditions is a list of conditions
conditions = []

if where_flag == 1:
	where_index = query.index('where')
	conditions = query[where_index+1 : ]
	#print conditions

	and_flag = 0
	or_flag = 0

	cond_1 = []
	cond_2 = []

	for i in conditions:
		if i.lower() == 'and':
			#print 'and is there\n'
			and_flag = 1
			cond_1 = query[where_index+1 : query.index(i)]
			cond_2 = query[query.index(i)+1 : ]
			cond_1 = "".join(cond_1)
			cond_2 = "".join(cond_2)

			cond_1 = cond_1.split("=")
			cond_2 = cond_2.split("=")
			break

		elif i.lower() == 'or':
			or_flag = 1
			cond_1 = query[where_index+1 : query.index(i)]
			cond_2 = query[query.index(i)+1 : ]
			cond_1 = "".join(cond_1)
			cond_2 = "".join(cond_2)

			cond_1 = cond_1.split("=")
			cond_2 = cond_2.split("=")
			break
	#cond_1, cond_2 are conditions(lists) after and before and/or

############################################################

#tables is a list of tables which we use in the query
tbls = []
tables = []

if where_flag == 0:
	tbls = query[query.index('from')+1 : ]
	for i in tbls:
		tables.extend(i.split(","))

elif where_flag == 1:
	tbls = query[query.index('from')+1 : query.index('where')]
	for i in tbls:
		tables.extend(i.split(","))

i = tables.count('')
while i!=0:
	tables.remove('')
	i = i - 1

tables.sort()

############################################################

error = check_errors(query, cols, tables)
if error['flag'] == 1:
	error_print()
	sys.exit("ERROR: " + error['msg'])


############################################################

#for queries with max, min, sum, avg, distinct
#print all_funcs
if all_funcs['func_flag'] == 1 and where_flag == 0:
	
	table_name = query[len(query) - 1]
	df = pandas.read_csv(table_name + '.csv', names = get_attributes(table_name))

	exist_funcs = {}
	for key in all_funcs['func']:
		if all_funcs['func'][key] != []:
			exist_funcs[key] = all_funcs['func'][key]

	#print exist_funcs
	answer = []
	header = []
	l = []
	distinct_answer = []
	distinct_checked = []

	#################################################
	#for queries: select max(A), min(B), sum(C), avg(D) from table;
	for key in exist_funcs:
		for i in exist_funcs[key]:
			column_name =  table_name + '.' + i
			k = (list(df[table_name + '.' +i]))
			header.append(key + '(' + i + ')')
			if key == 'max':
				answer.append(str(max(k)))

			if key == 'min':
				answer.append(str(min(k)))

			if key == 'avg':
				answer.append(str(numpy.mean(k)))

			if key == 'sum':
				answer.append(str(sum(k)))

	if len(all_funcs['func']['distinct']) == 0:
		print ",".join(header)
		print ",".join(answer)
	
	###################################################

	#for queries: select distinct(A), distinct(B) from table;
	if len(all_funcs['func']['distinct']) != 0:
		print ",".join(header)
		for i in all_funcs['func']['distinct']:
			l.append(list(df[table_name + '.' +i]))
		no_of_distinct_cols = len(l)
		
			
		i = 0
		j = 0
		random = []
		n = len(l[0])
		while i<n:
			while j<no_of_distinct_cols:
				random.append(str(l[j][i]))
				j = j + 1
			i = i + 1
			j = 0
			distinct_answer.append(random)
			random = []
		
		for i in distinct_answer:
			if i not in distinct_checked:
				distinct_checked.append(i)

		for i in distinct_checked:
			print ",".join(i)

	#####################################################
	


	

#for queries with no max, min, avg, sum, distinct
elif all_funcs['func_flag'] == 0 and where_flag == 0:
	
	#query for select * from table
	if len(query) == 4 and query[1] == '*' and query[0] == 'select':
		file_name = query[3] + ".csv"
		with open(file_name, "r+") as fp:
			list_of_ls = fp.readlines()
			# contains list of all tuples

		list_of_lines = []
		for i in list_of_ls:
			list_of_lines.append(i.rstrip("\r\n"))

		#get_attributes(query[3]);
		#print list_of_lines

	
		print ",".join(get_attributes(query[3]))
		for i in list_of_lines:
			print i

	#############################################################################
	#for queries of type select A, B, C from table_A;
	elif 'where' not in query and query[1] != '*' and len(cols) >= 1 and len(tables) == 1:
		
		table_name = query[len(query) - 1]
		j = len(cols)
		i = 0
		while i!=j:
			cols[i] = table_name + '.' + cols[i]
			i = i + 1

		print "\t".join(cols)
		not_present = 0
		for i in cols:
			if i not in get_attributes(table_name):
				not_present = 1

		if not_present == 1:
			print "\nError: All attributes are not present in table: " + table_name + "\n"

		if not_present == 0:
			df = pandas.read_csv(table_name + ".csv", names = get_attributes(table_name))
			saved_columns = []
			for i in cols:
				saved_columns.append(list(df[i]))

			no_of_tuples = len(saved_columns[0])
			no_of_cols = len(cols)

			i = 0
		
			while i<no_of_tuples:
				j = 0
				while j<no_of_cols:
					print str(saved_columns[j][i]) + '\t' ,
					j = j+1

				print ""
				i = i + 1
	###############################################################################

	#for queries of type select * from A, B, C.
	elif len(query) > 4 and 'where' not in query and query[1] == '*' and len(tables) > 1:
		tables_contents = evaluate_csv(tables)
		while len(tables_contents) > 1:
			a = tables_contents.pop()
			b = tables_contents.pop()
			c = project(a, b)
			tables_contents.append(c)

		header = []
		for i in tables:
			header.extend(get_attributes(i))
		print ",  ".join(header)

		for i in tables_contents[0]:
			print i
		#print len(tables_contents[0])

	#############################################################################	

	#for queries of type select a, t1.b, c from t1, t2;
	elif len(cols) > 1 and 'where' not in query and len(tables) > 1 and '*' not in cols:
		#print tables
		#print cols

		headers = []
		headers = get_list_of_contents(tables, cols)
		#print headers
		tables_contents = evaluate_csv(tables)
		while len(tables_contents) > 1:
			a = tables_contents.pop()
			b = tables_contents.pop()
			c = project(a, b)
			tables_contents.append(c)

		all_columns = []

		for i in tables:
			all_columns.extend(get_attributes(i))

		#print all_columns

		index_of_headers = []
		for i in headers:
			index_of_headers.append(all_columns.index(i))

		#print index_of_headers
		print ",  ".join(headers)

		for i in tables_contents[0]:
			x = i.split(",")
			y = []
			for j in index_of_headers:
				y.append(x[j])
			print ",\t".join(y)



	##############################################################################

#for queries: select * from A, B where A.x = B.y
elif where_flag==1 and and_flag == 0 and or_flag == 0:
	
	conds = conditions[:]
	conds = "".join(conds)
	conds = conds.split("=")
	conds = get_table_names(conds, tables)

	tables_contents = evaluate_csv(tables)
	while len(tables_contents) > 1:
		a = tables_contents.pop()
		b = tables_contents.pop()
		c = project(a, b)
		tables_contents.append(c)

	h = []
	for i in tables:
		h.extend(get_attributes(i))

	header = []
	header_indexes = []
	if query[1] == '*':
		header = h
	else:
		header = get_list_of_contents(tables, cols)
	#print ", ".join(header)

	for i in header:
		header_indexes.append(h.index(i))

	ans = []
	#both conds are columns
	if('.' in conds[1]):
		#indexes consist of index of conds in header
		indexes = []
		for i in conds:
			indexes.append(h.index(i))

		header.remove(h[indexes[0]])
		print ",  ".join(header)
		
		for i in tables_contents[0]:
			x = i.split(",")
			z = []
			if x[indexes[0]] == x[indexes[1]]:
				for j in header_indexes:
					if j != indexes[0]:
						z.append(x[j])
					

				
				print ",\t".join(z)
		#print ans
		#ans = check_repetitions(ans, header)
		#header = ans[1]
		#ans = ans[0]

		#print ",".join(header)
		##for i in ans:
		#	print i

	#2nd condition is value
	elif '.' not in conds[1]:
		print ""
		index_of_col1 = h.index(conds[0])

		for i in tables_contents[0]:
			x = i.split(",")
			z = []
			if x[index_of_col1] == conds[1]:
				for j in header_indexes:
					z.append(x[j])
				print ",".join(z)


#conditions with and statement
elif where_flag==1 and and_flag==1 and or_flag==0:

	cond_1 = get_table_names(cond_1, tables)
	cond_2 = get_table_names(cond_2, tables)

	tables_contents = evaluate_csv(tables)
	while len(tables_contents) > 1:
		a = tables_contents.pop()
		b = tables_contents.pop()
		c = project(a, b)
		tables_contents.append(c)

	h = []
	for i in tables:
		h.extend(get_attributes(i))

	header = []
	header_indexes = []
	if query[1] == '*':
		header = h
	else:
		header = get_list_of_contents(tables, cols)
	print ",  ".join(header)

	for i in header:
		header_indexes.append(h.index(i))

	if '.' in cond_1[1] and '.' in cond_2[1]:
		index_of_cond11 = h.index(cond_1[1])
		index_of_cond10 = h.index(cond_1[0])
		index_of_cond21 = h.index(cond_2[1])
		index_of_cond20 = h.index(cond_2[0])

		for i in tables_contents[0]:
			x = i.split(",")
			if x[index_of_cond10] == x[index_of_cond11] and x[index_of_cond20] == x[index_of_cond21]:
				y = []
				for j in header_indexes:
					y.append(x[j])
				print ",\t".join(y)




	elif '.' in cond_1[1] and '.' not in cond_2[1]:
		print ""
		index_of_cond11 = h.index(cond_1[1])
		index_of_cond10 = h.index(cond_1[0])
		index_of_cond20 = h.index(cond_2[0])

		for i in tables_contents[0]:
			x = i.split(",")
			if x[index_of_cond10] == x[index_of_cond11] and x[index_of_cond20] == cond_2[1]:
				y = []
				for j in header_indexes:
					y.append(x[j])
				print ",\t".join(y)				


	elif '.' not in cond_1[1] and '.' in cond_2[1]:
		print ""
		index_of_cond10 = h.index(cond_1[0])
		index_of_cond21 = h.index(cond_2[1])
		index_of_cond20 = h.index(cond_2[0])

		for i in tables_contents[0]:
			x = i.split(",")
			if x[index_of_cond20] == x[index_of_cond21] and x[index_of_cond10] == cond_1[1]:
				y = []
				for j in header_indexes:
					y.append(x[j])
				print ",\t".join(y)	


	elif '.' not in cond_1[1] and '.' not in cond_2[1]:
		print ""
		index_of_cond10 = h.index(cond_1[0])
		index_of_cond20 = h.index(cond_2[0])

		for i in tables_contents[0]:
			x = i.split(",")
			if x[index_of_cond20] == cond_2[1] and x[index_of_cond10] == cond_1[1]:
				y = []
				for j in header_indexes:
					y.append(x[j])
				print ",\t".join(y)


#conditions with or statement
elif where_flag==1 and and_flag==0 and or_flag==1:

	cond_1 = get_table_names(cond_1, tables)
	cond_2 = get_table_names(cond_2, tables)


	tables_contents = evaluate_csv(tables)
	while len(tables_contents) > 1:
		a = tables_contents.pop()
		b = tables_contents.pop()
		c = project(a, b)
		tables_contents.append(c)

	h = []
	for i in tables:
		h.extend(get_attributes(i))

	header = []
	header_indexes = []
	if query[1] == '*':
		header = h
	else:
		header = get_list_of_contents(tables, cols)
	print ",  ".join(header)

	for i in header:
		header_indexes.append(h.index(i))

	if '.' in cond_1[1] and '.' in cond_2[1]:
		index_of_cond11 = h.index(cond_1[1])
		index_of_cond10 = h.index(cond_1[0])
		index_of_cond21 = h.index(cond_2[1])
		index_of_cond20 = h.index(cond_2[0])

		for i in tables_contents[0]:
			x = i.split(",")
			if x[index_of_cond10] == x[index_of_cond11] or x[index_of_cond20] == x[index_of_cond21]:
				y = []
				for j in header_indexes:
					y.append(x[j])
				print ",\t".join(y)




	elif '.' in cond_1[1] and '.' not in cond_2[1]:
		print ""
		index_of_cond11 = h.index(cond_1[1])
		index_of_cond10 = h.index(cond_1[0])
		index_of_cond20 = h.index(cond_2[0])

		for i in tables_contents[0]:
			x = i.split(",")
			if x[index_of_cond10] == x[index_of_cond11] or x[index_of_cond20] == cond_2[1]:
				y = []
				for j in header_indexes:
					y.append(x[j])
				print ",\t".join(y)				


	elif '.' not in cond_1[1] and '.' in cond_2[1]:
		print ""
		index_of_cond10 = h.index(cond_1[0])
		index_of_cond21 = h.index(cond_2[1])
		index_of_cond20 = h.index(cond_2[0])

		for i in tables_contents[0]:
			x = i.split(",")
			if x[index_of_cond20] == x[index_of_cond21] or x[index_of_cond10] == cond_1[1]:
				y = []
				for j in header_indexes:
					y.append(x[j])
				print ",\t".join(y)	


	elif '.' not in cond_1[1] and '.' not in cond_2[1]:
		print ""
		index_of_cond10 = h.index(cond_1[0])
		index_of_cond20 = h.index(cond_2[0])

		for i in tables_contents[0]:
			x = i.split(",")
			if x[index_of_cond20] == cond_2[1] or x[index_of_cond10] == cond_1[1]:
				y = []
				for j in header_indexes:
					y.append(x[j])
				print ",\t".join(y)


