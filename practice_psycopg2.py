import csv
import psycopg2

def csv_to_dicts(input_file):
	results = []
	with open(input_file, 'r') as input:
		reader = csv.DictReader(input)
		for dic in reader:
			for key in dic:
				temp_key = key.strip().lower()
				temp_val = dic.pop(key).strip().lower()
				if temp_val == '':
					temp_val = 'NULL'
				dic[temp_key] = temp_val
			results.append(dic)
	print results

def breed_to_id(breedList):
	result = {}
	conn = psycopg2.connect("dbname='pets' host='localhost'")
	cur = conn.cursor()
	cur.execute("""SELECT * FROM breed""")
	breeds = cur.fetchall()
	for breed in breeds:
		result[breed[1]] = breed[0]
	return result
	



if __name__ == '__main__':
	breed_to_id([])
