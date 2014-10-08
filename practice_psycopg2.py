import csv
import psycopg2

def add_species(input_file):
	addSpecies = []
	speciesDict = table_to_id("species")
	with open(input_file, 'r') as input:
		reader = csv.DictReader(input)
		for dic in reader:
			normalized_val = dic[' species name'].strip().lower()
			if normalized_val in speciesDict or normalized_val == '':
				continue
			else:
				addSpecies.append({'name': normalized_val})
	return addSpecies

def add_breed(input_file):
	addBreed = []
	breedDict = table_to_id("breed")
	with open(input_file, 'r') as input:
		reader = csv.DictReader(input)
		for dic in reader:
			normalized_val = dic[' breed name'].strip().lower()
			if normalized_val in breedDict or normalized_val == '':
				continue
			else:
				addBreed.append({'name': normalized_val, 'species_id':None})
	return addBreed

def add_shelter(input_file):
	addShelter = []
	shelterDict = table_to_id("shelter")
	with open(input_file, 'r') as input:
		reader = csv.DictReader(input)
		for dic in reader:
			normalized_val = dic[' shelter name'].strip().lower()
			if normalized_val in shelterDict or normalized_val == '':
				continue
			else:
				addShelter.append({'name': normalized_val, 'website':None,
					'phone': None})
	return addShelter

def add_pet(input_file):
	addPet = []
	speciesDict =  table_to_id("species")
	breedDict = table_to_id("breed")
	shelterDict = table_to_id("shelter")
	with open(input_file, 'r') as input:
		reader = csv.DictReader(input)
		for dic in reader:
			temp_dic = {}
			temp_dic["dead"] = False #assuming we are not adding dead pets to db
			for key in dic:
				temp_key = key.strip().lower()
				temp_val = str(dic[key]).strip().lower()
				if temp_key == 'species name': #don't need to add this one to pet
					continue
				elif temp_key == 'breed name':
					try:
						temp_dic["breed_id"] = breedDict[temp_val]
					except KeyError:
						temp_dic["breed_id"] = None
				elif temp_key == 'shelter name':
					try:
						temp_dic["shelter_id"] = shelterDict[temp_val]
					except KeyError:
						temp_dic["shelter_id"] = None
				else:
					if temp_val == '':
						temp_val = None
					temp_dic[temp_key] = temp_val
			addPet.append(temp_dic)
	return addPet

def table_to_id(tablename):
	result = {}
	conn = psycopg2.connect("dbname='pets' host='localhost'")
	cur = conn.cursor()
	cur.execute("SELECT * FROM {}".format(tablename))
	rows = cur.fetchall()
	for row in rows:
		result[row[1].lower()] = row[0]
	cur.close()
	conn.close()
	return result
	
def species_sql(addSpecies):
	conn = psycopg2.connect("dbname='pets' host='localhost'")
	cur = conn.cursor()
	cur.executemany("INSERT INTO species(name) VALUES (%(name)s)", addSpecies)
	conn.commit()
	cur.close()
	conn.close()

def breed_sql(addBreed):
	conn = psycopg2.connect("dbname='pets' host='localhost'")
	cur = conn.cursor()
	cur.executemany("""INSERT INTO breed(name, species_id) VALUES 
		(%(name)s, %(species_id)s)""", addBreed)
	conn.commit()
	cur.close()
	conn.close()

def shelter_sql(addShelter):
	conn = psycopg2.connect("dbname='pets' host='localhost'")
	cur = conn.cursor()
	cur.executemany("""INSERT INTO shelter(name, website, phone) VALUES
		(%(name)s, %(website)s, %(phone)s)""", addShelter)
	conn.commit()
	cur.close()
	conn.close()

def pet_sql(addPet):
	conn = psycopg2.connect("dbname='pets' host='localhost'")
	cur = conn.cursor()
	cur.executemany("""INSERT INTO pet(name, age, adopted, dead, breed_id, shelter_id)
		VALUES (%(name)s, %(age)s, %(adopted)s, %(dead)s, %(breed_id)s, %(shelter_id)s)""",
		addPet)
	conn.commit()
	cur.close()
	conn.close()

if __name__ == '__main__':
	addSpecies = add_species("pet_input.csv")
	if len(addSpecies) > 0:
		species_sql(addSpecies)
	addBreed = add_breed("pet_input.csv")
	if len(addBreed) > 0:
		breed_sql(addBreed)
	addShelter = add_shelter("pet_input.csv")
	if len(addShelter) > 0:
		shelter_sql(addShelter)
	addPet = add_pet("pet_input.csv")
	if len(addPet) > 0:
		pet_sql(addPet)
