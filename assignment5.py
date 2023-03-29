import mysql.connector
import json
import csv

from mysql.connector import Error

# method that establishes a connection, can be passed with or without a specific database
def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:

        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )

        print("Connection to MySQL DB successful")

    except Error as e:
        print(f"The error '{e}' occurred")

    return connection

# method that creates a database
def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as e:
        print(f"The error '{e}' occurred")


# method that executes a query
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")

# first method that creates database and all relations
def create_relations(username, password, database_name):

	# connect to the host
	connection = create_connection("localhost", username, password, "")

	# create the database
	create_database_query = "CREATE DATABASE %s" % (database_name)
	create_database(connection, create_database_query)


	# connect to the actual database
	connection = create_connection("localhost", username, password, database_name)

	create_movie_table = """
		CREATE TABLE IF NOT EXISTS movies (
		budget INT UNSIGNED,
		homepage TEXT,
		id INT,
		original_lang VARCHAR(20),
		original_title TEXT,
		overview TEXT,
		popularity DOUBLE,
		release_date VARCHAR(20),
		revenue INT UNSIGNED,
		runtime INT,
		status VARCHAR(50),
		tagline TEXT,
		title TEXT,
		vote_average DOUBLE,
		vote_count INT,
		PRIMARY KEY (id)
		)
		"""

	execute_query(connection, create_movie_table)


	# GENRES

	# create relation containing genres
	create_genre_table = """
		CREATE TABLE IF NOT EXISTS genres (
		genre_id INT,
		genre_name VARCHAR(20),
		PRIMARY KEY (genre_id)
		)
		"""

	execute_query(connection, create_genre_table)

	# create table linking genres to movies
	create_genre_link_table = """
		CREATE TABLE IF NOT EXISTS genre_links (
		genre_id INT,
		movie_id INT,
		relation_id INT AUTO_INCREMENT,
		PRIMARY KEY (relation_id),
		FOREIGN KEY (genre_id) REFERENCES genres(genre_id),
		FOREIGN KEY (movie_id) REFERENCES movies(id)
		)
		"""

	execute_query(connection, create_genre_link_table)



	# KEYWORDS

	# create relation containing keywords
	create_keyword_table = """
		CREATE TABLE IF NOT EXISTS keywords (
		keyword_id INT,
		keyword_name VARCHAR(20),
		PRIMARY KEY (keyword_id)
		)
		"""

	execute_query(connection, create_keyword_table)

	# create table linking genres to movies
	create_keyword_link_table = """
		CREATE TABLE IF NOT EXISTS keyword_links (
		keyword_id INT,
		movie_id INT,
		PRIMARY KEY (keyword_id, movie_id),
		FOREIGN KEY (keyword_id) REFERENCES keywords(keyword_id),
		FOREIGN KEY (movie_id) REFERENCES movies(id)
		)
		"""

	execute_query(connection, create_keyword_link_table)



	# PRODUCTION COMPANIES

	# create table containing production compaines
	create_production_company_table = """
		CREATE TABLE IF NOT EXISTS production_companies (
		production_company_id INT,
		production_company_name VARCHAR(50),
		PRIMARY KEY (production_company_id)
		)
		"""

	execute_query(connection, create_production_company_table)

	# create table linking production company to movies
	create_production_company_link_table = """
		CREATE TABLE IF NOT EXISTS production_company_links (
		production_company_id INT,
		movie_id INT,
		PRIMARY KEY (production_company_id, movie_id),
		FOREIGN KEY (production_company_id) REFERENCES production_companies(production_company_id),
		FOREIGN KEY (movie_id) REFERENCES movies(id)
		)
		"""

	execute_query(connection, create_production_company_link_table)



	# PRODUCTION COUNTRIES


	# create table containing production countries
	create_production_country_table = """
		CREATE TABLE IF NOT EXISTS production_countries (
		production_country_iso VARCHAR(5),
		production_country_name VARCHAR(50),
		PRIMARY KEY (production_country_iso)
		)
		"""

	execute_query(connection, create_production_country_table)

	# create table linking production countries to movies
	create_production_country_link_table = """
		CREATE TABLE IF NOT EXISTS production_country_links (
		production_country_iso VARCHAR(5),
		movie_id INT,
		PRIMARY KEY (production_country_iso, movie_id),
		FOREIGN KEY (production_country_iso) REFERENCES production_countries(production_country_iso),
		FOREIGN KEY (movie_id) REFERENCES movies(id)
		)
		"""

	execute_query(connection, create_production_country_link_table)



	# SPOKEN LANGUAGE


	# create table containing spoken language
	create_language_table = """
		CREATE TABLE IF NOT EXISTS languages (
		language_iso VARCHAR(5),
		language_name VARCHAR(20),
		PRIMARY KEY (language_iso)
		)
		"""

	execute_query(connection, create_language_table)

	# create table linking language to movies
	create_language_link_table = """
		CREATE TABLE IF NOT EXISTS language_links (
		language_iso VARCHAR(5),
		movie_id INT,
		PRIMARY KEY (language_iso, movie_id),
		FOREIGN KEY (language_iso) REFERENCES languages(language_iso),
		FOREIGN KEY (movie_id) REFERENCES movies(id)
		)
		"""

	execute_query(connection, create_language_link_table)



def insert_data(username, password, database_name, filename):

	# connect to the actual database
	connection = create_connection("localhost", username, password, database_name)

	#Create a cursor object
	cursor = connection.cursor()


	with open(filename, encoding='utf-8') as csvf:

		reader = csv.DictReader(csvf)

		count = 0

		jsonArray = []

		for fields in reader:

			#jsonArray.append(row)

			budget = int(fields['budget'])
			genres = json.loads(fields['genres'])
			homepage = fields['homepage']
			movie_id = int(fields['id'])
			keywords = json.loads(fields['keywords'])
			original_language = fields['original_language']
			original_title = fields['original_title']
			overview = fields['overview']
			popularity = float(fields['popularity'])
			production_companies = json.loads(fields['production_companies'])
			production_countries = json.loads(fields['production_countries'])
			release_date = fields['release_date']
			revenue = int(fields['revenue'])

			if fields['runtime'] == "":
				fields['runtime'] = 0
			runtime = round(float(fields['runtime']))

			languages = json.loads(fields['spoken_languages'])
			status = fields['status']
			tagline = fields['tagline']
			title = fields['title']
			vote_average = float(fields['vote_average'])
			vote_count = float(fields['vote_count'])

			#count +=1
			#print(count)
			#if(count<2):
			#	print(fields['genres'])


			# INSERT string
			sql = """INSERT INTO movies
			   (budget, homepage, id, original_lang, original_title, overview, popularity, release_date,
			   revenue, runtime, status, tagline, title, vote_average, vote_count)
			   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

			val = (budget, homepage, movie_id, original_language, original_title, overview, popularity, \
				release_date, revenue, runtime, status, tagline, title, vote_average, vote_count)
			# Executing the SQL command
			cursor.execute(sql, val)

			# Commit your changes in the database
			connection.commit()



			# Insert genres
			for x in genres:

				sql = """INSERT IGNORE INTO genres
				   (genre_id, genre_name)
				   VALUES (%s, %s)"""

				val = (x["id"], x["name"])
				# Executing the SQL command
				cursor.execute(sql, val)

				# Commit your changes in the database
				connection.commit()

				# insert genre links
				sql = """INSERT IGNORE INTO genre_links
				   (genre_id, movie_id)
				   VALUES (%s, %s)"""

				val = (x["id"], movie_id)
				# Executing the SQL command
				cursor.execute(sql, val)

				# Commit your changes in the database
				connection.commit()



			# Insert keywords
			for x in keywords:

				sql = """INSERT IGNORE INTO keywords
				   (keyword_id, keyword_name)
				   VALUES (%s, %s)"""

				val = (x["id"], x["name"])
				# Executing the SQL command
				cursor.execute(sql, val)

				# Commit your changes in the database
				connection.commit()

				# insert genre links
				sql = """INSERT IGNORE INTO keyword_links
				   (keyword_id, movie_id)
				   VALUES (%s, %s)"""

				val = (x["id"], movie_id)
				# Executing the SQL command
				cursor.execute(sql, val)

				# Commit your changes in the database
				connection.commit()



			# Insert production companies
			for x in production_companies:

				sql = """INSERT IGNORE INTO production_companies
				   (production_company_id, production_company_name)
				   VALUES (%s, %s)"""

				val = (x["id"], x["name"])
				# Executing the SQL command
				cursor.execute(sql, val)

				# Commit your changes in the database
				connection.commit()

				# insert genre links
				sql = """INSERT IGNORE INTO production_company_links
				   (production_company_id, movie_id)
				   VALUES (%s, %s)"""

				val = (x["id"], movie_id)
				# Executing the SQL command
				cursor.execute(sql, val)

				# Commit your changes in the database
				connection.commit()



			# Insert production countries
			for x in production_countries:

				sql = """INSERT IGNORE INTO production_countries
				   (production_country_iso, production_country_name)
				   VALUES (%s, %s)"""

				val = (x["iso_3166_1"], x["name"])
				# Executing the SQL command
				cursor.execute(sql, val)

				# Commit your changes in the database
				connection.commit()

				# insert genre links
				sql = """INSERT IGNORE INTO production_country_links
				   (production_country_iso, movie_id)
				   VALUES (%s, %s)"""

				val = (x["iso_3166_1"], movie_id)
				# Executing the SQL command
				cursor.execute(sql, val)

				# Commit your changes in the database
				connection.commit()



			# Insert spoken languages
			for x in languages:

				sql = """INSERT IGNORE INTO languages
				   (language_iso, language_name)
				   VALUES (%s, %s)"""

				val = (x["iso_639_1"], x["name"])
				# Executing the SQL command
				cursor.execute(sql, val)

				# Commit your changes in the database
				connection.commit()

				# insert genre links
				sql = """INSERT IGNORE INTO language_links
				   (language_iso, movie_id)
				   VALUES (%s, %s)"""

				val = (x["iso_639_1"], movie_id)
				# Executing the SQL command
				cursor.execute(sql, val)

				# Commit your changes in the database
				connection.commit()







	cursor.close()
	connection.close()

if __name__ == "__main__":
	#create_relations("root", "tempPassword", "movies_database")

	#insert_data("root", "tempPassword", "movies_database", "tmdb_5000_movies.csv")



	# connect to the actual database
	connection = create_connection("localhost", "root", "tempPassword", "movies_database")

	#Create a cursor object
	cursor = connection.cursor()


	#Query 1
	query = "SELECT AVG(budget) FROM movies"
	cursor.execute(query)
	# get all records
	records = cursor.fetchall()

	print("\nQuery 1: average budget of all movies")
	for row in records:
		print(row[0])


	#Query 2
	query = """SELECT title, production_company_name FROM production_companies INNER JOIN
				(SELECT title, production_company_id FROM production_company_links INNER JOIN
					(SELECT title, id FROM movies INNER JOIN
						(SELECT movie_id FROM production_country_links
						WHERE production_country_iso = 'US') AS x
					ON movies.id = x.movie_id) AS y
				ON production_company_links.movie_id = y.id) AS z
			ON production_companies.production_company_id = z.production_company_id"""

	cursor.execute(query)
	# get all records
	records = cursor.fetchall()

	print("\nQuery 2: movies produced in US")
	count = 0
	for row in records:
		if count > 5:
			break
		print(row)
		count+=1


	#Query 3
	query = """SELECT title, revenue FROM movies ORDER BY revenue DESC LIMIT 5"""

	cursor.execute(query)
	# get all records
	records = cursor.fetchall()

	print("\nQuery 3: 5 movies that made most revenue")

	for row in records:
		print(row)



	#Query 4

	query = """SELECT id, title, genre_name FROM genres INNER JOIN
					(SELECT id, title, genre_id FROM genre_links INNER JOIN
						(SELECT id, title FROM movies INNER JOIN
							(SELECT DISTINCT movie_id
							FROM genre_links WHERE movie_id in
			 				(SELECT movie_id FROM genre_links JOIN
			 				genres WHERE genre_name='Science Fiction')
			 				AND movie_id in
							(SELECT movie_id FROM genre_links JOIN
							genres WHERE genre_name='Mystery')
							) AS bothGen
						ON movies.id = bothGen.movie_id) AS withIDs
					ON genre_links.movie_id = withIDs.id) AS withGenreID
				ON genres.genre_id = withGenreID.genre_id ORDER BY id"""

	cursor.execute(query)
	# get all records
	records = cursor.fetchall()

	print("\nQuery 4: movies with both science fiction and myster genres")
	count = 0
	currentTitle = ""
	currentID = 0
	currentGenres = ""
	print("(title, list of genres)")
	for row in records:
		# same id means add to the output string
		if currentID == row[0]:
			currentGenres = currentGenres + row[2] + ", "
		# new movie, print old line
		else:
			currentGenres = currentGenres[:-2]
			# ignore first pass
			if currentID != 0:
				print("(%s, (%s))" % (currentTitle, currentGenres))

			# set new
			currentID = row[0]
			currentTitle = row[1]
			currentGenres = row[2] + ", "






	#Query 5
	query = """SELECT title, popularity FROM movies
				WHERE popularity > (SELECT avg(popularity) FROM movies)"""

	cursor.execute(query)
	# get all records
	records = cursor.fetchall()

	print("\nQuery 5: movies with popularity greater than the average")
	count = 0
	for row in records:
		if count > 5:
			break
		print(row)
		count+=1
