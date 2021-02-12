'''
Fetch from an API
In the last video, you've seen that you can extract data from an API by sending a request
to the API and parsing the response which was in JSON format. 
In this exercise, you'll be doing the same by using the requests library 
to send a request to the Hacker News API.

Hacker News is a social news aggregation website, 
specifically for articles related to computer science or the tech world in general. 
Each post on the website has a JSON representation, 
which you'll see in the response of the request in the exercise.
'''

import requests

# Fetch the Hackernews post
resp = requests.get("https://hacker-news.firebaseio.com/v0/item/16222426.json")

# Print the response parsed as JSON
print(resp.json())

# Assign the score of the test to post_score
post_score = resp.json()['score']
print(post_score)

'''
In order to connect to the database, you'll have to use a PostgreSQL connection URI,
which looks something like this:

postgresql://[user[:password]@][host][:port][/database]
'''

# Function to extract table to a pandas DataFrame
def extract_table_to_pandas(tablename, db_engine):
    query = "SELECT * FROM {}".format(tablename)
    return pd.read_sql(query, db_engine)

# Connect to the database using the connection URI
connection_uri = "postgresql://repl:password@localhost:5432/pagila" 
db_engine = sqlalchemy.create_engine(connection_uri)

# Extract the film table into a pandas DataFrame
extract_table_to_pandas('film', db_engine)

# Extract the customer table into a pandas DataFrame
extract_table_to_pandas('customer', db_engine)

'''
Splitting the rental rate
In the video exercise, you saw how to use pandas to split the email address column 
of the film table in order to extract the users' domain names. 
Suppose you would want to have a better understanding of the rates users pay for movies, 
so you decided to divide the rental_rate column into dollars and cents.

In this exercise, you will use the same techniques used in the video exercises to do just that! 
The film table has been loaded into the pandas DataFrame film_df. Remember, the goal is 
to split up the rental_rate column into dollars and cents.

Instructions
Use the .astype() method to convert the rental_rate column into a column of string objects, 
and assign the results to rental_rate_str.
Split rental_rate_str on '.' and expand the results into columns. Assign the results to rental_rate_expanded.
Assign the newly created columns into films_df using the column names rental_rate_dollar 
and rental_rate_cents respectively.
'''

# Get the rental rate column as a string
rental_rate_str = film_df.rental_rate.astype(str)

# Split up and expand the column
rental_rate_expanded = rental_rate_str.str.split('.', expand=True)

# Assign the columns to film_df
film_df = film_df.assign(
    rental_rate_dollar=rental_rate_expanded[0],
    rental_rate_cents=rental_rate_expanded[1],
)

'''
Joining with ratings
In the video exercise, you saw how to use transformations in PySpark 
by joining the film and ratings tables to create a new column 
that stores the average rating per customer. In this exercise, 
you're going to create more synergies between the film and ratings tables 
by using the same techniques you learned in the video exercise to calculate the average rating for every film.

The PySpark DataFrame with films, film_df and the PySpark DataFrame 
with ratings, rating_df, are available in your workspace.
'''

# Use groupBy and mean to aggregate the column
ratings_per_film_df = rating_df.groupBy('film_id').mean('rating')

# Join the tables using the film_id column
film_df_with_ratings = film_df.join(
    ratings_per_film_df,
    film_df.film_id==ratings_per_film_df.film_id
)

# Show the 5 first results
print(film_df_with_ratings.show(5))

'''
Writing to a file
In the video, you saw that files are often loaded into a MPP database like Redshift 
in order to make it available for analysis.

The typical workflow is to write the data into columnar data files. 
These data files are then uploaded to a storage system and from there, 
they can be copied into the data warehouse. In case of Amazon Redshift, 
the storage system would be S3, for example.

The first step is to write a file to the right format. 
For this exercises you'll choose the Apache Parquet file format.

There's a PySpark DataFrame called film_sdf and a pandas DataFrame called film_pdf in your workspace.
'''

# Write the pandas DataFrame to parquet
film_pdf.to_parquet("films_pdf.parquet")

# Write the PySpark DataFrame to parquet
film_sdf.write.parquet("films_sdf.parquet")

'''
Load into Postgres
In this exercise, you'll write out some data to a PostgreSQL data warehouse. 
That could be useful when you have a result of some transformations, 
and you want to use it in an application.

For example, the result of a transformation could have added a column with film recommendations, 
and you want to use them in your online store.

There's a pandas DataFrame called film_pdf in your workspace.

As a reminder, here's the structure of a connection URI for sqlalchemy:

postgresql://[user[:password]@][host][:port][/database]
'''

# Finish the connection URI
connection_uri = "postgresql://repl:password@localhost:5432/dwh"
db_engine_dwh = sqlalchemy.create_engine(connection_uri)

# Transformation step, join with recommendations data
film_pdf_joined = film_pdf.join(recommendations)

# Finish the .to_sql() call to write to store.film
film_pdf_joined.to_sql("film", db_engine_dwh, schema="store", if_exists="replace")

# Run the query to fetch the data
pd.read_sql("SELECT film_id, recommended_film_ids FROM store.film", db_engine_dwh)

'''
Defining a DAG
In the previous exercises you applied the three steps in the ETL process:

Extract: Extract the film PostgreSQL table into pandas.
Transform: Split the rental_rate column of the film DataFrame.
Load: Load a the film DataFrame into a PostgreSQL data warehouse.
The functions extract_film_to_pandas(), transform_rental_rate() and load_dataframe_to_film() 
are defined in your workspace. In this exercise, you'll add an ETL task to an existing DAG. 
The DAG to extend and the task to wait for are defined in your workspace are defined 
as dag and wait_for_table respectively.
'''

# Define the ETL function
def etl():
    film_df = extract_film_to_pandas()
    film_df = transform_rental_rate(film_df)
    load_dataframe_to_film(film_df)

# Define the ETL task using PythonOperator
etl_task = PythonOperator(task_id='etl_film',
                          python_callable=etl,
                          dag=dag)

# Set the upstream to wait_for_table and sample run etl()
etl_task.set_upstream(wait_for_table)
etl()

'''
Setting up Airflow
In this exercise, you'll learn how to add a DAG to Airflow. 
To the right, you have a terminal at your disposal. 
The workspace comes with Airflow pre-configured, but it's easy to install on your own.

You'll need to move the dag.py file containing the DAG you defined in the previous exercise to, 
the DAGs folder. Here are the steps to find it:

The airflow home directory is defined in the AIRFLOW_HOME environment variable. 
Type echo $AIRFLOW_HOME to find out.
In this directory, find the airflow.cfg file. Use head to read the file, and find the value of the dags_folder.
Now you can find the folder and move the dag.py file there: mv ./dag.py <dags_folder>.

Which files does the DAGs folder have after you moved the file?
'''