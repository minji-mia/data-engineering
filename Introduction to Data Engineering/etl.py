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