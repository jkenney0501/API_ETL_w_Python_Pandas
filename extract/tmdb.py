
# ETL pipeline in Python with Pandas 

# import nedded libraries
import pandas as pd
import requests
import config 


# ******************** Start with API request and Extract Process **********************
# create an empty list to grab responses (r)
response_list = []

API_KEY = config.api_key

# loop through argument range, send request to API and get response
for movie_id in range(550,556): 
  url = 'https://api.themoviedb.org/3/movie/{}?api_key={}'.format(movie_id, API_KEY)
  r = requests.get(url)
  response_list.append(r.json())

 # print(movie_id)

#  print the response in a pandas table
df_movies_response = pd.DataFrame.from_dict(response_list)

# print table - should be 6 rows, 26 columns
print(df_movies_response)

# see a lsit of columns since table is rather large
print(df_movies_response.columns)

# see generes, returns json so its bneed to be broken out into a readbale.useable format (think of this as falttening the data for analysts downstream)
print(df_movies_response['genres'])



# ******************************* Transform ************************************
# genre column is in json format and needs to be "flattened" b/c it is hard to read currebtly.
# One way to do this is by exploding out the column of lists into one-hot categorical columns. 
# This is done by creating a single column for each categorical value and setting the row value to 1 if the movie belongs to that category and 0 if it doesn’t. 


# get specified columns by calling them in a list
df_movie_cols = ['budget', 'genres', 'id', 'original_title', 'release_date', 'revenue', 'runtime']

# We’ll only be using the name property, not the id. We’ll be able to access the id value in a separate table.
genres_list = df_movies_response['genres'].tolist()

# flatten the list
flat_list = [item for sublist in genres_list for item in sublist]



# We want to create a separate table for genres and a column of lists to explode out. 
# We’ll create a temporary column called genres_all as a list of lists of genres that we can later expand out into a separate column for each genre.
result = []
for l in genres_list:
    r = []
    for d in l:
        r.append(d['name'])
    result.append(r)
df = df_movies_response.assign(genres_all=result)


# create the genres table, we later join this on id to use this as a dimension
# this created a dataframe that is essentialy a look up table with the id and name
# we can now use this to identify the movies genre with one hot enncoding
df_genres = pd.DataFrame.from_records(flat_list).drop_duplicates()

print(df_genres)

# This gives us a table of the genre properties name and id. 
# We attach the list of genre names onto our df_columns list as shown below and remove the original genres column from the list.


# select columns to be used (not all are needed) and put them in a list.
# The last two lines are where we use the explode() and crosstab() functions to create the genre columns and join them onto the main table. 
# This takes a column of lists and turns it into a set of columns of frequency values.
# The last two lines are where we use the explode() and crosstab() functions to create the genre columns and join them onto the main table. 
# This takes a column of lists and turns it into a set of columns of frequency values.

df_columns = ['budget', 'id', 'imdb_id', 'original_title', 'release_date', 'revenue', 'runtime']

df_genre_columns = df_genres['name'].to_list()

# adds df genre to the df columns list
df_columns.extend(df_genre_columns)

# explode and join tables
s = df['genres_all'].explode()

df = df.join(pd.crosstab(s.index, s))

# print table - table now contains binary cols for exmploded genres. 
print(df[df_columns])


# ************** Creating a datetime table to reference ***********************
# Pandas has built-in functions to extract specific parts of a datetime. 
# Notice we need to convert the release_date column into a datetime first.
df['release_date'] = pd.to_datetime(df['release_date'])
df['day'] = df['release_date'].dt.day
df['month'] = df['release_date'].dt.month
df['year'] = df['release_date'].dt.year
df['day_of_week'] = df['release_date'].dt.day_name()
df_time_columns = ['id', 'release_date', 'day', 'month', 'year', 'day_of_week']

# this can be joined to df_colmns to add details for seprate columns of various date details
# you can also create a recurseive function to make a date table but not neccessary here (this is more of an RDBMS/SQL technique).
print(df[df_time_columns])


# *************************** Load Data ******************************
# This is being loaded to CSV but it simulates loading to the sink in cloud storage or an RDBMS, 
# the connect string would be added and destination would change otherwise its very much the same.
# to add data daily in an automated process you would devlop a "boundary" for dates, to do this
# you would add a column that would capture the current date of that load. This is typically called "load_dt" 
# then taking the max load_dt and setting arange between laod_dt and today would give you any new records in this interval
# we wont do this here but it is common for incrementqal loads in the cloud or in RDBMS.

df[df_columns].to_csv('movies_path', index = False)

df_genres.to_csv('genres_path', index=False)

df[df_time_columns].to_csv('date_path', index=False)