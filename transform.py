import pandas as pd

from sqlalchemy import create_engine
 

engine = create_engine(

    "postgresql://postgres:postgres@localhost:5432/postgres"

)
 

query = "SELECT * FROM raw_data.covid19_tweets_raw;"

df = pd.read_sql(query, engine)
 
print("Loaded RAW data:", df.shape)
 

df['user_location'] = df['user_location'].fillna("Unknown")

df['text'] = df['text'].fillna("")
 
df['user_location'] = df['user_location'].astype(str).str.strip()

df['text'] = df['text'].astype(str).str.strip()
 

df['word_count'] = df['text'].apply(lambda x: len(str(x).split()))
 

df['char_count'] = df['text'].apply(lambda x: len(str(x)))
 

df['user_location_clean'] = df['user_location'].str.title()
 

location_summary = (

    df.groupby('user_location_clean')

      .size()

      .reset_index(name='tweet_count')

      .sort_values(by='tweet_count', ascending=False)

)
 

df.to_sql(

    "covid19_tweets_transformed",

    engine,

    schema="production",

    if_exists="replace",

    index=False

)
 

location_summary.to_sql(

    "location_summary",

    engine,

    schema="production",

    if_exists="replace",

    index=False

)
 
print("✔")


 