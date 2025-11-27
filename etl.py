import pandas as pd
from sqlalchemy import create_engine

df = pd.read_csv("covid19_tweets_220k_.csv", low_memory=False)

engine = create_engine(
    "postgresql://postgres:postgres@localhost:5432/postgres"
)

df.to_sql(
    "covid19_tweets",
    engine,
    schema="raw_data",
    if_exists="replace",
    index=False
)

print("ผ่าน")