import pandas as pd
import numpy as np
import re

df = pd.read_csv("covid19_tweets_220k.csv")

# 1) Strip whitespace (not touching user_name)
for col in df.columns:
    if df[col].dtype == "object" and col != "user_name":
        df[col] = df[col].astype(str).str.strip()

# 2) Fix weird encoding (without unidecode)
def fix_unicode(text):
    try:
        return text.encode("latin1", "ignore").decode("utf-8", "ignore")
    except:
        return str(text)

for col in df.columns:
    if df[col].dtype == 'object' and col != 'user_name':
        df[col] = df[col].apply(lambda x: fix_unicode(str(x)))

# 3) Remove emoji
emoji_pattern = re.compile("["
    u"\U0001F600-\U0001F64F"
    u"\U0001F300-\U0001F5FF"
    u"\U0001F680-\U0001F6FF"
    u"\U0001F1E0-\U0001F1FF"
"]+", flags=re.UNICODE)

for col in df.columns:
    if df[col].dtype == "object" and col != "user_name":
        df[col] = df[col].apply(lambda x: emoji_pattern.sub("", str(x)))

# 4) Fix user_location
valid_locations = [
    "Bangkok, Thailand", "Tokyo, Japan", "Seoul, South Korea",
    "Paris, France", "London, United Kingdom", "New York, USA",
    "Los Angeles, USA", "Berlin, Germany", "Sydney, Australia",
    "Toronto, Canada"
]

def clean_location(loc):
    text = str(loc).strip()
    if len(text) < 3 or text.lower() in ["nan", "none", "null", "unknown"]:
        return np.random.choice(valid_locations)
    if re.search(r"[^a-zA-Z0-9 ,.-]", text):
        return np.random.choice(valid_locations)
    return text

df["user_location"] = df["user_location"].apply(clean_location)

# 5) Replace missing values
for col in df.columns:
    if df[col].dtype == "object" and col != "user_name":
        df[col].replace(["nan", "None", "NULL", ""], np.nan, inplace=True)
        df[col].fillna("Unknown", inplace=True)

# 6) Drop duplicates
df.drop_duplicates(inplace=True)

df.to_csv("covid19_clean.csv", index=False)
print("Clean เสร็จแล้ว!")
