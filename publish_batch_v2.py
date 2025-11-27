import pandas as pd
from sqlalchemy import create_engine
import gspread
from gspread_dataframe import set_with_dataframe
 
engine = create_engine(
    "postgresql://postgres:postgres@localhost:5432/postgres"
)
 
query = "SELECT * FROM raw_data.covid19_tweets"
df = pd.read_sql(query, engine)
 
print("Loaded production data:", df.shape)
 
df = df.fillna("")   
 
df = df.astype(str)
 
gc = gspread.service_account(filename="credentials.json.json")
 
SHEET_ID = "1j70V4BtA1fMY9fw-c2xUqgvdapkEocWH0_U5_a-PPxU"  
sh = gc.open_by_key(SHEET_ID)
worksheet = sh.sheet1
 
worksheet.clear()
 
BATCH_SIZE = 10000
 
header = list(df.columns)
worksheet.update(values=[header], range_name="A1")
 
start_row = 2
 
for i in range(0, len(df), BATCH_SIZE):
    batch = df.iloc[i:i + BATCH_SIZE]
    print(f"Uploading rows {i} → {i + len(batch)}...")
 
    data = batch.values.tolist()
 
    end_col = chr(65 + len(header) - 1)
    end_row = start_row + len(batch) - 1
    cell_range = f"A{start_row}:{end_col}{end_row}"
 
    worksheet.update(values=data, range_name=cell_range)
    start_row += len(batch)
 
print("✔")