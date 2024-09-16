import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
db_staging = os.getenv('DB_STAGING')
db_dwh = os.getenv('DB_DWH')

# ============================== Extract ============================================================

def validate_extract_script(heart_df: pd.DataFrame):
    engine_read = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_staging}')
    read_df = pd.read_sql('heart_data', con = engine_read)
    if read_df.shape == heart_df.shape:
        return True
    else:
        return False

# ============================== Transform ==========================================================

def validate_transform_tables_rows(check_df: pd.DataFrame, table_name: str):
    engine_read = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_dwh}')
    read_df = pd.read_sql(table_name, con = engine_read)
    if read_df.shape == check_df.shape:
        return True
    else:
        return False