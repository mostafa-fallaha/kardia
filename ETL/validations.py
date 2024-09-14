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
        return "The loaded Heart Data to the staging area is Valid"
    else:
        return "The loaded Heart Data to the staging area is Invalid"

# ============================== Heart ============================================================

def validate_heart_rows(heart_copy_df: pd.DataFrame):
    engine_read = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_dwh}')
    read_df = pd.read_sql('f_data', con = engine_read)
    if read_df.shape == heart_copy_df.shape:
        return "The loaded Heart Data to the DWH is Valid"
    else:
        return "The loaded Heart Data to the DWH is Invalid"

# ============================== State ============================================================

def validate_state_rows(state_df: pd.DataFrame):
    engine_read = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_dwh}')
    read_df = pd.read_sql('d_state', con = engine_read)
    if read_df.shape == state_df.shape:
        return "The loaded State Data to the DWH is Valid"
    else:
        return "The loaded State Data to the DWH is Invalid"

# ============================== Gender ============================================================

def validate_gender_rows(sex_df: pd.DataFrame):
    engine_read = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_dwh}')
    read_df = pd.read_sql('d_gender', con = engine_read)
    if read_df.shape == sex_df.shape:
        return "The loaded Gender Data to the DWH is Valid"
    else:
        return "The loaded Gender Data to the DWH is Invalid"

# ============================== General Health ============================================================

def validate_gen_health_rows(gen_health_df: pd.DataFrame):
    engine_read = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_dwh}')
    read_df = pd.read_sql('d_general_health', con = engine_read)
    if read_df.shape == gen_health_df.shape:
        return "The loaded General Health Data to the DWH is Valid"
    else:
        return "The loaded General Health Data to the DWH is Invalid"

# ============================== Last Checkup ============================================================

def validate_last_checkup_rows(last_checkup_df: pd.DataFrame):
    engine_read = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_dwh}')
    read_df = pd.read_sql('d_last_checkup_time', con = engine_read)
    if read_df.shape == last_checkup_df.shape:
        return "The loaded Last Checkup Data to the DWH is Valid"
    else:
        return "The loaded Last Checkup Data to the DWH is Invalid"

# ============================== Removed Teeth ============================================================

def validate_rem_teeth_rows(rem_teeth_df: pd.DataFrame):
    engine_read = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_dwh}')
    read_df = pd.read_sql('d_removed_teeth', con = engine_read)
    if read_df.shape == rem_teeth_df.shape:
        return "The loaded Removed Teeth Data to the DWH is Valid"
    else:
        return "The loaded Removed Teeth Data to the DWH is Invalid"

# ============================== Diabetes ============================================================

def validate_diabetes_rows(diabetes_df: pd.DataFrame):
    engine_read = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_dwh}')
    read_df = pd.read_sql('d_diabetes_status', con = engine_read)
    if read_df.shape == diabetes_df.shape:
        return "The loaded Diabetes Data to the DWH is Valid"
    else:
        return "The loaded Diabetes Data to the DWH is Invalid"

# ============================== Smoking Status ============================================================

def validate_smoking_status_rows(smoking_status_df: pd.DataFrame):
    engine_read = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_dwh}')
    read_df = pd.read_sql('d_smoking_status', con = engine_read)
    if read_df.shape == smoking_status_df.shape:
        return "The loaded Smoking Status Data to the DWH is Valid"
    else:
        return "The loaded Smoking Status Data to the DWH is Invalid"

# ============================== E-Cigarette Usage ============================================================

def validate_e_cigarette_usage_rows(e_cigarette_usage_df: pd.DataFrame):
    engine_read = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_dwh}')
    read_df = pd.read_sql('d_e_cigarette_usage', con = engine_read)
    if read_df.shape == e_cigarette_usage_df.shape:
        return "The loaded E-Cigarette Usage Data to the DWH is Valid"
    else:
        return "The loaded E-Cigarette Usage Data to the DWH is Invalid"

# ============================== Race/Ethnicity ============================================================

def validate_race_ethnicity_category_rows(race_ethnicity_category_df: pd.DataFrame):
    engine_read = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_dwh}')
    read_df = pd.read_sql('d_race_ethnicity_category', con = engine_read)
    if read_df.shape == race_ethnicity_category_df.shape:
        return "The loaded Race/Ethnicity Data to the DWH is Valid"
    else:
        return "The loaded Race/Ethnicity Data to the DWH is Invalid"

# ============================== Age Category ============================================================

def validate_age_category_rows(age_category_df: pd.DataFrame):
    engine_read = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_dwh}')
    read_df = pd.read_sql('d_age_category', con = engine_read)
    if read_df.shape == age_category_df.shape:
        return "The loaded Age Category Data to the DWH is Valid"
    else:
        return "The loaded Age Category Data to the DWH is Invalid"

# ============================== Tetanus ============================================================

def validate_tetanus_last_10_tdap_rows(tetanus_last_10_tdap_df: pd.DataFrame):
    engine_read = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_dwh}')
    read_df = pd.read_sql('d_tetanus_last_10_tdap', con = engine_read)
    if read_df.shape == tetanus_last_10_tdap_df.shape:
        return "The loaded Tetanus Data to the DWH is Valid"
    else:
        return "The loaded Tetanus Data to the DWH is Invalid"

# ============================== Covid Status ============================================================

def validate_covid_pos_rows(covid_pos_df: pd.DataFrame):
    engine_read = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_dwh}')
    read_df = pd.read_sql('d_covid_pos', con = engine_read)
    if read_df.shape == covid_pos_df.shape:
        return "The loaded Covid Status Data to the DWH is Valid"
    else:
        return "The loaded Covid Status Data to the DWH is Invalid"
