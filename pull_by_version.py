import pandas as pd
import subprocess
from sqlalchemy import create_engine, text, Table, Column, Integer, String, Text, MetaData, TIMESTAMP, insert
from sqlalchemy.orm import sessionmaker
import traceback
from datetime import datetime
import argparse
from dotenv import load_dotenv
import os
from ETL.validations import validate_transform_tables_rows

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Version of data by Git tag")
parser.add_argument("git_tag", type=str, help="The Git tag to check out")
parser.add_argument("branch_name", type=str, help="The main branch name to switch to")
args = parser.parse_args()

branch_name = args.branch_name
git_tag = args.git_tag
commit_message = f'went back to version {git_tag}'

# Run DVC and Git commands
subprocess.run(["git", "checkout", git_tag], check=True)
subprocess.run(["dvc", "checkout"], check=True)
subprocess.run(["git", "checkout", branch_name], check=True)

#========================================================================================
#========================== Reload the DWH ===============================================
heart_df = pd.read_csv('ETL/dvc_data/data/heart_data.csv')
state_df = pd.read_csv('ETL/dvc_data/data/state.csv')
gender_df = pd.read_csv('ETL/dvc_data/data/gender.csv')
gen_health_df = pd.read_csv('ETL/dvc_data/data/general_health.csv')
rem_teeth_df = pd.read_csv('ETL/dvc_data/data/removed_teeth.csv')
diabetes_df = pd.read_csv('ETL/dvc_data/data/diabetes_status.csv')
smoking_status_df = pd.read_csv('ETL/dvc_data/data/smoking_status.csv')
e_cigarette_usage_df = pd.read_csv('ETL/dvc_data/data/e_cigarette_usage.csv')
race_ethnicity_category_df = pd.read_csv('ETL/dvc_data/data/race_ethnicity_category.csv')
age_category_df = pd.read_csv('ETL/dvc_data/data/age_category.csv')
tetanus_last_10_tdap_df = pd.read_csv('ETL/dvc_data/data/tetanus_last_10_tdap.csv')
covid_pos_df = pd.read_csv('ETL/dvc_data/data/covid_pos.csv')
last_checkup_df = pd.read_csv('ETL/dvc_data/data/last_checkup.csv')

load_dotenv()
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
db_dwh = os.getenv('DB_DWH')
logs_db = os.getenv('LOGS_DB')

engine_load = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_dwh}')

with engine_load.begin() as connection:
    connection.execute(text("TRUNCATE TABLE f_data"))
    connection.execute(text("DELETE FROM d_state"))
    connection.execute(text("DELETE FROM d_gender"))
    connection.execute(text("DELETE FROM d_general_health"))
    connection.execute(text("DELETE FROM d_last_checkup_time"))
    connection.execute(text("DELETE FROM d_removed_teeth"))
    connection.execute(text("DELETE FROM d_diabetes_status"))
    connection.execute(text("DELETE FROM d_smoking_status"))
    connection.execute(text("DELETE FROM d_e_cigarette_usage"))
    connection.execute(text("DELETE FROM d_race_ethnicity_category"))
    connection.execute(text("DELETE FROM d_age_category"))
    connection.execute(text("DELETE FROM d_tetanus_last_10_tdap"))
    connection.execute(text("DELETE FROM d_covid_pos"))

print("--- DWH being loaded")

log_engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{logs_db}')
Session = sessionmaker(bind=log_engine)
session = Session()

metadata = MetaData()
logs_table = Table(
    'logs_table', metadata,
    Column('id', Integer, primary_key=True),
    Column('script_name', String(50)),
    Column('source_db', String(50)),
    Column('destination_db', String(50)),
    Column('name_table', String(50)),
    Column('log_message', Text),
    Column('log_time', TIMESTAMP, default=datetime.now())
)

log_entry = {
    "script_name": "transform.py",
    "source_db": "heart_staging_1",
    "destination_db": "heart_dwh",
    "name_table": "",
    "log_message": ""
}

def log_to_db(log_entry: dict):
    try:
        stmt = insert(logs_table).values(log_entry)
        session.execute(stmt)
        session.commit()
    except Exception as e:
        print(f"Failed to log to DB: {e}")
        session.rollback()

# --------------------------------------------------------------------------------------
log_entry['name_table'] = "d_state"
try:
    state_df.to_sql("d_state", con = engine_load, if_exists= 'append', index= False)
    val = validate_transform_tables_rows(state_df, "d_state")
    if val:
        log_entry["log_message"] = "The loaded State Data to the DWH is Valid"
    else:
        log_entry["log_message"] = "The loaded State Data to the DWH is Invalid"
    log_to_db(log_entry)
except Exception as e:
    log_entry["log_message"] = f"An error occurred: {traceback.format_exc()}"
    log_to_db(log_entry)

# --------------------------------------------------------------------------------------
log_entry['name_table'] = "d_gender"
try:
    gender_df.to_sql("d_gender", con = engine_load, if_exists= 'append', index= False)
    val = validate_transform_tables_rows(gender_df, "d_gender")
    if val:
        log_entry["log_message"] = "The loaded Gender Data to the DWH is Valid"
    else:
        log_entry["log_message"] = "The loaded Gender Data to the DWH is Invalid"
    log_to_db(log_entry)
except Exception as e:
    log_entry["log_message"] = f"An error occurred: {traceback.format_exc()}"
    log_to_db(log_entry)

# --------------------------------------------------------------------------------------
log_entry['name_table'] = "d_general_health"
try:
    gen_health_df.to_sql("d_general_health", con = engine_load, if_exists= 'append', index= False)
    val = validate_transform_tables_rows(gen_health_df, "d_general_health")
    if val:
        log_entry["log_message"] = "The loaded General Health Data to the DWH is Valid"
    else:
        log_entry["log_message"] = "The loaded General Health Data to the DWH is Invalid"
    log_to_db(log_entry)
except Exception as e:
    log_entry["log_message"] = f"An error occurred: {traceback.format_exc()}"
    log_to_db(log_entry)

# --------------------------------------------------------------------------------------
log_entry['name_table'] = "d_last_checkup_time"
try:
    last_checkup_df.to_sql("d_last_checkup_time", con = engine_load, if_exists= 'append', index= False)
    val = validate_transform_tables_rows(last_checkup_df, "d_last_checkup_time")
    if val:
        log_entry["log_message"] = "The loaded Last Checkup Data to the DWH is Valid"
    else:
        log_entry["log_message"] = "The loaded Last Checkup Data to the DWH is Invalid"
    log_to_db(log_entry)
except Exception as e:
    log_entry["log_message"] = f"An error occurred: {traceback.format_exc()}"
    log_to_db(log_entry)

# --------------------------------------------------------------------------------------
log_entry['name_table'] = "d_removed_teeth"
try:
    rem_teeth_df.to_sql("d_removed_teeth", con = engine_load, if_exists= 'append', index= False)
    val = validate_transform_tables_rows(rem_teeth_df, "d_removed_teeth")
    if val:
        log_entry["log_message"] = "The loaded Removed Teeth Data to the DWH is Valid"
    else:
        log_entry["log_message"] = "The loaded Removed Teeth Data to the DWH is Invalid"
    log_to_db(log_entry)
except Exception as e:
    log_entry["log_message"] = f"An error occurred: {traceback.format_exc()}"
    log_to_db(log_entry)

# --------------------------------------------------------------------------------------
log_entry['name_table'] = "d_diabetes_status"
try:
    diabetes_df.to_sql("d_diabetes_status", con = engine_load, if_exists= 'append', index= False)
    val = validate_transform_tables_rows(diabetes_df, "d_diabetes_status")
    if val:
        log_entry["log_message"] = "The loaded Diabetes Data to the DWH is Valid"
    else:
        log_entry["log_message"] = "The loaded Diabetes Data to the DWH is Invalid"
    log_to_db(log_entry)
except Exception as e:
    log_entry["log_message"] = f"An error occurred: {traceback.format_exc()}"
    log_to_db(log_entry)

# --------------------------------------------------------------------------------------
log_entry['name_table'] = "d_smoking_status"
try:
    smoking_status_df.to_sql("d_smoking_status", con = engine_load, if_exists= 'append', index= False)
    val = validate_transform_tables_rows(smoking_status_df, "d_smoking_status")
    if val:
        log_entry["log_message"] = "The loaded Smoking Status Data to the DWH is Valid"
    else:
        log_entry["log_message"] = "The loaded Smoking Status Data to the DWH is Invalid"
    log_to_db(log_entry)
except Exception as e:
    log_entry["log_message"] = f"An error occurred: {traceback.format_exc()}"
    log_to_db(log_entry)

# --------------------------------------------------------------------------------------
log_entry['name_table'] = "d_e_cigarette_usage"
try:
    e_cigarette_usage_df.to_sql("d_e_cigarette_usage", con = engine_load, if_exists= 'append', index= False)
    val = validate_transform_tables_rows(e_cigarette_usage_df, "d_e_cigarette_usage")
    if val:
        log_entry["log_message"] = "The loaded E-Cigarette Usage Data to the DWH is Valid"
    else:
        log_entry["log_message"] = "The loaded E-Cigarette Usage Data to the DWH is Invalid"
    log_to_db(log_entry)
except Exception as e:
    log_entry["log_message"] = f"An error occurred: {traceback.format_exc()}"
    log_to_db(log_entry)

# --------------------------------------------------------------------------------------
log_entry['name_table'] = "d_race_ethnicity_category"
try:
    race_ethnicity_category_df.to_sql("d_race_ethnicity_category", con = engine_load, if_exists= 'append', index= False)
    val = validate_transform_tables_rows(race_ethnicity_category_df, "d_race_ethnicity_category")
    if val:
        log_entry["log_message"] = "The loaded Race/Ethnicity Data to the DWH is Valid"
    else:
        log_entry["log_message"] = "The loaded Race/Ethnicity Data to the DWH is Invalid"
    log_to_db(log_entry)
except Exception as e:
    log_entry["log_message"] = f"An error occurred: {traceback.format_exc()}"
    log_to_db(log_entry)

# --------------------------------------------------------------------------------------
log_entry['name_table'] = "d_age_category"
try:
    age_category_df.to_sql("d_age_category", con = engine_load, if_exists= 'append', index= False)
    val = validate_transform_tables_rows(age_category_df, "d_age_category")
    if val:
        log_entry["log_message"] = "The loaded Age Category Data to the DWH is Valid"
    else:
        log_entry["log_message"] = "The loaded Age Category Data to the DWH is Invalid"
    log_to_db(log_entry)
except Exception as e:
    log_entry["log_message"] = f"An error occurred: {traceback.format_exc()}"
    log_to_db(log_entry)

# --------------------------------------------------------------------------------------
log_entry['name_table'] = "d_tetanus_last_10_tdap"
try:
    tetanus_last_10_tdap_df.to_sql("d_tetanus_last_10_tdap", con = engine_load, if_exists= 'append', index= False)
    val = validate_transform_tables_rows(tetanus_last_10_tdap_df, "d_tetanus_last_10_tdap")
    if val:
        log_entry["log_message"] = "The loaded Tetanus Data to the DWH is Valid"
    else:
        log_entry["log_message"] = "The loaded Tetanus Data to the DWH is Invalid"
    log_to_db(log_entry)
except Exception as e:
    log_entry["log_message"] = f"An error occurred: {traceback.format_exc()}"
    log_to_db(log_entry)

# --------------------------------------------------------------------------------------
log_entry['name_table'] = "d_covid_pos"
try:
    covid_pos_df.to_sql("d_covid_pos", con = engine_load, if_exists= 'append', index= False)
    val = validate_transform_tables_rows(covid_pos_df, "d_covid_pos")
    if val:
        log_entry["log_message"] = "The loaded Covid Status Data to the DWH is Valid"
    else:
        log_entry["log_message"] = "The loaded Covid Status Data to the DWH is Invalid"
    log_to_db(log_entry)
except Exception as e:
    log_entry["log_message"] = f"An error occurred: {traceback.format_exc()}"
    log_to_db(log_entry)

# --------------------------------------------------------------------------------------
log_entry['name_table'] = "f_data"
try:
    heart_df.to_sql("f_data", con = engine_load, if_exists= 'append', index= False)
    val = validate_transform_tables_rows(heart_df, "f_data")
    if val:
        log_entry["log_message"] = "The loaded Heart Data to the DWH is Valid"
    else:
        log_entry["log_message"] = "The loaded Heart Data to the DWH is Invalid"
    log_to_db(log_entry)
except Exception as e:
    log_entry["log_message"] = f"An error occurred: {traceback.format_exc()}"
    log_to_db(log_entry)

print("--- Done")