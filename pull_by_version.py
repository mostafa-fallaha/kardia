import pandas as pd
import subprocess
from sqlalchemy import create_engine, text
import argparse
from dotenv import load_dotenv
import os

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

state_df.to_sql("d_state", con = engine_load, if_exists= 'append', index= False)
gender_df.to_sql("d_gender", con = engine_load, if_exists= 'append', index= False)
gen_health_df.to_sql("d_general_health", con = engine_load, if_exists= 'append', index= False)
last_checkup_df.to_sql("d_last_checkup_time", con = engine_load, if_exists= 'append', index= False)
rem_teeth_df.to_sql("d_removed_teeth", con = engine_load, if_exists= 'append', index= False)
diabetes_df.to_sql("d_diabetes_status", con = engine_load, if_exists= 'append', index= False)
smoking_status_df.to_sql("d_smoking_status", con = engine_load, if_exists= 'append', index= False)
e_cigarette_usage_df.to_sql("d_e_cigarette_usage", con = engine_load, if_exists= 'append', index= False)
race_ethnicity_category_df.to_sql("d_race_ethnicity_category", con = engine_load, if_exists= 'append', index= False)
age_category_df.to_sql("d_age_category", con = engine_load, if_exists= 'append', index= False)
tetanus_last_10_tdap_df.to_sql("d_tetanus_last_10_tdap", con = engine_load, if_exists= 'append', index= False)
covid_pos_df.to_sql("d_covid_pos", con = engine_load, if_exists= 'append', index= False)
heart_df.to_sql("f_data", con = engine_load, if_exists= 'append', index= False)

print("--- Done")