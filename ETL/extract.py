import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, Table, Column, Integer, String, Text, MetaData, TIMESTAMP, insert
from sqlalchemy.orm import sessionmaker
import traceback
from datetime import datetime
from dotenv import load_dotenv
import os
from validations import validate_extract_script

pd.set_option('display.max_columns', None)

heart_df = pd.read_parquet("C:/Users/MYCOM/Desktop/FSD/FinalProject/ETL/docs/heart_converted.parquet")

VAR_LIST_PATH = Path('C:/Users/MYCOM/Desktop/FSD/FinalProject/ETL/docs/vars_list_with_descriptions.txt')

NEW_VAR_NAMES = [
    "SurveyDate",
    "State",
    "Sex",
    "GeneralHealth",
    "PhysicalHealthDays",
    "MentalHealthDays",
    "LastCheckupTime",
    "PhysicalActivities",
    "SleepHours",
    "RemovedTeeth",
    "HadHeartAttack",
    "HadAngina",
    "HadStroke",
    "HadAsthma",
    "HadSkinCancer",
    "HadCOPD",
    "HadDepressiveDisorder",
    "HadKidneyDisease",
    "HadArthritis",
    "HadDiabetes",
    "DeafOrHardOfHearing",
    "BlindOrVisionDifficulty",
    "DifficultyConcentrating",
    "DifficultyWalking",
    "DifficultyDressingBathing",
    "DifficultyErrands",
    "SmokerStatus",
    "ECigaretteUsage",
    "ChestScan",
    "RaceEthnicityCategory",
    "AgeCategory",
    "HeightInMeters",
    "WeightInKilograms",
    "BMI",
    "AlcoholDrinkers",
    "HIVTesting",
    "FluVaxLast12",
    "PneumoVaxEver",
    "TetanusLast10Tdap",
    "HighRiskLastYear",
    "CovidPos"
]

var_list_df = pd.read_csv(VAR_LIST_PATH, sep=' - ', header=None, names=['Variable', 'Description'], engine='python')

var_list = var_list_df['Variable'].to_numpy()

heart_df = heart_df[var_list]

heart_df.columns = NEW_VAR_NAMES

heart_df.insert(0, 'id', heart_df.index + 1)

load_dotenv()

user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
db_staging = os.getenv('DB_STAGING')
logs_db = os.getenv('LOGS_DB')

load_engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_staging}')

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
    "script_name": "extract.py",
    "source_db": "heart_converted.parquet",
    "destination_db": "heart_staging_1",
    "name_table": "heart_data",
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

try:
    heart_df.to_sql("heart_data", con = load_engine, if_exists= 'replace', index= False)
    val = validate_extract_script(heart_df)
    if val:
        log_entry["log_message"] = "The loaded Heart Data to the staging DB is Valid"
    else:
        log_entry["log_message"] = "The loaded Heart Data to the staging DB is Invalid"
    log_to_db(log_entry)
except Exception as e:
    log_entry["log_message"] = f"An error occurred: {traceback.format_exc()}"
    log_to_db(log_entry)
