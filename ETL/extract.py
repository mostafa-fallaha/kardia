import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
pd.set_option('display.max_columns', None)
from dotenv import load_dotenv
import os

heart_df = pd.read_parquet("ETL/docs/heart_converted.parquet")

VAR_LIST_PATH = Path('ETL/docs/vars_list_with_descriptions.txt')

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

engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_staging}')

heart_df.to_sql("heart_data", con = engine, if_exists= 'replace', index= False)