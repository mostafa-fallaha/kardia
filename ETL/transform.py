import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine, text, Table, Column, Integer, String, Text, MetaData, TIMESTAMP, insert
from sqlalchemy.orm import sessionmaker
import traceback
from datetime import datetime
import subprocess
from dotenv import load_dotenv
import os
pd.set_option('display.max_columns', None)

script_dir = os.path.dirname(__file__)
versioning_script = os.path.join(script_dir, '..', 'run_versioning.ps1')

load_dotenv()
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
db_staging = os.getenv('DB_STAGING')
db_dwh = os.getenv('DB_DWH')
logs_db = os.getenv('LOGS_DB')

engine_read = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_staging}')

heart_df = pd.read_sql('heart_data', con = engine_read)

# ============================================= Cleaning and Transforming ===================================================

def adjust_invalid_date(date_str):
    month = int(date_str[:2])
    day = int(date_str[2:4])
    year = int(date_str[4:])
    
    if month == 0 or month > 12:
        month = 1
    
    if day == 0:
        day = 1

    adjusted_date_str = f"{year}-{month:02d}-{day:02d}"
    return adjusted_date_str
    
heart_df['SurveyDate'] = heart_df['SurveyDate'].apply(adjust_invalid_date)
heart_df['SurveyDate'] = pd.to_datetime(heart_df['SurveyDate'])


heart_copy_df = heart_df.copy()


# for c in heart_df.columns:
#     print("\n", heart_df[c].value_counts().sort_index())
#     print("-"*30, end="\n")


sleep_hours_median = heart_copy_df['SleepHours'].median()

GEN_HEALTH = {
    1: 1,
    2: 2,
    3: 3,
    4: 4,
    5: 5
}

PHYS_MEN_HEALTH = {77: np.nan, 88: 0, 99: np.nan}

LAST_CHECKUP = {
    1: 1,
    2: 2,
    3: 3,
    4: 4
}

YES_NO_QUESTIONS = {1: 1, 2: 0}

SLEEP_TIME = lambda x: np.where(x > 24, sleep_hours_median, x)

TEETH_REMOVED = {
    1: 1,
    2: 2,
    3: 3,
    8: 4
}

DIABETES = {
    1: 1,
    2: 2,
    3: 3,
    4: 4,
}

SMOKER_STATUS = {
    1: 1,
    2: 2,
    3: 3,
    4: 4
}

ECIGARETTES = {
    1: 1,
    2: 2,
    3: 3,
    4: 4
}

RACE = {
    1: 1,
    2: 2,
    3: 3,
    4: 4,
    5: 5
}

AGE_CATEGORY = {
    1: 1,
    2: 2,
    3: 3,
    4: 4,
    5: 5,
    6: 6,
    7: 7,
    8: 8,
    9: 9,
    10: 10,
    11: 11,
    12: 12,
    13: 13
}

TETANUS = {
    1: 1,
    2: 2,
    3: 3,
    4: 4,
}

COVID = {
    1: 1,
    2: 2,
    3: 3
}

heart_copy_df['GeneralHealth'] = heart_copy_df['GeneralHealth'].map(GEN_HEALTH)
heart_copy_df['PhysicalHealthDays'] = heart_copy_df['PhysicalHealthDays'].replace(PHYS_MEN_HEALTH)
heart_copy_df['MentalHealthDays'] = heart_copy_df['MentalHealthDays'].replace(PHYS_MEN_HEALTH)
heart_copy_df['LastCheckupTime'] = heart_copy_df['LastCheckupTime'].map(LAST_CHECKUP)
heart_copy_df['PhysicalActivities'] = heart_copy_df['PhysicalActivities'].map(YES_NO_QUESTIONS)
heart_copy_df['SleepHours'] = heart_copy_df['SleepHours'].apply(SLEEP_TIME)
heart_copy_df['RemovedTeeth'] = heart_copy_df['RemovedTeeth'].map(TEETH_REMOVED)
heart_copy_df['HadHeartAttack'] = heart_copy_df['HadHeartAttack'].map(YES_NO_QUESTIONS)
heart_copy_df['HadAngina'] = heart_copy_df['HadAngina'].map(YES_NO_QUESTIONS)
heart_copy_df['HadStroke'] = heart_copy_df['HadStroke'].map(YES_NO_QUESTIONS)
heart_copy_df['HadAsthma'] = heart_copy_df['HadAsthma'].map(YES_NO_QUESTIONS)
heart_copy_df['HadSkinCancer'] = heart_copy_df['HadSkinCancer'].map(YES_NO_QUESTIONS)
heart_copy_df['HadCOPD'] = heart_copy_df['HadCOPD'].map(YES_NO_QUESTIONS)
heart_copy_df['HadDepressiveDisorder'] = heart_copy_df['HadDepressiveDisorder'].map(YES_NO_QUESTIONS)
heart_copy_df['HadKidneyDisease'] = heart_copy_df['HadKidneyDisease'].map(YES_NO_QUESTIONS)
heart_copy_df['HadArthritis'] = heart_copy_df['HadArthritis'].map(YES_NO_QUESTIONS)
heart_copy_df['HadDiabetes'] = heart_copy_df['HadDiabetes'].map(DIABETES)
heart_copy_df['DeafOrHardOfHearing'] = heart_copy_df['DeafOrHardOfHearing'].map(YES_NO_QUESTIONS)
heart_copy_df['BlindOrVisionDifficulty'] = heart_copy_df['BlindOrVisionDifficulty'].map(YES_NO_QUESTIONS)
heart_copy_df['DifficultyConcentrating'] = heart_copy_df['DifficultyConcentrating'].map(YES_NO_QUESTIONS)
heart_copy_df['DifficultyWalking'] = heart_copy_df['DifficultyWalking'].map(YES_NO_QUESTIONS)
heart_copy_df['DifficultyDressingBathing'] = heart_copy_df['DifficultyDressingBathing'].map(YES_NO_QUESTIONS)
heart_copy_df['DifficultyErrands'] = heart_copy_df['DifficultyErrands'].map(YES_NO_QUESTIONS)
heart_copy_df['SmokerStatus'] = heart_copy_df['SmokerStatus'].map(SMOKER_STATUS)
heart_copy_df['ECigaretteUsage'] = heart_copy_df['ECigaretteUsage'].map(ECIGARETTES)
heart_copy_df['ChestScan'] = heart_copy_df['ChestScan'].map(YES_NO_QUESTIONS)
heart_copy_df['RaceEthnicityCategory'] = heart_copy_df['RaceEthnicityCategory'].map(RACE)
heart_copy_df['AgeCategory'] = heart_copy_df['AgeCategory'].map(AGE_CATEGORY)
heart_copy_df['HeightInMeters'] = heart_copy_df['HeightInMeters'] / 100
heart_copy_df['WeightInKilograms'] = heart_copy_df['WeightInKilograms'] / 100
heart_copy_df['BMI'] = heart_copy_df['BMI'] / 100
heart_copy_df['AlcoholDrinkers'] = heart_copy_df['AlcoholDrinkers'].map(YES_NO_QUESTIONS)
heart_copy_df['HIVTesting'] = heart_copy_df['HIVTesting'].map(YES_NO_QUESTIONS)
heart_copy_df['FluVaxLast12'] = heart_copy_df['FluVaxLast12'].map(YES_NO_QUESTIONS)
heart_copy_df['PneumoVaxEver'] = heart_copy_df['PneumoVaxEver'].map(YES_NO_QUESTIONS)
heart_copy_df['TetanusLast10Tdap'] = heart_copy_df['TetanusLast10Tdap'].map(TETANUS)
heart_copy_df['HighRiskLastYear'] = heart_copy_df['HighRiskLastYear'].map(YES_NO_QUESTIONS)
heart_copy_df['CovidPos'] = heart_copy_df['CovidPos'].map(COVID)


def generate_random_date(date, checkup):
    start_date = pd.to_datetime('2012-01-01')
    end_date = pd.to_datetime('2012-01-01')
    if checkup == 1:
        start_date = date - pd.DateOffset(years=1)
        end_date = date
    elif checkup == 2:
        start_date = date - pd.DateOffset(years=2)
        end_date = date - pd.DateOffset(years=1)
    elif checkup == 3:
        start_date = date - pd.DateOffset(years=5)
        end_date = date - pd.DateOffset(years=2)
    else:
        start_date = pd.to_datetime('2012-01-01')
        end_date = date - pd.DateOffset(years=5)
    return start_date + (end_date - start_date) * np.random.rand()

heart_copy_df['DiagnoseDate'] = heart_copy_df.apply(lambda row: generate_random_date(row['SurveyDate'], row['LastCheckupTime']), axis=1)

heart_copy_df = heart_copy_df[['id', 'SurveyDate', 'DiagnoseDate', 'State', 'Sex', 'GeneralHealth',
       'PhysicalHealthDays', 'MentalHealthDays', 'LastCheckupTime',
       'PhysicalActivities', 'SleepHours', 'RemovedTeeth', 'HadHeartAttack',
       'HadAngina', 'HadStroke', 'HadAsthma', 'HadSkinCancer', 'HadCOPD',
       'HadDepressiveDisorder', 'HadKidneyDisease', 'HadArthritis',
       'HadDiabetes', 'DeafOrHardOfHearing', 'BlindOrVisionDifficulty',
       'DifficultyConcentrating', 'DifficultyWalking',
       'DifficultyDressingBathing', 'DifficultyErrands', 'SmokerStatus',
       'ECigaretteUsage', 'ChestScan', 'RaceEthnicityCategory', 'AgeCategory',
       'HeightInMeters', 'WeightInKilograms', 'BMI', 'AlcoholDrinkers',
       'HIVTesting', 'FluVaxLast12', 'PneumoVaxEver', 'TetanusLast10Tdap',
       'HighRiskLastYear', 'CovidPos']]

# ================================= Creating the dimesions tables ================================================

STATE = {
    1: "Alabama",
    2: "Alaska",
    4: "Arizona",
    5: "Arkansas",
    6: "California",
    8: "Colorado",
    9: "Connecticut",
    10: "Delaware",
    11: "District of Columbia",
    12: "Florida",
    13: "Georgia",
    15: "Hawaii",
    16: "Idaho",
    17: "Illinois",
    18: "Indiana",
    19: "Iowa",
    20: "Kansas",
    21: "Kentucky",
    22: "Louisiana",
    23: "Maine",
    24: "Maryland",
    25: "Massachusetts",
    26: "Michigan",
    27: "Minnesota",
    28: "Mississippi",
    29: "Missouri",
    30: "Montana",
    31: "Nebraska",
    32: "Nevada",
    33: "New Hampshire",
    34: "New Jersey",
    35: "New Mexico",
    36: "New York",
    37: "North Carolina",
    38: "North Dakota",
    39: "Ohio",
    40: "Oklahoma",
    41: "Oregon",
    42: "Pennsylvania",
    44: "Rhode Island",
    45: "South Carolina",
    46: "South Dakota",
    47: "Tennessee",
    48: "Texas",
    49: "Utah",
    50: "Vermont",
    51: "Virginia",
    53: "Washington",
    54: "West Virginia",
    55: "Wisconsin",
    56: "Wyoming",
    66: "Guam",
    72: "Puerto Rico",
    78: "Virgin Islands"
}
state_df = pd.DataFrame.from_dict(STATE, orient='index', columns=['state'])
state_df.reset_index(names='code', inplace=True)
# ----------------------------------------------------------------------------------------------------------------------------
GENDER = {1: 'Male', 2: 'Female'}
sex_df = pd.DataFrame.from_dict(GENDER, orient='index', columns=['gender'])
sex_df.reset_index(names='id', inplace=True)
# ----------------------------------------------------------------------------------------------------------------------------
GEN_HEALTH_TXT = {
    1: "Excellent",
    2: "Very good",
    3: "Good",
    4: "Fair",
    5: "Poor"
}
gen_health_df = pd.DataFrame.from_dict(GEN_HEALTH_TXT, orient='index', columns=['general_health'])
gen_health_df.reset_index(names='id', inplace=True)
# ----------------------------------------------------------------------------------------------------------------------------
LAST_CHECKUP_TXT = {
    1: "Within past year (anytime less than 12 months ago)",
    2: "Within past 2 years (1 year but less than 2 years ago)",
    3: "Within past 5 years (2 years but less than 5 years ago)",
    4: "5 or more years ago"
}
last_checkup_df = pd.DataFrame.from_dict(LAST_CHECKUP_TXT, orient='index', columns=['last_checkup_time'])
last_checkup_df.reset_index(names='id', inplace=True)
# ----------------------------------------------------------------------------------------------------------------------------
TEETH_REMOVED_TXT = {
    1: "1 to 5",
    2: "6 or more, but not all",
    3: "All",
    4: "None of them"
}
rem_teeth_df = pd.DataFrame.from_dict(TEETH_REMOVED_TXT, orient='index', columns=['removed_teeth'])
rem_teeth_df.reset_index(names='id', inplace=True)
# ----------------------------------------------------------------------------------------------------------------------------
DIABETES_TXT = {
    1: "Yes",
    2: "Yes, but only during pregnancy (female)",
    3: "No",
    4: "No, pre-diabetes or borderline diabetes",
}
diabetes_df = pd.DataFrame.from_dict(DIABETES_TXT, orient='index', columns=['diabetes_status'])
diabetes_df.reset_index(names='id', inplace=True)
# ----------------------------------------------------------------------------------------------------------------------------
SMOKING_STATUS_TXT = {
    1: "Current smoker - now smokes every day",
    2: "Current smoker - now smokes some days",
    3: "Former smoker",
    4: "Never smoked"
}
smoking_status_df = pd.DataFrame.from_dict(SMOKING_STATUS_TXT, orient='index', columns=['smoking_status'])
smoking_status_df.reset_index(names='id', inplace=True)
# ----------------------------------------------------------------------------------------------------------------------------
ECIGARETTES_TXT = {
    1: "Never used e-cigarettes in my entire life",
    2: "Use them every day",
    3: "Use them some days",
    4: "Not at all (right now)"
}
e_cigarette_usage_df = pd.DataFrame.from_dict(ECIGARETTES_TXT, orient='index', columns=['e_cigarette_usage'])
e_cigarette_usage_df.reset_index(names='id', inplace=True)
# ----------------------------------------------------------------------------------------------------------------------------
RACE_TXT = {
    1: "White only, Non-Hispanic",
    2: "Black only, Non-Hispanic",
    3: "Other race only, Non-Hispanic",
    4: "Multiracial, Non-Hispanic",
    5: "Hispanic"
}
race_ethnicity_category_df = pd.DataFrame.from_dict(RACE_TXT, orient='index', columns=['race_ethnicity_category'])
race_ethnicity_category_df.reset_index(names='id', inplace=True)
# ----------------------------------------------------------------------------------------------------------------------------
AGE_CATEGORY_TXT = {
    1: "Age 18 to 24",
    2: "Age 25 to 29",
    3: "Age 30 to 34",
    4: "Age 35 to 39",
    5: "Age 40 to 44",
    6: "Age 45 to 49",
    7: "Age 50 to 54",
    8: "Age 55 to 59",
    9: "Age 60 to 64",
    10: "Age 65 to 69",
    11: "Age 70 to 74",
    12: "Age 75 to 79",
    13: "Age 80 or older"
}
age_category_df = pd.DataFrame.from_dict(AGE_CATEGORY_TXT, orient='index', columns=['age_category'])
age_category_df.reset_index(names='id', inplace=True)
# ----------------------------------------------------------------------------------------------------------------------------
TETANUS_TXT = {
    1: "Yes, received Tdap",
    2: "Yes, received tetanus shot, but not Tdap",
    3: "Yes, received tetanus shot but not sure what type",
    4: "No, did not receive any tetanus shot in the past 10 years",
}
tetanus_last_10_tdap_df = pd.DataFrame.from_dict(TETANUS_TXT, orient='index', columns=['tetanus_last_10_tdap'])
tetanus_last_10_tdap_df.reset_index(names='id', inplace=True)
# ----------------------------------------------------------------------------------------------------------------------------
COVID_TXT = {
    1: "Yes",
    2: "No",
    3: "Tested positive using home test without a health professional"
}
covid_pos_df = pd.DataFrame.from_dict(COVID_TXT, orient='index', columns=['covid_pos'])
covid_pos_df.reset_index(names='id', inplace=True)
# ----------------------------------------------------------------------------------------------------------------------------

heart_copy_df.rename(columns={'SurveyDate':'survey_date',
                             'DiagnoseDate':'diagnose_date', 'State':'state_code',
                             'Sex':'gender_id', 'GeneralHealth':'general_health_id',
                             'PhysicalHealthDays':'physical_health_days', 'MentalHealthDays':'mental_health_days',
                             'LastCheckupTime':'last_checkup_time_id', 'PhysicalActivities':'physical_activities',
                             'SleepHours':'sleep_hours', 'RemovedTeeth':'removed_teeth_id',
                             'HadHeartAttack':'had_heart_attack', 'HadAngina':'had_angina',
                             'HadStroke':'had_stroke', 'HadAsthma':'had_asthma',
                             'HadSkinCancer':'had_skin_cancer', 'HadCOPD':'had_copd',
                             'HadDepressiveDisorder':'had_depressive_disorder', 'HadKidneyDisease':'had_kidney_disease',
                             'HadArthritis':'had_arthritis', 'HadDiabetes':'diabetes_status_id',
                             'DeafOrHardOfHearing':'deaf_or_hard_of_hearing', 'BlindOrVisionDifficulty':'blind_or_vision_difficulity',
                             'DifficultyConcentrating':'difficulty_concentrating', 'DifficultyWalking':'difficulty_walking',
                             'DifficultyDressingBathing':'difficulty_dressing_bathing', 'DifficultyErrands':'difficulty_errands',
                             'SmokerStatus':'smoking_status_id', 'ECigaretteUsage':'e_cigarette_usage_id',
                             'ChestScan':'chest_scan', 'RaceEthnicityCategory':'race_ethnicity_category_id',
                             'AgeCategory':'age_category_id', 'HeightInMeters':'height_in_meters',
                             'WeightInKilograms':'weight_in_kilograms', 'BMI':'bmi',
                             'AlcoholDrinkers':'alcohol_drinkers', 'HIVTesting':'hiv_testing',
                             'FluVaxLast12':'flu_vax_last_12', 'PneumoVaxEver':'pneumo_vax_ever',
                             'TetanusLast10Tdap':'tetanus_last_10_tdap_id', 'HighRiskLastYear':'high_risk_last_year',
                             'CovidPos':'covid_pos_id'
                             }, inplace=True)

# ============================== Loading the data to MySQL =========================================================
from validations import validate_transform_tables_rows

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
    sex_df.to_sql("d_gender", con = engine_load, if_exists= 'append', index= False)
    val = validate_transform_tables_rows(sex_df, "d_gender")
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
    heart_copy_df.to_sql("f_data", con = engine_load, if_exists= 'append', index= False)
    val = validate_transform_tables_rows(heart_copy_df, "f_data")
    if val:
        log_entry["log_message"] = "The loaded Heart Data to the DWH is Valid"
    else:
        log_entry["log_message"] = "The loaded Heart Data to the DWH is Invalid"
    log_to_db(log_entry)
except Exception as e:
    log_entry["log_message"] = f"An error occurred: {traceback.format_exc()}"
    log_to_db(log_entry)

# ============================== Versioning the Data with DVC =========================================================

filepath = Path('ETL/dvc_data/data/heart_data.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
heart_copy_df.to_csv(filepath, index=False)

filepath = Path('ETL/dvc_data/data/state.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
state_df.to_csv(filepath, index=False)

filepath = Path('ETL/dvc_data/data/gender.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
sex_df.to_csv(filepath, index=False)

filepath = Path('ETL/dvc_data/data/general_health.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
gen_health_df.to_csv(filepath, index=False)

filepath = Path('ETL/dvc_data/data/removed_teeth.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
rem_teeth_df.to_csv(filepath, index=False)

filepath = Path('ETL/dvc_data/data/diabetes_status.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
diabetes_df.to_csv(filepath, index=False)

filepath = Path('ETL/dvc_data/data/smoking_status.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
smoking_status_df.to_csv(filepath, index=False)

filepath = Path('ETL/dvc_data/data/e_cigarette_usage.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
e_cigarette_usage_df.to_csv(filepath, index=False)

filepath = Path('ETL/dvc_data/data/race_ethnicity_category.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
race_ethnicity_category_df.to_csv(filepath, index=False)

filepath = Path('ETL/dvc_data/data/age_category.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
age_category_df.to_csv(filepath, index=False)

filepath = Path('ETL/dvc_data/data/tetanus_last_10_tdap.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
tetanus_last_10_tdap_df.to_csv(filepath, index=False)

filepath = Path('ETL/dvc_data/data/covid_pos.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
covid_pos_df.to_csv(filepath, index=False)

filepath = Path('ETL/dvc_data/data/last_checkup.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
last_checkup_df.to_csv(filepath, index=False)

version = os.getenv('VERSION')
log_entry = {
    "script_name": "transform.py",
    "source_db": "heart_staging_1",
    "destination_db": "DVC Storage",
    "name_table": "None",
    "log_message": ""
}
try:
    subprocess.run(["powershell.exe", "-ExecutionPolicy", "Unrestricted", "-File", versioning_script], check=True)
    log_entry["log_message"] = f'Successfully versioned data with DVC. v{version}'
    log_to_db(log_entry)
except Exception as e:
    log_entry["log_message"] = f"An error occurred: {traceback.format_exc()}"
    log_to_db(log_entry)