from scipy.stats import chi2_contingency
import pandas as pd
import dvc.api, dvc.repo
import io
# from sklearn.model_selection import cross_val_score

# =======================================================================================================================
# ================================= Pre-processing ======================================================================
# =======================================================================================================================
def preprocessing():
    url = "https://github.com/mostafa-fallaha/heart-disease-prediction.git"
    data = dvc.api.read("ETL/dvc_data/data/heart_data.csv", repo=url)
    df = pd.read_csv(io.StringIO(data))

    df.rename(columns={'state_code':'state', 'gender_id': 'gender', 'general_health_id':'general_health',
                                    'last_checkup_time_id':'last_checkup_time', 'removed_teeth_id':'removed_teeth',
                                    'diabetes_status_id':'diabetes_status', 'smoking_status_id':'smoking_status',
                                    'e_cigarette_usage_id':'e_cigarette_usage', 'race_ethnicity_category_id':'race_ethnicity_category',
                                    'age_category_id':'age_category', 'tetanus_last_10_tdap_id':'tetanus_last_10_tdap',
                                    'covid_pos_id':'covid_pos'}, inplace=True)

    df = df.dropna().reset_index(drop=True)

    df['state'] = df['state'].astype(int)
    df['gender'] = df['gender'].astype(int)
    df['general_health'] = df['general_health'].astype(int)
    df['last_checkup_time'] = df['last_checkup_time'].astype(int)
    df['removed_teeth'] = df['removed_teeth'].astype(int)
    df['diabetes_status'] = df['diabetes_status'].astype(int)
    df['smoking_status'] = df['smoking_status'].astype(int)
    df['e_cigarette_usage'] = df['e_cigarette_usage'].astype(int)
    df['race_ethnicity_category'] = df['race_ethnicity_category'].astype(int)
    df['age_category'] = df['age_category'].astype(int)
    df['tetanus_last_10_tdap'] = df['tetanus_last_10_tdap'].astype(int)
    df['covid_pos'] = df['covid_pos'].astype(int)
    df['mental_health_days'] = df['mental_health_days'].astype(int)
    df['physical_activities'] = df['physical_activities'].astype(int)
    df['had_heart_attack'] = df['had_heart_attack'].astype(int)
    df['had_angina'] = df['had_angina'].astype(int)
    df['had_stroke'] = df['had_stroke'].astype(int)
    df['had_asthma'] = df['had_asthma'].astype(int)
    df['had_skin_cancer'] = df['had_skin_cancer'].astype(int)
    df['had_copd'] = df['had_copd'].astype(int)
    df['had_depressive_disorder'] = df['had_depressive_disorder'].astype(int)
    df['had_kidney_disease'] = df['had_kidney_disease'].astype(int)
    df['had_arthritis'] = df['had_arthritis'].astype(int)
    df['deaf_or_hard_of_hearing'] = df['deaf_or_hard_of_hearing'].astype(int)
    df['blind_or_vision_difficulity'] = df['blind_or_vision_difficulity'].astype(int)
    df['difficulty_concentrating'] = df['difficulty_concentrating'].astype(int)
    df['difficulty_walking'] = df['difficulty_walking'].astype(int)
    df['difficulty_dressing_bathing'] = df['difficulty_dressing_bathing'].astype(int)
    df['difficulty_errands'] = df['difficulty_errands'].astype(int)
    df['chest_scan'] = df['chest_scan'].astype(int)
    df['alcohol_drinkers'] = df['alcohol_drinkers'].astype(int)
    df['hiv_testing'] = df['hiv_testing'].astype(int)
    df['flu_vax_last_12'] = df['flu_vax_last_12'].astype(int)
    df['pneumo_vax_ever'] = df['pneumo_vax_ever'].astype(int)
    df['high_risk_last_year'] = df['high_risk_last_year'].astype(int)

    df = pd.DataFrame(df.drop(columns={'id', 'state', 'survey_date', 'diagnose_date'}, axis=1))

    return df

# =======================================================================================================================
# ================================= feature selection ===================================================================
# =======================================================================================================================
def feature_selection():
    df = preprocessing()

    chi_dict = {}
    for col in df.columns:
        contingency_table = pd.crosstab(df[col], df['had_heart_attack'])
        chi2, p, dof, ex = chi2_contingency(contingency_table)
        chi_dict[col] = chi2

    chi_df = pd.DataFrame.from_dict(chi_dict, orient='index', columns=['value'])


    # ---- Commented this cz it's taking a lot of time to run ---------
    # ---- So, i ran it once to get the best threshold ----------------
    # model = RandomForestClassifier()
    # thresholds = [1000, 1500, 2000, 5000]
    # best_score = 0
    # best_threshold = 0

    # for threshold in thresholds:
    #     selected_features = chi_df[chi_df['value'] > threshold].index.to_list()
    #     df_model = df[selected_features]

    #     X = df_model.drop(columns=['had_heart_attack'])
    #     y = df_model['had_heart_attack']
        
    #     scores = cross_val_score(model, X, y, cv=5)
    #     avg_score = scores.mean()
        
    #     if avg_score > best_score:
    #         best_score = avg_score
    #         best_threshold = threshold

    best_threshold = 1000
    chi_df = chi_df.loc[chi_df['value'] >= best_threshold]
    chi_df = chi_df.loc[(chi_df.index != 'weight_in_kilograms') & (chi_df.index != 'last_checkup_time')]

    features_list = chi_df.index.to_list()

    df_model = df[features_list]

    return df_model