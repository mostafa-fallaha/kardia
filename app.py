# import mlflow.sklearn
import pickle
import numpy as np
import streamlit as st
import os
import gdown

st.set_page_config(
        page_title="Heart Attack Prediction",
        page_icon="images/heart_real.png"
    )

url = 'https://drive.google.com/uc?id=1ayPTkM36XIXk0GyA986Wy1rT_XUJE3eJ'

output = '/tmp/model.pkl'

if os.path.exists(output):
    st.write("Model already exists, skipping download.")
else:
    st.write("Downloading model from Google Drive...")
    gdown.download(url, output, quiet=False)
    st.write("Model downloaded successfully")

with open(output, 'rb') as f:
    rf_clf = pickle.load(f)

# --------------------------------------------------------------------
GENDER_TXT = {
    1: "Male",
    2: "Female"
}
GENDER_NUM = {v: k for k, v in GENDER_TXT.items()}

# --------------------------------------------------------------------
GEN_HEALTH_TXT = {
    1: "Excellent",
    2: "Very good",
    3: "Good",
    4: "Fair",
    5: "Poor"
}
GEN_HEALTH_NUM = {v: k for k, v in GEN_HEALTH_TXT.items()}

# --------------------------------------------------------------------
TEETH_REMOVED_TXT = {
    1: "1 to 5",
    2: "6 or more, but not all",
    3: "All",
    4: "None of them"
}
TEETH_REMOVED_NUM = {v: k for k, v in TEETH_REMOVED_TXT.items()}

# --------------------------------------------------------------------
YES_NO_TXT = {
    0: "No",
    1: "Yes"
}
YES_NO_NUM = {v: k for k, v in YES_NO_TXT.items()}

# --------------------------------------------------------------------
DIABETES_TXT = {
    1: "Yes",
    2: "Yes, but only during pregnancy (female)",
    3: "No",
    4: "No, pre-diabetes or borderline diabetes",
}
DIABETES_NUM = {v: k for k, v in DIABETES_TXT.items()}

# --------------------------------------------------------------------
SMOKING_STATUS_TXT = {
    1: "Current smoker - now smokes every day",
    2: "Current smoker - now smokes some days",
    3: "Former smoker",
    4: "Never smoked"
}
SMOKING_STATUS_NUM = {v: k for k, v in SMOKING_STATUS_TXT.items()}

# --------------------------------------------------------------------
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
AGE_CATEGORY_NUM = {v: k for k, v in AGE_CATEGORY_TXT.items()}


# ================================= Main Body =============================================
st.title("Heart Attack Prediction")
st.subheader("🎯 Take Control of Your Health with Advanced AI Predictions!")

col1, col2 = st.columns([1, 3])

with col1:
    st.image("images/doctor_1.png",
                caption="I'll help you diagnose your heart health! - Dr. RandomForestClassifier ",
                width=150)
    submit = st.button("Predict")

with col2:
    st.markdown("""
    Did you know that machine learning models can predict your risk of heart disease with high accuracy?
    In this app, you can estimate your chance of heart disease in just seconds!

    This application uses a state-of-the-art RandomForestClassifier model,
    which was trained on survey data from over 200k US residents from the year 2022.
    After extensive testing and tuning, this model achieved an impressive accuracy of around 95%,
    making it a highly reliable tool for assessing your heart disease risk.
                
    #### To predict your heart disease status, simply follow these steps:
    1. Enter the parameters that best describe you: Fill in your health and lifestyle details in the left sidebar,
    selecting options that accurately represent your current status.

    2. Press the "Predict" button: Once all the fields are completed, click on the button and let the model process your information.

    3. Wait for the result: In a matter of seconds, you'll receive a prediction on your heart disease risk.
    """)

# ================================= Sidebar =============================================
st.sidebar.title("Feature Selection")
st.sidebar.image("images/magnifier.png", width=100)

# Input fields
gender = st.sidebar.selectbox("Gender", list(GENDER_TXT.values()))
general_health = st.sidebar.selectbox("Would you say that in general your health is:", list(GEN_HEALTH_TXT.values()))
physical_health_days = st.sidebar.number_input("Now thinking about your physical health, "
                                               "which includes physical illness and injury, "
                                               "for how many days during the past 30 days "
                                               "was your physical health not good?", min_value=0, max_value=30, step=1)
physical_activities = st.sidebar.selectbox("During the past month, other than your regular job, "
                                           "did you participate in any physical activities or exercises such as "
                                           "running, calisthenics, golf, gardening, or walking for exercise?", list(YES_NO_TXT.values()))
sleep_hours = st.sidebar.number_input("On average, how many hours of sleep do you get in a 24-hour period?", min_value=0, max_value=24, step=1)
removed_teeth = st.sidebar.selectbox("Not including teeth lost for injury or orthodontics, "
                                     "how many of your permanent teeth have been removed "
                                     "because of tooth decay or gum disease?", list(TEETH_REMOVED_TXT.values()))
had_angina = st.sidebar.selectbox("Ever told you had Angina? (ذبحة)", list(YES_NO_TXT.values()))
had_stroke = st.sidebar.selectbox("Ever told you had a Stroke? (سكتة دماغية)", list(YES_NO_TXT.values()))
had_copd = st.sidebar.selectbox("Ever told you had COPD? (انسداد رئوي مزمن)", list(YES_NO_TXT.values()))
had_kidney_disease = st.sidebar.selectbox("Ever told you had Kidney Disease?", list(YES_NO_TXT.values()))
had_arthritis = st.sidebar.selectbox("Ever told you had Arthritis? (التهاب المفاصل)", list(YES_NO_TXT.values()))
diabetes_status = st.sidebar.selectbox("Have you ever had diabetes?", list(DIABETES_TXT.values()))
deaf_or_hard_of_hearing = st.sidebar.selectbox("Are you deaf or do you have serious difficulty hearing?", list(YES_NO_TXT.values()))
blind_or_vision_difficulity = st.sidebar.selectbox("Are you blind or do you have serious difficulty seeing, "
                                                   "even when wearing glasses?", list(YES_NO_TXT.values()))
difficulty_walking = st.sidebar.selectbox("Do you have serious difficulty walking or climbing stairs?", list(YES_NO_TXT.values()))
difficulty_dressing_bathing = st.sidebar.selectbox("Do you have difficulty dressing or bathing?", list(YES_NO_TXT.values()))
difficulty_errands = st.sidebar.selectbox("Because of a physical, mental, or emotional condition, "
                                          "do you have difficulty doing errands alone "
                                          "such as visiting a doctor's office or shopping?", list(YES_NO_TXT.values()))
smoking_status = st.sidebar.selectbox("Define your Smoking Status", list(SMOKING_STATUS_TXT.values()))
chest_scan = st.sidebar.selectbox("Have you ever had a CT or CAT scan of your chest area?", list(YES_NO_TXT.values()))
age_category = st.sidebar.selectbox("Your Age Category", list(AGE_CATEGORY_TXT.values()))
height = st.sidebar.number_input("Height (in meters)", min_value=0.1, max_value=2.5, step=0.1)
weight = st.sidebar.number_input("Weight (in Kg)", min_value=1.0, max_value=200.0, step=1.0)
alcohol_drinkers = st.sidebar.selectbox("Did you have at least one drink of alcohol in the past 30 days?", list(YES_NO_TXT.values()))
pneumo_vax_ever = st.sidebar.selectbox("Have you ever had a pneumonia shot also known as a pneumococcal vaccine? "
                                       "(لقاح المكورات الرئوية)", list(YES_NO_TXT.values()))

bmi = weight / (height*height)

# # ================================= Prediction =============================================
if submit:
    input_data = {
        'gender': np.int32(GENDER_NUM[gender]),
        'general_health': np.int32(GEN_HEALTH_NUM[general_health]),
        'physical_health_days': np.float64(physical_health_days),
        'physical_activities': np.int32(YES_NO_NUM[physical_activities]),
        'sleep_hours': np.float64(sleep_hours),
        'removed_teeth': np.int32(TEETH_REMOVED_NUM[removed_teeth]),
        'had_angina': np.int32(YES_NO_NUM[had_angina]),
        'had_stroke': np.int32(YES_NO_NUM[had_stroke]),
        'had_copd': np.int32(YES_NO_NUM[had_copd]),
        'had_kidney_disease': np.int32(YES_NO_NUM[had_kidney_disease]),
        'had_arthritis': np.int32(YES_NO_NUM[had_arthritis]),
        'diabetes_status': np.int32(DIABETES_NUM[diabetes_status]),
        'deaf_or_hard_of_hearing': np.int32(YES_NO_NUM[deaf_or_hard_of_hearing]),
        'blind_or_vision_difficulity': np.int32(YES_NO_NUM[blind_or_vision_difficulity]),
        'difficulty_walking': np.int32(YES_NO_NUM[difficulty_walking]),
        'difficulty_dressing_bathing': np.int32(YES_NO_NUM[difficulty_dressing_bathing]),
        'difficulty_errands': np.int32(YES_NO_NUM[difficulty_errands]),
        'smoking_status': np.int32(SMOKING_STATUS_NUM[smoking_status]),
        'chest_scan': np.int32(YES_NO_NUM[chest_scan]),
        'age_category': np.int32(AGE_CATEGORY_NUM[age_category]),
        'bmi': np.float64(bmi),
        'alcohol_drinkers': np.int32(YES_NO_NUM[alcohol_drinkers]),
        'pneumo_vax_ever': np.int32(YES_NO_NUM[pneumo_vax_ever])
    }

    input_list = list(input_data.values())
    print(input_list)

    prediction = (rf_clf.predict_proba([input_list])[0, 1])*100
    
    st.success(f"The predicted probability of having a heart attack is: {prediction:.0f}%")