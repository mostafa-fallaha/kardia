# Kardía: Heart Attack Risk Predictor

## Overview

This project is a machine learning-based web application designed to analyze factors contributing to heart attacks and predict the likelihood of someone experiencing one. The model, built using a Random Forest Classifier, achieves an impressive accuracy of 95%, offering a quick and insightful assessment of heart attack risks based on user-provided data.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)

## Features

- **ETL**: An ETL pipeline that extract the data from the parquet file, clean it, transform it and then load it into the data warehouse.
- **Data versioning with DVC**: At the end of the ETL process, the data is versioned via DVC and stored locally. With a _pull by git tag (version)_ functionality, that pulls the data of the specified version, and then reload the DWH automatically with that version of data.
- **Machine learning model versioning with MLFLOW**: versioning of the Random Forest Classifier model with MLFLOW.
- **Streamlit app**:

1. A streamlit app that loads the latest version of the machine learning model from MLFLOW.
2. Users can input their personal health data directly into the app.
3. Get a quick estimate of the heart attack risk in seconds.

## Installation

### ETL

1. Download the parquet file:

```bash
https://drive.google.com/uc?export=download&id=1rXp1FxHpeMIqU9JV8NmVnJQ8X4fQnYtQ
```

2. put it in `ETL/docs`.

3. create a `.env` file in the root of the project containing the following:<br>

```bash
DB_USER=your database username
DB_PASSWORD=your database password
DB_HOST=your host (usually localhost)
DB_PORT=the port where mysql is running (usually 3306)
DB_STAGING=the staging schema name (create the schema in mysql workbench, no need to create any table)
DB_DWH=the DWH schema name (you need to create tables, in the step 5)
VERSION=0.9 (this to increment the version whenever you run the ETL process)
```

4. run the `extract.ipynb` to load the staging schema.

5. in mysql workbench, create a new schema (the DWH schema). and then run the `final_dwh.sql` (you can find it in ETL/dwh) in the newly created schema to create the tables and the relations.

6. run the `transform.ipynb` to transform the data and load it to the DWH.

### Streamlit app

```bash
git clone https://github.com/mostafa-fallaha/heart-disease-prediction.git
cd heart-disease-prediction
pip instal -r requirements.txt
```

1. run the _model_versioning.py_: cd to the DataScience directory

```bash
cd DataScience
python3 model_versioning.py
```

2. run the mlflow ui: cd to the root directory

```bash
cd ..
mlflow ui
```

3. run the streamlit app: open a new terminal in the project directory.

```bash
cd DataScience
streamlit run app.py
```

the app now will open in a new tab in your default browser.
