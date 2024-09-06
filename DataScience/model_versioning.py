import mlflow
import mlflow.sklearn
from sklearn.metrics import precision_score, recall_score, f1_score, accuracy_score, classification_report
from datetime import datetime
# import joblib
# import warnings
# warnings.filterwarnings('ignore')

from model_training import train_model

rf_clf, X_test, y_test = train_model()

y_pred_proba = rf_clf.predict_proba(X_test)[:, 1]
threshold = 0.3
y_pred_custom = (y_pred_proba >= threshold).astype(int)
cl_report = classification_report(y_test, y_pred_custom)
print(cl_report)

accuracy = accuracy_score(y_test, y_pred_custom)

recall_0 = recall_score(y_test, y_pred_custom, pos_label=0).round(3)
recall_1 = recall_score(y_test, y_pred_custom, pos_label=1).round(3)

precision_0 = precision_score(y_test, y_pred_custom, pos_label=0).round(3)
precision_1 = precision_score(y_test, y_pred_custom, pos_label=1).round(3)

f1 = f1_score(y_test, y_pred_custom).round(3)

# print(f'recall_0: {recall_0} | recll_1: {recall_1}')
# print(f'precision_0: {precision_0} | precision_1: {precision_1}')
# print(f'f1: {f1}')

runname = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

# Start MLflow run
mlflow.set_experiment('heart_disease_model')
with mlflow.start_run(run_name=runname) as mlflow_run:
    run_id = mlflow_run.info.run_id

    mlflow.log_param("threshold", 0.3)

    # Log metrics
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("recall_0", recall_0)
    mlflow.log_metric("recall_1", recall_1)
    mlflow.log_metric("precision_0", precision_0)
    mlflow.log_metric("precision_1", precision_1)
    mlflow.log_metric("f1_score", f1)

    # Log the model
    mlflow.sklearn.log_model(rf_clf, "rf_clf_model")

    # Register the model
    model_uri = f"runs:/{run_id}/model"
    mlflow.register_model(model_uri=model_uri, name="rf_clf_model")