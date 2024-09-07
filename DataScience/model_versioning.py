import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature
from sklearn.metrics import precision_score, recall_score, f1_score, accuracy_score
from datetime import datetime
# import warnings
# warnings.filterwarnings('ignore')

from model_training import train_model

runname = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

mlflow.set_experiment('heart_disease_model')
with mlflow.start_run(run_name=runname) as mlflow_run:
    run_id = mlflow_run.info.run_id

    # =========== Model metrics ======================
    rf_clf, X_test, y_test = train_model()

    y_pred_proba = rf_clf.predict_proba(X_test)[:, 1]
    threshold = 0.3
    y_pred_custom = (y_pred_proba >= threshold).astype(int)

    accuracy = round(accuracy_score(y_test, y_pred_custom), 3)

    recall_0 = recall_score(y_test, y_pred_custom, pos_label=0).round(3)
    recall_1 = recall_score(y_test, y_pred_custom, pos_label=1).round(3)

    precision_0 = precision_score(y_test, y_pred_custom, pos_label=0).round(3)
    precision_1 = precision_score(y_test, y_pred_custom, pos_label=1).round(3)

    f1 = f1_score(y_test, y_pred_custom).round(3)

    # =========== Model versioning ======================
    signature = infer_signature(X_test, y_test)

    # Log params
    mlflow.log_params({
        "prediction_threshold": 0.3,
        "n_estimators": 100,
        "random_state": 42
    })

    # Log metrics
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("recall_0", recall_0)
    mlflow.log_metric("recall_1", recall_1)
    mlflow.log_metric("precision_0", precision_0)
    mlflow.log_metric("precision_1", precision_1)
    mlflow.log_metric("f1_score", f1)

    # Log and Register the model
    mlflow.sklearn.log_model(
        sk_model=rf_clf,
        artifact_path="rf_clf_model",
        signature=signature,
        registered_model_name="rf_clf_registered_model"
    )