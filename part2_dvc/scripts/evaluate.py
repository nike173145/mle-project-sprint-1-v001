# scripts/evaluate.py

import os
import json
import yaml
import joblib
import pandas as pd

from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


def evaluate_model():
    # читаем параметры
    with open("params.yaml", "r", encoding="utf-8") as f:
        params = yaml.safe_load(f)

    target_col = params["target_col"]

    # загружаем данные
    data = pd.read_csv("data/initial_data.csv")

    X = data.drop(columns=[target_col])
    y = data[target_col]

    # загружаем уже обученную модель
    model = joblib.load("models/fitted_model.pkl")

    # предсказания
    y_pred = model.predict(X)

    # метрики
    rmse = mean_squared_error(y, y_pred) ** 0.5
    mae = mean_absolute_error(y, y_pred)
    r2 = r2_score(y, y_pred)

    eval_res = {
        "rmse": round(rmse, 3),
        "mae": round(mae, 3),
        "r2": round(r2, 3)
    }

    # сохраняем результат
    os.makedirs("cv_results", exist_ok=True)

    with open("cv_results/cv_res.json", "w", encoding="utf-8") as f:
        json.dump(eval_res, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    evaluate_model()