import pandas as pd

def remove_duplicates(data):
    feature_cols = data.columns.drop('flat_id').tolist()
    is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
    data = data[~is_duplicated_features].reset_index(drop=True)
    return data

def remove_outliers_iqr(data: pd.DataFrame, threshold: float = 1.5, do_not_touch_cols: list[str] = []) -> pd.DataFrame:
    num_cols = data.drop(columns=do_not_touch_cols).select_dtypes(include=["float", "int"]).columns
    mask = pd.Series(True, index=data.index)

    for col in num_cols:
        Q1 = data[col].quantile(0.25)
        Q3 = data[col].quantile(0.75)
        IQR = Q3 - Q1

        margin = threshold * IQR
        lower = Q1 - margin
        upper = Q3 + margin

        mask &= data[col].between(lower, upper)

    return data[mask]