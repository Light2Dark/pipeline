import pandas as pd

if __name__ == "__main__":
    df = pd.DataFrame({'Yes': [50, 21], 'No': [131, 2]}, index=["a", "b"])
    print(df)
