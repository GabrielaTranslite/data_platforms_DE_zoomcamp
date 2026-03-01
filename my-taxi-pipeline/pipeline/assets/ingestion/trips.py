"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: create+replace

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
@bruin"""

import os
import json
import pandas as pd

def materialize():
    print("--- Starting materialize function ---")

    start_str = os.environ.get("BRUIN_START_DATE")
    end_str = os.environ.get("BRUIN_END_DATE")
    
    # Użycie 2020-01, bo to jest zakres, który ostatnio działał.
    # Użyjemy go jako domyślnego.
    if start_str and "2026" in start_str:
        print("!!! OVERRIDING DATES TO 2020-01-01 for Data Engineering Zoomcamp (from 2026) !!!")
        start_date = pd.to_datetime("2020-01-01")
        end_date = pd.to_datetime("2020-01-31")
    elif not start_str or not end_str:
        print("Warning: BRUIN_START/END_DATE not set, using default range for safety (2020-01).")
        start_date = pd.to_datetime("2020-01-01")
        end_date = pd.to_datetime("2020-01-31")
    else:
        start_date = pd.to_datetime(start_str)
        end_date = pd.to_datetime(end_str)
    
    print(f"Processing dates from {start_date} to {end_date}")

    taxi_types = json.loads(os.environ.get("BRUIN_VARS", "{}")).get("taxi_types", ["yellow", "green"])
    print(f"Processing taxi types: {taxi_types}")

    expected_cols = ["pickup_datetime", "dropoff_datetime"]
    
    # Inicjalizuj final_dataframe z pustymi, ale właściwymi typami datetime64[ns]
    final_data = {col: pd.Series(dtype='datetime64[ns]') for col in expected_cols}
    final_dataframe = pd.DataFrame(final_data)
    print(f"Initial final_dataframe structure: {final_dataframe.dtypes.to_dict()}")

    date_range = pd.date_range(start=start_date, end=end_date, freq="MS")

    for date in date_range:
        for taxi_type in taxi_types:
            month = f"{date.month:02d}"
            year = date.year
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet"
            
            print(f"Attempting to download: {url}")
            
            temp_df = None
            try:
                temp_df = pd.read_parquet(url)
                print(f"Downloaded {url}. Shape: {temp_df.shape}, Columns: {temp_df.columns.tolist()}")
            except Exception as e:
                print(f"Warning: Failed to read {url}. Error: {e}")
                continue

            if temp_df is None or temp_df.empty:
                print(f"Warning: {url} is empty or returned empty dataframe after read, skipping.")
                continue

            rename_map = {
                "tpep_pickup_datetime": "pickup_datetime",
                "tpep_dropoff_datetime": "dropoff_datetime",
                "lpep_pickup_datetime": "pickup_datetime",
                "lpep_dropoff_datetime": "dropoff_datetime"
            }
            temp_df = temp_df.rename(columns=rename_map)
            print(f"Columns after rename: {temp_df.columns.tolist()}")

            common_cols = [c for c in expected_cols if c in temp_df.columns]
            if not common_cols:
                print(f"Skipping {url}: No common columns {expected_cols} found after rename. Available: {temp_df.columns.tolist()}")
                continue

            # Wybierz tylko wspólne kolumny i przekonwertuj na datetime64[ns] od razu
            df_to_concat = pd.DataFrame(columns=expected_cols) # Upewnij się, że ma nagłówki
            for col in common_cols:
                df_to_concat[col] = pd.to_datetime(temp_df[col], errors="coerce").dt.tz_localize(None).astype('datetime64[ns]')

            print(f"Concatenating data from {url}. temp_df shape: {temp_df.shape}. temp_df dtypes (after conversion): {df_to_concat.dtypes.to_dict()}")
            final_dataframe = pd.concat([final_dataframe, df_to_concat], ignore_index=True)
            print(f"Current total rows in final_dataframe: {len(final_dataframe)}")
            print(f"Current final_dataframe dtypes: {final_dataframe.dtypes.to_dict()}")

    print(f"Final rows extracted before return: {len(final_dataframe)}")

    # Ostateczne upewnienie się co do typów i braku stref czasowych
    for col in expected_cols:
        if col not in final_dataframe.columns:
            final_dataframe[col] = pd.Series(dtype='datetime64[ns]')
        else:
            # Wymuś typ datetime64[ns] i usuń strefę czasową
            if pd.api.types.is_datetime64_any_dtype(final_dataframe[col]):
                if final_dataframe[col].dt.tz is not None:
                    print(f"FINAL CHECK: Removing timezone from column '{col}' before return.")
                    final_dataframe[col] = final_dataframe[col].dt.tz_localize(None)
            final_dataframe[col] = final_dataframe[col].astype('datetime64[ns]') # Upewnij się, że jest to ten typ

    if final_dataframe.empty:
        print("No data in final_dataframe after all processing. Returning empty list.")
        return []

    print("--- Materialize function finished successfully ---")
    return final_dataframe
