## Step 3: Integration Pipeline

import pandas as pd
import numpy as np
import os
import glob
import re

SCHEMA = [
    'YEAR', 'IMO', 'NAME', 'TYPE', 'GT', 'LDT', 
    'BUILT', 'AGE', 'LAST_FLAG', 'PLACE', 'COUNTRY'
]

#mappings for each column
COLUMN_MAPPING = {
    'IMO#': 'IMO', 'IMO number': 'IMO', 'IMO  number': 'IMO',
    'NAME': 'NAME', 'Name of ship ': 'NAME', 'VESSEL': 'NAME',
    'TYPE': 'TYPE', 'Type of ship': 'TYPE',
    'GT': 'GT', 'GT / LDT': 'GT_LDT_MIXED',
    'LDT': 'LDT', 'Ldt (light displacement ton)': 'LDT',
    'BUILT': 'BUILT', 'Built in (y)': 'BUILT',
    'FLAG': 'LAST_FLAG', 'Last flag': 'LAST_FLAG', 'LAST FLAG': 'LAST_FLAG', 'FLAG_STATE': 'LAST_FLAG',
    'PLACE': 'PLACE', 'Destination yard': 'PLACE',
    'COUNTRY': 'COUNTRY',
    'BENEFICIAL OWNER': 'OWNER_NAME', 'Beneficial owner': 'OWNER_NAME', 'Beneficial owner of the ship': 'OWNER_NAME'
}

#extracting the year from the column name and using it as a column
def get_year_from_filename(filepath):
    match = re.search(r'\d{4}', os.path.basename(filepath))
    return int(match.group(0)) if match else None

#loading and cleaning the data into a single csv file
def clean_and_harmonize_data(filepath):
    year = get_year_from_filename(filepath)
    if not year:
        print(f"  - Warning: No year in filename '{os.path.basename(filepath)}'. Skipping.")
        return pd.DataFrame()

    print(f"Processing: {os.path.basename(filepath)} [Year: {year}]")

    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            header_row_index = next((i for i, line in enumerate(f) if any(kw in line for kw in ['IMO#', 'IMO number', 'Beneficial owner'])), 0)
        df = pd.read_csv(filepath, skiprows=header_row_index, header=0, dtype=str, on_bad_lines='skip')
    except Exception as e:
        print(f"  - Error parsing CSV '{os.path.basename(filepath)}': {e}. Skipping file.")
        return pd.DataFrame()

    df = df.rename(columns=lambda c: c.strip())
    df = df.rename(columns=COLUMN_MAPPING)
    df['YEAR'] = year

    if 'GT_LDT_MIXED' in df.columns:
        df['GT'] = df['GT_LDT_MIXED'].str.extract(r'GT:?\s*(\d+)', expand=False, flags=re.IGNORECASE)
        df['LDT'] = df['GT_LDT_MIXED'].str.extract(r'LDT:?\s*([\d\.]+)', expand=False, flags=re.IGNORECASE)

    all_needed_cols = set(SCHEMA) | {'OWNER_NAME'} - {'AGE'}
    for col in all_needed_cols:
        if col not in df.columns:
            df[col] = pd.NA
    
    df['NAME'] = df['NAME'].str.strip()
    missing_name_mask = df['NAME'].isna() | df['NAME'].isin(['', 'Unknown', 'nan'])
    df.loc[missing_name_mask, 'NAME'] = df.loc[missing_name_mask, 'OWNER_NAME']

    df['TYPE'] = df['TYPE'].str.lower().fillna('unknown')
    type_conditions = [
        df['TYPE'].str.contains('container'), df['TYPE'].str.contains('tanker'),
        df['TYPE'].str.contains(r'bulk|ore carrier'), df['TYPE'].str.contains(r'ro-ro|roro|vehicles'),
        df['TYPE'].str.contains('passenger|cruise'), df['TYPE'].str.contains('supply|tug|offshore'),
        df['TYPE'].str.contains('fishing'), df['TYPE'].str.contains(r'dredger|hopper'),
        df['TYPE'].str.contains('refrigerated|reefer')
    ]
    type_choices = ['Container Ship', 'Tanker', 'Bulk Carrier', 'Ro-Ro/Vehicle Carrier', 'Passenger/Cruise Ship', 
                    'Offshore/Supply Vessel', 'Fishing Vessel', 'Dredger', 'Reefer']
    df['TYPE'] = np.select(type_conditions, type_choices, default='Other')

    numeric_cols = ['IMO', 'GT', 'LDT', 'BUILT', 'YEAR']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['AGE'] = df['YEAR'] - df['BUILT']
    
    def impute_country(place):
        if pd.isna(place): return 'Unknown'
        place_lower = str(place).lower()
        if any(p in place_lower for p in ['alang', 'sachana', 'mumbai']): return 'India'
        if any(p in place_lower for p in ['chittagong', 'chattogram']): return 'Bangladesh'
        if 'gadani' in place_lower: return 'Pakistan'
        if 'aliaga' in place_lower: return 'Turkey'
        return 'Unknown'
    
    df['COUNTRY'] = df['COUNTRY'].fillna(df['PLACE'].apply(impute_country))
    
    string_cols = ['NAME', 'TYPE', 'LAST_FLAG', 'PLACE', 'COUNTRY']
    for col in string_cols:
        df[col] = df[col].str.strip().fillna('Unknown')
        
    return df[SCHEMA]

#loading and processing the csv files, using imputation for missing values
def create_integration_pipeline(csv_dir='curr_data', output_file='shipbreaking_unified.csv'):
    all_files = sorted(glob.glob(os.path.join(csv_dir, '*.csv')))
    if not all_files:
        print(f"Error: No CSV files found in '{csv_dir}'.")
        return

    print(f"Found {len(all_files)} files to process...")
    all_dfs = [clean_and_harmonize_data(f) for f in all_files]
    integrated_df = pd.concat(all_dfs, ignore_index=True)
    
   #imputing GT and LDT based on other ship records using IMO 
    print("\nPerforming ship-specific imputation for GT and LDT...")
    integrated_df['GT'] = integrated_df.groupby('IMO')['GT'].transform(lambda x: x.ffill().bfill())
    integrated_df['LDT'] = integrated_df.groupby('IMO')['LDT'].transform(lambda x: x.ffill().bfill())
    print("Ship-specific imputation complete. Remaining missing values will be handled in the modeling phase.")

    initial_rows = len(integrated_df)
    integrated_df.dropna(subset=['IMO'], inplace=True)
    integrated_df['IMO'] = integrated_df['IMO'].astype(int)
    print(f"\nDropped {initial_rows - len(integrated_df)} rows with missing IMO numbers.")
    
    integrated_df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"\nPipeline complete. Unified data saved to '{output_file}'.")
    
    print("\n--- Sample of Final Unified Data (with NaNs preserved) ---")
    print(integrated_df.head())
    print("\n--- Unified Data Info ---")
    integrated_df.info()

if __name__ == '__main__':
    create_integration_pipeline()