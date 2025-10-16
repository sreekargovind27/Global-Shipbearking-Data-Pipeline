# Phase 1: Data Pipeline & Schema Harmonization 
## Step 1: File Conversion

import pandas as pd
import os
import glob

#converting excel files to csv
def convert_excel_to_csv(excel_dir='excel_files', csv_dir='curr_data'):
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)

    excel_files = glob.glob(os.path.join(excel_dir, '*.xlsx')) + glob.glob(os.path.join(excel_dir, '*.xls'))
    
    if not excel_files:
        print(f"No Excel files found in '{excel_dir}'. Place original files here to run.")
        return

    print(f"Found {len(excel_files)} Excel files. Starting conversion...")

    for excel_file in excel_files:
        try:
            #read all data as strings to prevent automatic type conversion errors
            df = pd.read_excel(excel_file, dtype=str)
            
            base_name = os.path.basename(excel_file)
            csv_name = os.path.splitext(base_name)[0] + '.csv'
            csv_path = os.path.join(csv_dir, csv_name)
            
            #save to CSV with UTF-8 encoding
            df.to_csv(csv_path, index=False, encoding='utf-8')
            print(f"Successfully converted '{excel_file}' to '{csv_path}'")
        
        except Exception as e:
            print(f"Could not convert '{excel_file}'. Error: {e}")

if __name__ == '__main__':
    if not os.path.exists('excel_data'):
        os.makedirs('excel_data')
    convert_excel_to_csv()