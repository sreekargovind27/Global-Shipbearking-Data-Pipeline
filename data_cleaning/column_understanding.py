## Step 2: Schema Discovery & Unification (JUST UNDERSTANDNING THE RAW DATA)

import os
import csv

# Define folder and output file
folder_path = 'curr_data'
output_file = 'output.txt'

# Open the output file for writing
with open(output_file, 'w', encoding='utf-8') as out_f:
    # Traverse all files in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            out_f.write(f"--- File: {filename} ---\n")
            
            try:
                with open(file_path, 'r', encoding='utf-8') as csvfile:
                    reader = csv.reader(csvfile)
                    rows = []
                    for i, row in enumerate(reader):
                        if i >= 10:  # Only take first 10 rows
                            break
                        rows.append(row)
                    
                    if rows:
                        for idx, row_list in enumerate(rows, 1):
                            out_f.write(f"Row {idx}: {row_list}\n")
                    else:
                        out_f.write("No data rows found.\n")
                        
            except Exception as e:
                out_f.write(f"Error reading file: {e}\n")
            
            out_f.write("\n")  

print(f"Output written to {output_file}")