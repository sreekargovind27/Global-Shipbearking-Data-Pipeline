# CSE 587 - Mid-Term: Global Shipbreaking Data Pipeline

This project is a solution for the CSE 587 (Data Intensive Computing) mid-term assignment. The goal is to design and implement a reproducible data pipeline to process, clean, analyze, and model real-world data on global ship dismantling from 2012-2024, sourced from the NGO Shipbreaking Platform.

## Project Structure

The repository is organized as follows:

.

├── data_cleaning/

│ ├── curr_data/ # Folder for the raw CSV files.

│ ├── excel_files/ # Folder for the original Excel files.

│ ├── column_understanding.py # Script to inspect raw data schemas.

│ ├── file_conversion.py # Script for reproducible Excel-to-CSV conversion.

│ └── integrate_shipbreaking.py # Main script for data cleaning, harmonization, and integration.

│ └──shipbreaking_unified.csv # The final, clean, and integrated dataset produced by the pipeline.

│

├── PLOTS/

│ ├── eda_plots

│ │ ├── plots ... .. ..

│ ├── ml_algorithms_plots

│ │ ├── plots ... .. ..

├── eda_predictive_modelling_govindsr.ipynb # Jupyter Notebook for EDA and Predictive Modeling.

├── ReadMe.md # This readme file.

└── requirements.txt # Required Python packages for the project.

## Core Deliverables

*   **`file_conversion.py`**: A Python script to convert the original Excel files into a consistent CSV format, handling encoding issues and preserving raw data.
*   **`integrate_shipbreaking.py`**: The main data pipeline script. It loads all raw CSV files, harmonizes their schemas, cleans the data, performs advanced imputation (for `NAME`, `GT`, `LDT`), standardizes ship types, calculates the `AGE` column, and outputs the final `shipbreaking_unified.csv`.
*   **`shipbreaking_unified.csv`**: The final integrated dataset, ready for analysis.
*   **`eda_predictive_modelling_govindsr.ipynb`**: A Jupyter Notebook that performs all the required Exploratory Data Analysis (EDA) and builds, trains, and evaluates multiple machine learning models (k-NN, SVM, Naive Bayes, Logistic Regression, Random Forest, Gradient Boosting).

## How to Run the Project

Follow these steps to set up the environment and run the complete pipeline.

### 1. Setup Environment

It is recommended to use a virtual environment.

## Create a virtual environment
``` bash
python -m venv midtermenv
```
## Activate it (Windows)
``` bash
.\midtermenv\Scripts\activate
```

## Activate it (macOS/Linux)
``` bash
source midtermenv/bin/activate
```

### 2. Install Dependencies
Install all the required Python packages using the requirements.txt file.

``` bash
pip install --upgrade -r requirements.txt
```

### 3. Place Data Files
- Place the original Excel files inside the data_cleaning/excel_files/ directory.
- Place the CSV files inside the data_cleaning/curr_data/ directory.

### 4. Run the Pipeline and Analysis

- Execute the scripts and notebook in the following order from the project's root directory:

#### Step A: Run the Integration Pipeline

- This script will process all CSVs in data_cleaning/curr_data/ and generate the final shipbreaking_unified.csv in the root directory.

``` bash
python data_cleaning/integrate_shipbreaking.py
```

#### Step B: Run the EDA and Modeling Notebook

Open and run all cells in the eda_predictive_modelling_govindsr.ipynb Jupyter Notebook. This will generate all visualizations and model evaluation results. You can run it manually by clicking the run button of each cell/Run All command to run every cell.






