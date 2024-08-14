import os, json
import numpy as np
import pandas as pd
from datetime import datetime

# Function to check date format and return True if valid, False otherwise
def validate_and_convert_date(date_str):
    if date_str.strip():  # Check if not empty or null
        try:
            # Use '%m/%d/%y' format for validation and conversion
            return datetime.strptime(date_str, '%m/%d/%y')
        except ValueError:
            return None  # Return None for invalid date strings
    return None  # Return None for empty strings or non-string values

def join_unique(x):
    return ', '.join(set(filter(None, map(str.strip, x))))

def join_unique_address(x):
    return '/ '.join(set(filter(None, map(str.strip, x))))

# File paths
input_folder_path = '/Users/bhavyateja/Kona_AI/work/CytrexCyber/Workbooks/'
final_output_file_path = '/Users/bhavyateja/Kona_AI/work/CytrexCyber/final_df1.csv'
levels_json_file = '/Users/bhavyateja/Kona_AI/work/CytrexCyber/levels.json'
files = os.listdir(input_folder_path)

dtype_dict = {
    'Associated Docs (People)': str,
    'People Tracker Control Number': str,
    'First Name or Initial': str,
    'Middle Name or Initial': str,
    'Last Name': str,
    'City': str,
    'State': str,
    'ZIP Code': str,
    'Date of Birth': str,
    'Financial Account Number': str
}

# List to store DataFrames
dfs = []

# Loop through each file and read it into a DataFrame, then append to the list
for file in files:
    if file.endswith('.csv'):  # Check if the file is a CSV file
        file_path = os.path.join(input_folder_path, file)  # Get the full file path
        df = pd.read_csv(file_path, dtype=dtype_dict)
        dfs.append(df)

# Combine DataFrames
combined_df = pd.concat(dfs, ignore_index=True)

# Removing Row Duplicates

combined_df.drop_duplicates(inplace=True)

# Fill NaN values with empty string ('') for non-numeric columns
non_numeric_columns = combined_df.select_dtypes(exclude=['number']).columns
combined_df[non_numeric_columns] = combined_df[non_numeric_columns].fillna('')

# Remove rows with NaN and empty strings in 'Social Security Number' column
combined_df = combined_df[
    (combined_df['Social Security Number'].notnull()) &  # Not NaN
    (combined_df['Social Security Number'] != '')       # Not empty string
].sort_values(by='Social Security Number')

# Filter rows based on valid date format in 'Date of Birth' column
combined_df['Date of Birth'] = combined_df['Date of Birth'].apply(validate_and_convert_date)
# Assuming df_sorted is your DataFrame
combined_df['Date of Birth'] = pd.to_datetime(combined_df['Date of Birth'], errors='coerce')
# combined_df['Date of Birth'] = combined_df['Date of Birth'].fillna(pd.to_datetime('1900-01-01'))
combined_df['Date of Birth'] = combined_df['Date of Birth'].fillna('')

# If you want to convert the 'Date of Birth' back to string format with the desired format
combined_df['Date of Birth'] = combined_df['Date of Birth'].dt.strftime('%m/%d/%Y')

# # Cleansing the Address Fields & Concatenating them

# combined_df.fillna({'Suffix': '', 'State': '', 'ZIP Code': '', 'Country (if not USA)': ''}, inplace=True)

# Validating the ZIP Codes, State, City fields
valid_zip_mask = combined_df['ZIP Code'].str.match(r'^\d{5}$') | (combined_df['ZIP Code'] == '')
valid_state_mask = combined_df['State'].str.match(r'^[A-Z]{2}$') | (combined_df['State'] == '')
valid_city_mask = combined_df['City'].str.match(r'^[a-zA-Z\s]+$') | (combined_df['City'] == '')
filtered_df = combined_df[valid_zip_mask & valid_state_mask & valid_city_mask]
filtered_df['Address'] = filtered_df['Address'].str.strip()

# # Concatenate fields with ',' as separator, excluding empty or '' fields
# filtered_df['Full Address'] = filtered_df.apply(
#     lambda row: ', '.join(filter(None, [row['Address'], row['City'], row['State'], row['ZIP Code'], row['Country (if not USA)']])),
#     axis=1
# )

# # Drop the columns
# filtered_df.drop(columns=['Address', 'City', 'State', 'ZIP Code', 'Country (if not USA)'], inplace=True)

# Replacing Nan's with empty strings
df_sorted = filtered_df.applymap(lambda x: '' if pd.isna(x) else x)

# Convering the columns to strings
df_sorted['Address'] = df_sorted['Address'].astype(str)
df_sorted['City'] = df_sorted['City'].astype(str)
df_sorted['State'] = df_sorted['State'].astype(str)
df_sorted['ZIP Code'] = df_sorted['ZIP Code'].astype(str)
df_sorted['Country (if not USA)'] = df_sorted['Country (if not USA)'].astype(str)
df_sorted['Financial Account Number'] = df_sorted['Financial Account Number'].astype(str)
df_sorted['Driver\'s License Number / State ID Number'] = df_sorted['Driver\'s License Number / State ID Number'].astype(str)
df_sorted['PII'] = df_sorted['PII'].astype(str)
df_sorted['Associated Docs (People)'] = df_sorted['Associated Docs (People)'].astype(str)
df_sorted['People Tracker Control Number'] = df_sorted['Associated Docs (People)'].astype(str)

# Load the JSON file
with open(levels_json_file, 'r') as file:
    levels_data = json.load(file)

# Remove quotations around 'join_unique' in aggregation functions
for level_data in levels_data.values():
    aggregation_functions = level_data['aggregation_functions']
    for col, func in aggregation_functions.items():
        if func == 'join_unique':
            aggregation_functions[col] = join_unique
        if func == 'join_unique_address':
            aggregation_functions[col] = join_unique_address

# Iterate over the levels and perform aggregation
for level_name, level_data in levels_data.items():
    columns = level_data['columns']
    aggregation_functions = level_data['aggregation_functions']
    df_grouped = df_sorted.groupby(columns).agg(aggregation_functions).reset_index()
    # Perform further operations with df_grouped as needed
    print(f"Grouping by {level_name} with columns - {columns} completed.")


# Split the 'firstname', 'lastname', 'middle name', and 'dob' columns by commas and expand them into separate columns
df_split = pd.concat([df_grouped[col].str.split(',', expand=True).add_prefix(f'{col}_') for col in ['First Name or Initial', 'Last Name', 'Middle Name or Initial', 'Date of Birth']], axis=1)

# Concatenate the split columns with the original DataFrame
df_combined = pd.concat([df_grouped, df_split], axis=1)

# Writing the final output to a file
df_combined.to_csv(final_output_file_path, index=False)