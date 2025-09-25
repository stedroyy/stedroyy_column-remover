import pandas as pd
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Define the input and output file paths
input_file = "mnq_1min_historical_data_modified_shifted.csv"  # Input CSV file
output_file = "mnq_1min_historical_data_modified_shifted.csv"  # Output CSV file

# Ensure the input file exists
if not os.path.exists(input_file):
    print(f"Error: The file {input_file} does not exist.")
    exit()

# List of columns to delete (modify this based on your CSV's column names)
columns_to_delete = [
    "volume",  # Example: common column in Databento data
            # Example: another common Databento column
]

try:
    # Read the CSV file
    df = pd.read_csv(input_file)

    # Check which columns to delete are actually present in the CSV
    existing_columns = df.columns.tolist()
    columns_to_drop = [col for col in columns_to_delete if col in existing_columns]

    if not columns_to_drop:
        print("None of the specified columns were found in the CSV.")
        exit()

    # Drop the specified columns
    df = df.drop(columns=columns_to_drop)

    # Save the modified DataFrame to a new CSV file
    df.to_csv(output_file, index=False)
    print(f"Successfully deleted columns {columns_to_drop} and saved to {output_file}")

except Exception as e:
    print(f"An error occurred: {e}")
