import pandas as pd
from datetime import datetime
import pytz
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

def modify_csv_values(csv_path):
    try:
        # Read the CSV file, keeping ts_event as strings initially
        df = pd.read_csv(csv_path)
        
        # Verify required columns exist
        required_columns = ['ts_event', 'open', 'high', 'low', 'close']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Error: Missing required columns: {missing_columns}")
            return
        
        # Store original ts_event as strings to preserve exact format
        original_ts_event = df['ts_event'].astype(str).copy()
        
        # Convert ts_event to datetime for filtering
        try:
            df['ts_event'] = pd.to_datetime(df['ts_event'], format='mixed', errors='coerce')
            if df['ts_event'].isna().any():
                raise ValueError("Some ts_event values could not be parsed as datetime. Please check the format.")
        except Exception as e:
            print(f"Error: Failed to parse ts_event as datetime. Details: {e}")
            return
        
        # Convert to EST timezone
        est = pytz.timezone('America/New_York')
        if df['ts_event'].dt.tz is None:
            df['ts_event'] = df['ts_event'].dt.tz_localize(est)
        else:
            df['ts_event'] = df['ts_event'].dt.tz_convert(est)
        
        # Get default timestamps from the DataFrame
        first_timestamp = df['ts_event'].iloc[0]  # First row timestamp (oldest)
        last_timestamp = df['ts_event'].iloc[-1]  # Last row timestamp (most recent)
        
        # Prompt for start date and time
        print("Enter the start date and time (format: YYYY-MM-DD HH:MM:SS, e.g., 2025-09-17 14:00:00)")
        print("Leave blank to use the first row's timestamp (oldest)")
        start_date_input = input("Start date and time: ").strip()
        
        if start_date_input:
            try:
                # Parse the start date and time, assuming EST
                start_datetime = est.localize(datetime.strptime(start_date_input, '%Y-%m-%d %H:%M:%S'))
            except ValueError as e:
                print(f"Error: Invalid start date format. Please use YYYY-MM-DD HH:MM:SS. Details: {e}")
                return
        else:
            start_datetime = first_timestamp  # Use first row's timestamp if blank
        
        # Prompt for end date and time
        print("Enter the end date and time (format: YYYY-MM-DD HH:MM:SS, e.g., 2025-09-17 16:00:00)")
        print("Leave blank to use the last row's timestamp (most recent)")
        end_date_input = input("End date and time: ").strip()
        
        if end_date_input:
            try:
                # Parse the end date and time, assuming EST
                end_datetime = est.localize(datetime.strptime(end_date_input, '%Y-%m-%d %H:%M:%S'))
            except ValueError as e:
                print(f"Error: Invalid end date format. Please use YYYY-MM-DD HH:MM:SS. Details: {e}")
                return
        else:
            end_datetime = last_timestamp  # Use last row's timestamp if blank
        
        # Validate that end_datetime is after start_datetime
        if end_datetime <= start_datetime:
            print("Error: End date and time must be after start date and time.")
            return
        
        # Prompt for columns to modify
        print("Select columns to modify (comma-separated: open, high, low, close, or 'all' for all price columns):")
        columns_input = input("Columns: ").lower().strip()
        
        if columns_input == 'all':
            columns_to_modify = ['open', 'high', 'low', 'close']
        else:
            columns_to_modify = [col.strip() for col in columns_input.split(',')]
            # Validate columns
            valid_columns = ['open', 'high', 'low', 'close']
            invalid_columns = [col for col in columns_to_modify if col not in valid_columns]
            if invalid_columns:
                print(f"Error: Invalid columns {invalid_columns}. Choose from: open, high, low, close.")
                return
        
        # Prompt for adjustment value
        print("Enter the adjustment value (e.g., +60 or -40):")
        adjustment_input = input("Adjustment value: ").strip()
        
        try:
            adjustment_value = float(adjustment_input)
        except ValueError:
            print("Error: Adjustment value must be a number (e.g., +60 or -40).")
            return
        
        # Filter rows within the specified time range
        mask = (df['ts_event'] >= start_datetime) & (df['ts_event'] <= end_datetime)
        
        # Apply the adjustment only to the specified columns
        for column in columns_to_modify:
            df.loc[mask, column] += adjustment_value
        
        # Restore original ts_event strings to ensure no changes
        df['ts_event'] = original_ts_event
        
        # Save the modified DataFrame to the original CSV
        df.to_csv(csv_path, index=False)
        print(f"Modified CSV saved to {csv_path}")
        print(f"Adjusted {len(df[mask])} rows between {start_datetime} and {end_datetime} by {adjustment_value} in columns: {columns_to_modify}")
        
    except FileNotFoundError:
        print(f"Error: File {csv_path} not found.")
    except Exception as e:
        print(f"Error: An unexpected error occurred: {e}")

if __name__ == "__main__":
    input_csv = "historical_ohlcv_15m.csv"
    modify_csv_values(input_csv)