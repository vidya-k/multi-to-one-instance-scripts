import pandas as pd

# Specify the Excel file and sheet name

excel_file = 'colleges_data.xlsx'
#excel_file = 'colleges_data.xlsx'
sheet_name = 'DEEMED UNIVERSITY'  # Change to the actual sheet name

# Try to load the Excel file
try:
    df = pd.read_excel(excel_file, sheet_name=sheet_name, engine='openpyxl')
except Exception as e:
    print(f"Error loading the Excel file: {e}")
    df = None

# If the DataFrame is not empty, proceed to save as CSV
if df is not None and not df.empty:
    # Specify the CSV file
    csv_file = 'output.csv'

    # Save the DataFrame to CSV
    try:
        df.to_csv(csv_file, index=False)
        print(f"CSV file '{csv_file}' created successfully.")
    except Exception as e:
        print(f"Error saving DataFrame to CSV: {e}")
else:
    print("DataFrame is empty. Check the Excel file and sheet name.")

