import pandas as pd

# Sample data
data = {
    'Name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
    'Email': ['john.doe@example.com', 'jane.smith@example.com', 'bob.johnson@example.com'],
    'Phone': ['123-456-7890', '987-654-3210', '555-123-4567']
}

# Create a DataFrame
df = pd.DataFrame(data)

# Specify the Excel file name
excel_file = 'output.xlsx'

# Write the DataFrame to Excel
df.to_excel(excel_file, index=False, engine='openpyxl')

print(f"Excel file '{excel_file}' created successfully.")

