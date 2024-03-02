import pandas as pd

def create_excel(filename, sheet_name, columns):
    # Create a new workbook
    writer = pd.ExcelWriter(filename, engine='openpyxl')
    writer.book.create_sheet(title=sheet_name, index=0)

    # Add columns to the sheet
    df = pd.DataFrame(columns=columns)
    df.to_excel(writer, sheet_name=sheet_name, index=False)

    # Save the workbook
    writer.book.save(filename)

# Example usage
excel_file = 'colleges_data.xlsx'
sheet_name = 'DEEMED UNIVERSITY'
column_names = ['Name', 'Email', 'Phone', 'Department', 'Degree', 'Year of Passing', 'College ID']

create_excel(excel_file, sheet_name, column_names)

