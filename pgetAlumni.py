import json
import xlsxwriter
from pymongo import MongoClient, UpdateMany

list_tenants = {}
details = {}

def create_or_load_excel(filename, sheet_name, columns):
    workbook = xlsxwriter.Workbook(filename)

    # Truncate or modify the sheet name to fit within the 31-character limit
    sheet_name = sheet_name[:31]

    worksheet = workbook.add_worksheet(sheet_name)

    # Write column headers
    for col_num, header in enumerate(columns):
        worksheet.write(0, col_num, header)

    return workbook, worksheet

def insert_data(worksheet, data, start_row=1):
    for row_num, row_data in enumerate(data, start=start_row):
        for col_num, value in enumerate(row_data):
            print(value)
            worksheet.write(row_num, col_num, value)

if __name__ == "__main__":
    excel_file = "colleges_data.xlsx"
    column_names = ["Name", "Email", "Phone", "Department", "Degree", "Year of Passing", "College ID"]

    def getTenants():
        with open('multi-tenancy.yml', 'r') as reader:
            for line in reader.readlines():
                dbip = ''
                details = {}
                if 'tenantName' in line:
                    tenantName = line.strip('tenantName: ').split(':')[1]
                    details['tenant'] = tenantName
                if 'uri' in line:
                    uri = line.strip('uri: ')
                    dbip = line.split(':')[3].split('@')[1]
                    details['dburi'] = uri
                    global list_tenants
                    list_tenants.setdefault(dbip, []).append(details)

    getTenants()

    for serverip in list_tenants:
        try:
            for dburi in list_tenants[serverip]:
                try:
                    configDetails = {}
                    dbname = dburi['dburi'].split('/')[3].split('?')[0].strip()
                    client = MongoClient(dburi['dburi'].strip())
                    db = client[dbname]
                    college_col = db['college']
                    cursor = college_col.find({})

                    for document in cursor:
                        collage_name = document['name']

                    print(collage_name)
                    usercol = db['dhi_user']
                    user_list = usercol.find({"userType": "ALUMNI"},
                                              {"name": 1, "email": 1, "mobile": 1, "deptName": 1, "degreeId": 1,
                                               "academicYear": 1, "tenantId": 1})

                    workbook, worksheet = create_or_load_excel(excel_file, collage_name, column_names)

                    # Insert data into the sheet
                    #user_data = [[user["name"], user["email"], user["mobile"], user["deptName"], user["degreeId"],
                     #             user["academicYear"], user["tenantId"]] for user in user_list]

                    # Insert data into the sheet
                    user_data = [
                                 [
                                     user["name"] if "name" in user else "",              # Use user["name"] if it exists, otherwise an empty string
                                     user["email"] if "email" in user else "",
                                     user["mobile"] if "mobile" in user else "",
                                     user["deptName"] if "deptName" in user else "",
                                     user["degreeId"] if "degreeId" in user else "",
                                     user["academicYear"] if "academicYear" in user else "",
                                     user["tenantId"] if "tenantId" in user else ""
                                 ]
                                 for user in user_list
                               ]

                    insert_data(worksheet, user_data)

                    workbook.close()

                except Exception as e:
                    print(e)
        except Exception as e:
            print(e)

