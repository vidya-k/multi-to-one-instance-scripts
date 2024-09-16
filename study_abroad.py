import json
import xlsxwriter
from pymongo import MongoClient

list_tenants = {}
results = []

def create_or_load_excel(filename, sheet_name, columns):
    workbook = xlsxwriter.Workbook(filename)
    sheet_name = sheet_name[:31]  # Truncate or modify the sheet name to fit within the 31-character limit
    worksheet = workbook.add_worksheet(sheet_name)
    # Write column headers
    for col_num, header in enumerate(columns):
        worksheet.write(0, col_num, header)
    return workbook, worksheet

def insert_data(worksheet, data, start_row=1):
    for row_num, row_data in enumerate(data, start=start_row):
        for col_num, value in enumerate(row_data):
            worksheet.write(row_num, col_num, value)

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
                list_tenants.setdefault(dbip, []).append(details)

def sortTerm(e):
    return e['termNumber']

def getAcademicYearData(yearDeatils):
    try:
        if yearDeatils !=None:
            for year in yearDeatils:
                if year["currentYear"]==True:
                    return year["academicYear"]
        return "NA"
    except:
        return "NA"
            
def getResultData(user):
    try:
        for inner_list in results:
            if "usn" in inner_list and "usn" in user and inner_list["usn"] == user["usn"]:
                inner_list["terms"].sort(key=sortTerm)
                term_sgpa = {}
                for term in inner_list["terms"]:
                    term_sgpa[f"SGPA Term {term['termNumber']}"] = term["sgpa"]
                user["cgpa"] = inner_list["cgpa"]
                return user["cgpa"], term_sgpa  # Return both CGPA and dynamic SGPA terms
        # If no matching USN is found, return 'NA' for CGPA and an empty dictionary for SGPA terms
        return 'NA', {}
    except Exception as e:
        # If an exception occurs, return 'NA' and an empty dictionary as fallback values
        print(f"Error in getResultData for user {user['usn']}: {e}")
        return 'NA', {}


if __name__ == "__main__":
    excel_file = "updated_alumni_data.xlsx"
    static_columns = ["Name", "Email", "Phone", "Department", "Degree", "Year of Passing",
                      "P.O. Academic Year", "College Name", "CGPA"]

    getTenants()

    # Open the workbook outside of the loop
    workbook = xlsxwriter.Workbook(excel_file)

    for serverip in list_tenants:
        for dburi in list_tenants[serverip]:
            try:
                dbname = dburi['dburi'].split('/')[3].split('?')[0].strip()
                client = MongoClient(dburi['dburi'].strip())
                db = client[dbname]

                college_col = db['college']
                user_col = db['dhi_user']
                result_col = db['dhi_university_exam']

                # Get college name
                college_name = college_col.find_one().get('name', '')[:31]  # Truncate college name if too long

                # Query alumni data
                user_list = user_col.find({"$or": [{"studentStatus": "PASS_OUT"}, {"userType": "ALUMNI"}]},
                                          {"name": 1, "email": 1, "mobile": 1, "deptName": 1, "degreeId": 1,
                                           "passOutYear": 1, "academicYear": 1, "usn": 1,"yearDetails":1})

                # Build pipeline for results
                pipeline = [
                    {"$unwind": "$term.scores"},
                    {"$project": {"usn": "$term.scores.usn",
                                  "data": {"termNumber": "$term.termNumber", "cgpa": "$term.scores.cgpa", "sgpa": "$term.scores.sgpaOrPercentage"}}},
                    {"$group": {"_id": {"usn": "$usn", "termNumber": "$data.termNumber"}, "sgpa": {"$max": "$data.sgpa"},
                                "cgpa": {"$max": "$data.cgpa"}}},
                    {"$group": {"_id": "$_id.usn", "maxTermData": {"$max": {"termNumber": "$_id.termNumber", "cgpa": "$cgpa"}},
                                "terms": {"$push": {"termNumber": "$_id.termNumber", "sgpa": "$sgpa"}}}},
                    {"$project": {"usn": "$_id", "cgpa": "$maxTermData.cgpa", "terms": 1}}
                ]

                results = list(result_col.aggregate(pipeline))

                if user_list.count() > 0:
                    worksheet = workbook.add_worksheet(college_name)

                    # Dynamically generate SGPA column headers based on the maximum number of terms
                    max_terms = 0
                    for result in results:
                        if len(result['terms']) > max_terms:
                            max_terms = len(result['terms'])

                    # Add dynamic SGPA columns to static columns
                    dynamic_sgpa_columns = [f"SGPA Term {i + 1}" for i in range(max_terms)]
                    column_names = static_columns + dynamic_sgpa_columns

                    # Write column headers
                    for col_num, header in enumerate(column_names):
                        worksheet.write(0, col_num, header)

                    # Insert data into the sheet
                    user_data = []
                    for user in user_list:
                        user_row = [
                            user.get("name", ""),
                            user.get("email", ""),
                            user.get("mobile", ""),
                            user.get("deptName", ""),
                            user.get("degreeId", ""),
                            user.get("passOutYear", ""),
                            getAcademicYearData(user["yearDetails"]),
                            college_name,
                        ]
                        cgpa, sgpa_terms = getResultData(user)

                        # Add CGPA
                        user_row.append(cgpa)

                        # Append SGPA data for the terms (and pad missing terms with blank values)
                        for i in range(max_terms):
                            user_row.append(sgpa_terms.get(f"SGPA Term {i + 1}", ""))

                        user_data.append(user_row)

                    insert_data(worksheet, user_data)

                else:
                    print(f"No user data available for {college_name}")

            except Exception as e:
                print(f"Error processing {dbname}: {e}")

    workbook.close()
