import json
import xlsxwriter
from pymongo import MongoClient, UpdateMany

list_tenants = {}
details = {}
results = []
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
            worksheet.write(row_num, col_num, value)

if __name__ == "__main__":
    excel_file = "updated_alumni_data.xlsx"
    column_names = ["Name", "Email", "Phone", "Department", "Degree", "Year of Passing", 
                    "P.O. Academic Year", "College Name","CGPA"]

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
       
    def getResultData(usn):
        try:
            for inner_list in results:
                 if inner_list["usn"] == usn :
                    return inner_list["cgpa"]    
        except Exception as e:
                    print(e)
                    return 'NA'

                
    getTenants()

    # Open the workbook outside of the loop
    workbook = xlsxwriter.Workbook(excel_file)

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
                        temp_collage_name= document['name']

                    #print(collage_name)
                    usercol = db['dhi_user']
                    user_list = usercol.find({"$or":[{"studentStatus": "PASS_OUT"},{"userType":"ALUMNI"}]},
                                              {"name": 1, "email": 1, "mobile": 1, "deptName": 1, "degreeId": 1,
                                               "passOutYear": 1, "academicYear": 1,"usn":1}) 
                     
                    resultCollection = db['dhi_university_exam']
                    pipeline = [
                        {"$match":{"term.scores.cgpa":{"$gt":0}}},
                        {"$unwind":"$term.scores"},
                        {"$match":{"term.scores.cgpa":{"$gt":0}}},
                         {"$project":{"usn":"$term.scores.usn","data":{"termNumber":"$term.termNumber","cgpa":"$term.scores.cgpa"}}},
                        {"$group":{"_id":"$usn","maxTermData": {
                                "$max": {
                                  "termNumber": "$data.termNumber" ,  
                                  "cgpa": "$data.cgpa"                        
                                    }
                                  }}},
                        {"$project":{"usn":"$_id","cgpa":"$maxTermData.cgpa"}}
                         ]
                    results = list(resultCollection.aggregate(pipeline))
                    # # Truncate or modify the sheet name to fit within the 31-character limit
                    collage_name = collage_name[:31]
                    # Check if user data is available
                    if int(user_list.count()) > 0:
                        # Create or load the worksheet for each college
                        worksheet = workbook.add_worksheet(collage_name)

                        # Write column headers
                        for col_num, header in enumerate(column_names):
                            worksheet.write(0, col_num, header)

                        # Insert data into the sheet
                        user_data = [
                            [
                                user["name"] if "name" in user else "",
                                user["email"] if "email" in user else "",
                                user["mobile"] if "mobile" in user else "",
                                user["deptName"] if "deptName" in user else "",
                                user["degreeId"] if "degreeId" in user else "",
                                user["passOutYear"] if "passOutYear" in user else "",
                                user["academicYear"] if "academicYear" in user else "",
                                temp_collage_name,
                                getResultData(user["usn"]) if "usn" in user else "",
                                
                            ]
                            for user in user_list
                        ]
                        results=[]
                        insert_data(worksheet, user_data)

                    else:
                        # Print college name if there's no user data
                        print(f"No user data available for {temp_collage_name}")

                except Exception as e:
                    print(f"Error in {dburi['dburi'].split('/')[3].split('?')[0].strip()} {e}")
        except Exception as e:
            print(e)

    # Save the workbook after processing all colleges
    workbook.close()

