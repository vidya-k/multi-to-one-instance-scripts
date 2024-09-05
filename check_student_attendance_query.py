from pymongo import MongoClient
import subprocess
            

def restore_attendance_data(mongo_uri_prod,mongo_uri_qa,date_str):   
             #  mongoUri=d['uri']
             print("Date  " + date_str)
             collectionName="dhi_student_attendance"
             mongoUri='{}/{}'.format(mongo_uri_prod['uri'].split(',')[0],mongo_uri_prod['uri'].split(',')[2].split('/')[1].split('?')[0])
            #  print(mongoUri)
             client = MongoClient(mongoUri)
             dbName='{}'.format(mongo_uri_prod['uri'].split(',')[2].split('/')[1].split('?')[0])
            #  dbName = d['uri'].split('/')[3]
             data_base = client[dbName]
             prodDB=data_base[collectionName]
             pipeline = [
               {"$match":{"academicYear":"2024-25","students.studentAttendance.attendanceStatus":"BLANK"}},
                ]
             results = list(prodDB.aggregate(pipeline))
             count=0
             if len(results)>0:
                print("results " + str(len(results)))
               # restore backup
                ids = [obj["_id"] for obj in results]
               #  qaMongoUri='{}/{}'.format(mongo_uri_qa.split(',')[0],mongo_uri_qa['uri'].split(',')[2].split('/')[1].split('?')[0])
                qaclient = MongoClient(mongo_uri_qa)
               #  qaDbName='{}'.format(mongo_uri_qa.split(',')[2].split('/')[1].split('?')[0])
                qaDbName=mongo_uri_qa.split('/')[3]
                qa_database=qaclient[qaDbName]
                qa_db=qa_database[collectionName]
                qaQuery = [
                {"$match":{"_id":{"$in":ids}}},
                 ]
                qaResults = list(qa_db.aggregate(qaQuery))
                if len(qaResults)>0:
                  print("qaResults " + str(len(qaResults)))
                  for result in results:
                     found_objects = list(filter(lambda obj: obj["_id"] == result["_id"], qaResults))
                     if len(found_objects)>0:
                        available=False
                        for student in result["students"]:
                              found_student = list(filter(lambda obj: obj["studentId"] == student["studentId"], found_objects[0]["students"]))
                              if len(found_student)>0:
                                 for attendance in list(filter(lambda obj: obj["attendanceStatus"] == "BLANK", student["studentAttendance"])):
                                       found_attendance = list(filter(lambda obj: obj["date"] == attendance["date"] and obj["classType"] == attendance["classType"] and sorted(obj["periodNumbers"]) == sorted(attendance["periodNumbers"]), found_student[0]["studentAttendance"]))
                                       if len(found_attendance)>0 and found_attendance[0]["attendanceStatus"]!="BLANK":
                                          attendance["attendanceStatus"]=found_attendance[0]["attendanceStatus"]
                                          available=True
                              student["totalNumberOfClasses"]=len(list(filter(lambda attn: attn["attendanceStatus"]=="PRESENT" or attn["attendanceStatus"]=="ABSENT", student["studentAttendance"])))
                              student["presentCount"]=len(list(filter(lambda attn: attn["attendanceStatus"]=="PRESENT", student["studentAttendance"])))
                              student["absentCount"]=len(list(filter(lambda attn: attn["attendanceStatus"]=="ABSENT", student["studentAttendance"])))
                              student["percentage"]=(student["presentCount"]/student["totalNumberOfClasses"] * 100.0) if student["totalNumberOfClasses"] > 0  else 0
                        if available:
                           count=count+1
                           # prodDB.update({"_id":result["_id"]},{"$set":{"students":result["students"]}})
                if count>0: 
                 print(dbName + "Data Foound  ---> "+ date_str + "  ---> "+str(count))
                 subprocess.run(f"echo {dbName} Data Foound  ---> {date_str} ---> {str(count)} >> log.txt",check=True)