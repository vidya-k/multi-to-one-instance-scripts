import yaml
import csv
import os
from pymongo import MongoClient

stream = open("multi-tenancy.yml", "r")
docs = yaml.load_all(stream, yaml.FullLoader)
print(docs)
for doc in docs:
         print(doc)
         for d in doc['tenantDataList']:
          #   print(d['uri'])
             mongoUri='{}/{}'.format(d['uri'].split(',')[0],d['uri'].split(',')[2].split('/')[1].split('?')[0])
             print(mongoUri)
             client = MongoClient(mongoUri)
             dbName='{}'.format(d['uri'].split(',')[2].split('/')[1].split('?')[0])
          #    data_base = client[d['uri'].split('/')[3]]
             data_base = client[dbName]
             print("***********************************************************")
             print(data_base.name)
             updatingCollectionList=['dhi_faculty_course_merge',
                                  'dhi_student_attendance','dhi_lesson_plan',
                                  'dhi_courseoutcome_info_refactored','dhi_copo_mapping','dhi_lab_sore_data'
                                  ,'dhi_assessment','dhi_class_transfer','dhi_approval_config'
                                  ,'dhi_feedback','dhi_counselling_history','employee_salary'
                                  ,'dhi_performance','dhi_payroll'
                                  ]
             userCollection=data_base['dhi_user']
             pipeline = [
                  {
                       "$match":{"userType":{"$nin":["STUDENT","PARENT","ALUMNI"]}}
                },
                {
                "$group": {
                "_id": "$email",
                "users":{"$push":{"facultyId":"$_id","employeeGivenId":"$employeeGivenId"}}
                          }
                },{
                     "$addFields":{
                          "userCount":{"$size":"$users"}
                     }
                },
                {
                     "$match":{"userCount":{"$gt":1}}
                }
                ]
             results = userCollection.aggregate(pipeline)
             for collectionName in updatingCollectionList: 
                for userData in results:
                        employeeGivenId=userData['users'][0]['employeeGivenId']
                        facultyId=userData['users'][0]['facultyId']
                        print(employeeGivenId + " --------> " +  str(facultyId))
                        collection = data_base[collectionName]
                        if collectionName == 'dhi_faculty_course_merge':
                           dataUpdated= collection.update_many({'mergeCombination.faculties.facultyGivenId':employeeGivenId},{"$set": {  "mergeCombination.faculties.$.facultyId": facultyId}})
                        elif collectionName == 'dhi_timetable':
                           dataUpdated= collection.update_many({'timetable.faculties.facultyGivenId':employeeGivenId},{"$set": {  "timetable.$.faculties.$.facultyId": facultyId}})
                        elif collectionName == 'dhi_courseoutcome_info_refactored':
                            dataUpdated= collection.update_many({'facultiesList.facultyGivenId':employeeGivenId},{"$set": {  "facultiesList.$.facultyId": facultyId}})
                        elif collectionName == 'dhi_class_transfer':
                            dataUpdated = collection.update_many({'transfereeGivenId':employeeGivenId},{"$set": {  "transfereeId": facultyId}})
                            dataUpdated = collection.update_many({'transfererGivenId':employeeGivenId},{"$set": {  "transfererId": facultyId}})
                        elif collectionName == 'dhi_approval_config':
                            dataUpdated= collection.update_many({'approvers.employeeGivenId':employeeGivenId},{"$set": {  "approvers.$.approverId": facultyId}})
                        else :
                            dataUpdated = collection.update_many({'faculties.employeeGivenId':employeeGivenId},{"$set": {  "faculties.$.facultyId": facultyId}})
                            dataUpdated = collection.update_many({'approvers.employeeGivenId':employeeGivenId},{"$set": {  "approvers.$.approverId": facultyId}})
                        print(collectionName + " ----> "+employeeGivenId)
                        for faculty in userData['users']:
                            print(faculty['facultyId'])
                            if str(facultyId) != str(faculty['facultyId']) :
                                 userCollection.delete_one({"_id":faculty['facultyId']})
