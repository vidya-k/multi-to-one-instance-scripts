import yaml
import csv
import os
from pymongo import MongoClient
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('pymongo')
logger.setLevel(logging.DEBUG)

stream = open("multi-tenancy.yml", "r")
docs = yaml.load_all(stream, yaml.FullLoader)
for doc in docs:
         print(doc)
         for d in doc['tenantDataList']:
            #  print(d['uri'])
            #  mongoUri=d['uri']
             mongoUri='{}/{}'.format(d['uri'].split(',')[0],d['uri'].split(',')[2].split('/')[1].split('?')[0])
             print(mongoUri)
             client = MongoClient(mongoUri)
             dbName='{}'.format(d['uri'].split(',')[2].split('/')[1].split('?')[0])
            #  dbName = d['uri'].split('/')[3]
             data_base = client[dbName]
             print("***********************************************************")
             print(data_base.name)
             updatingCollectionList=['dhi_faculty_course_merge','dhi_student_attendance',
                                  'dhi_lesson_plan',
                                  'dhi_courseoutcome_info_refactored','dhi_copo_mapping','dhi_lab_sore_data'
                                  ,'dhi_assessment','dhi_class_transfer','dhi_approval_config'
                                  ,'dhi_feedback','dhi_counselling_history','employee_salary'
                                  ,'dhi_performance','dhi_payroll'
                                  ]
             userCollection=data_base['dhi_user']
             pipeline = [
                  {
                       "$match":{"employeeGivenId":{"$exists":True},"userType":{"$nin":["STUDENT","PARENT","ALUMNI","APPLICANT"]}}
                },
                {
                "$group": {
                "_id": "$employeeGivenId",
                "users":{"$push":{"facultyId":"$_id","employeeGivenId":"$employeeGivenId"}}
                          }
                },{
                     "$addFields":{
                          "userCount":{"$size":"$users"}
                     }
                }
                ]
             results = list(userCollection.aggregate(pipeline))
             for collectionName in updatingCollectionList: 
                for userData in results:
                        employeeGivenId=userData['users'][0]['employeeGivenId']
                        facultyId=str(userData['users'][0]['facultyId'])
                        print(employeeGivenId + " --------> " +  facultyId)
                        collection = data_base[collectionName]
                        if collectionName == 'dhi_faculty_course_merge':
                           dataUpdated= collection.update_many({'mergeCombination.faculties.facultyGivenId':employeeGivenId},{"$set": {  "mergeCombination.faculties.$.facultyId": facultyId}})
                           message1 = 'Collection {} --> updated DegreeId matched count {} , modified count {}'.format(collectionName,dataUpdated.matched_count,dataUpdated.modified_count)
                        elif collectionName == 'dhi_courseoutcome_info_refactored':
                           dataUpdated= collection.update_many({'facultiesList.facultyGivenId':employeeGivenId},{"$set": {  "facultiesList.$.facultyId": facultyId}})
                           message1 = 'Collection {} --> updated DegreeId matched count {} , modified count {}'.format(collectionName,dataUpdated.matched_count,dataUpdated.modified_count)
                        elif collectionName == 'dhi_lesson_plan' or collectionName == 'dhi_assessment' or collectionName == 'dhi_copo_mapping' or collectionName == 'dhi_lab_score_data':
                           dataUpdated= collection.update_many({'faculties.facultyGivenId':employeeGivenId},{"$set": {  "faculties.$.facultyId": facultyId}})
                           message1 = 'Collection {} --> updated DegreeId matched count {} , modified count {}'.format(collectionName,dataUpdated.matched_count,dataUpdated.modified_count)   
                        elif collectionName == 'dhi_class_transfer':
                           dataUpdated = collection.update_many({'transfereeGivenId':employeeGivenId},{"$set": {  "transfereeId": facultyId}})
                           message1 = 'Collection {} --> updated DegreeId matched count {} , modified count {}'.format(collectionName,dataUpdated.matched_count,dataUpdated.modified_count)
                        elif collectionName == 'dhi_approval_config':
                           dataUpdated= collection.update_many({'approvers.employeeGivenId':employeeGivenId},{"$set": {  "approvers.$.approverId": facultyId}})
                           message1 = 'Collection {} --> updated DegreeId matched count {} , modified count {}'.format(collectionName,dataUpdated.matched_count,dataUpdated.modified_count)
                        else :
                           dataUpdated = collection.update_many({'faculties.employeeGivenId':employeeGivenId},{"$set": {  "faculties.$.facultyId": facultyId}})
                           message1 = 'Collection {} --> updated DegreeId matched count {} , modified count {}'.format(collectionName,dataUpdated.matched_count,dataUpdated.modified_count)
                           dataUpdated = collection.update_many({'approvers.employeeGivenId':employeeGivenId},{"$set": {  "approvers.$.approverId": facultyId}})
                           message1 = 'Collection {} --> updated DegreeId matched count {} , modified count {}'.format(collectionName,dataUpdated.matched_count,dataUpdated.modified_count)
                        print(collectionName)
                        print(message1)
