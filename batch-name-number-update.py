import yaml
import csv
import os
from pymongo import MongoClient
import bson
import copy

stream = open("multi.yml", "r")
docs = yaml.load_all(stream, yaml.FullLoader)
# print(docs)
for doc in docs:
         for d in doc['tenantDataList']:
          #   print(d['uri'])
             mongoUri=d['uri']
            #  mongoUri='{}/{}'.format(d['uri'].split(',')[0],d['uri'].split(',')[2].split('/')[1].split('?')[0])
            #  print(mongoUri)
             client = MongoClient(mongoUri)
            #  dbName='{}'.format(d['uri'].split(',')[2].split('/')[1].split('?')[0])
             dbName = d['uri'].split('/')[3]
             data_base = client[dbName]
             print("***********************************************************")
             print(data_base.name)
             collectionsNames=["dhi_course_enrollment","dhi_timetable","dhi_faculty_course_merge"
                               ,"dhi_feedback","dhi_courseoutcome_info_refactored","dhi_copo_mapping","dhi_assessment"]
             for collectionName in collectionsNames:
                        print(collectionName)
                        collection = data_base[collectionName]
                        if collectionName == 'dhi_course_enrollment':
                             upadatedEnrollments=collection.find({'enrolledCourses.enrolledStudents.batchName':{"$ne":None}})
                             for enroll in upadatedEnrollments:
                                   for course in enroll["enrolledCourses"]:
                                         for student in course["enrolledStudents"]:
                                               if student["batchName"]==None:
                                                     student["batchNumber"]=student["batchName"]
                                                     student["batchName"]=None
                                   collection.find_one_and_delete({'_id':enroll["_id"]})
                                   collection.insert_one(enroll)                 
                        if collectionName == 'dhi_timetable':
                             updateReportConfig=collection.update_many({"batchName":"1"},{"$set":{"batchNumber":1,"batchName":None}})
                             updateReportConfig=collection.update_many({"batchName":"2"},{"$set":{"batchNumber":2,"batchName":None}})
                             updateReportConfig=collection.update_many({"batchName":"3"},{"$set":{"batchNumber":3,"batchName":None}})
                             message1 = 'Collection {} --> updated batchName matched count {} , modified count {}'.format(collectionName,updateReportConfig.matched_count,updateReportConfig.modified_count)
                             print(message1)
                        if collectionName == 'dhi_faculty_course_merge' :
                            updateReportConfig=collection.update_many({"mergeCombination.departments.batchName":"1"}
                                                                       ,{"$set":{"mergeCombination.departments.$.batchNumber":1,"mergeCombination.departments.$.batchName":None
                                                                                 ,"mergeCombination.students.$.batchNumber":1,"mergeCombination.students.$.batchName":None}})
                            message1 = 'Collection {} --> updated batchName matched count {} , modified count {}'.format(collectionName,updateReportConfig.matched_count,updateReportConfig.modified_count)
                            updateReportConfig=collection.update_many({"mergeCombination.departments.batchName":"2"}
                                                                       ,{"$set":{"mergeCombination.departments.$.batchNumber":2,"mergeCombination.departments.$.batchName":None,"mergeCombination.students.$.batchNumber":2,"mergeCombination.students.$.batchName":None}})
                            updateReportConfig=collection.update_many({"mergeCombination.departments.batchName":"3"}
                                                                       ,{"$set":{"mergeCombination.departments.$.batchNumber":3,"mergeCombination.departments.$.batchName":None,"mergeCombination.students.$.batchNumber":3,"mergeCombination.students.$.batchName":None}})
                            print(message1)
                        if collectionName == 'dhi_courseoutcome_info_refactored' or "dhi_copo_mapping" or "dhi_feedback":
                            updateReportConfig=collection.update_many({"departments.batchName":"1"} ,{"$set":{"departments.$.batchNumber":1,"departments.$.batchName":None}})
                            message1 = 'Collection {} --> updated batchName matched count {} , modified count {}'.format(collectionName,updateReportConfig.matched_count,updateReportConfig.modified_count)
                            updateReportConfig=collection.update_many({"departments.batchName":"2"},{"$set":{"departments.$.batchNumber":2,"departments.$.batchName":None}})
                            updateReportConfig=collection.update_many({"departments.batchName":"3"}
                                                                       ,{"$set":{"departments.$.batchNumber":3,"departments.$.batchName":None}})
                            print(message1)
                        if collectionName == 'dhi_assessment':
                            updateReportConfig=collection.update_many({"departments.batchName":"1"}
                                                                       ,{"$set":{"departments.$.batchNumber":1,"departments.$.batchName":None
                                                                                 }})
                            updateReportConfig=collection.update_many({"studentScores.batchName":"1"}
                                                                       ,{"$set":{"studentScores.$.batchNumber":1,"studentScores.$.batchName":None}})
                            message1 = 'Collection {} --> updated batchName matched count {} , modified count {}'.format(collectionName,updateReportConfig.matched_count,updateReportConfig.modified_count)

                            updateReportConfig=collection.update_many({"studentScores.batchName":"2"}
                                                                       ,{"$set":{"studentScores.$.batchNumber":2,"studentScores.$.batchName":None}})
                            updateReportConfig=collection.update_many({"studentScores.batchName":"3"}
                                                                       ,{"$set":{"studentScores.$.batchNumber":3,"studentScores.$.batchName":None}})
                            message1 = 'Collection {} --> updated batchName matched count {} , modified count {}'.format(collectionName,updateReportConfig.matched_count,updateReportConfig.modified_count)
                            updateReportConfig=collection.update_many({"departments.batchName":"2"}
                                                                       ,{"$set":{"departments.$.batchNumber":2,"departments.$.batchName":None}})
                            updateReportConfig=collection.update_many({"departments.batchName":"3"}
                                                                       ,{"$set":{"departments.$.batchNumber":3,"departments.$.batchName":None}})    
                               
