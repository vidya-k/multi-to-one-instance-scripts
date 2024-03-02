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
             print(d['uri'])
             mongoUri='{}/{}'.format(d['uri'].split(',')[0],d['uri'].split(',')[2].split('/')[1].split('?')[0])
             print(mongoUri)
             client = MongoClient(mongoUri)
             dbName='{}'.format(d['uri'].split(',')[2].split('/')[1].split('?')[0])
             data_base = client[dbName]
             print("***********************************************************")
             print(data_base.name)
             collectionsNames=data_base.list_collection_names()
             for collectionName in collectionsNames:
                     collection = data_base[collectionName]
                     reader = csv.reader(open(os.getcwd() + "/yenapoya_degree_details.csv"))
                     for line_elem in reader:
                        oldDegreeId= line_elem[0]
                        newDegreeId= line_elem[1]
                        print(oldDegreeId + "  ------->   "+newDegreeId);          
                        upadatedDegreeIds=collection.update_many({'degreeId':oldDegreeId},{"$set": {  "degreeId": newDegreeId}})
                        message1 = 'Collection {} --> updated DegreeId matched count {} , modified count {}'.format(collectionName,upadatedDegreeIds.matched_count,upadatedDegreeIds.modified_count)
                        print(message1)
                        upadatedDegrees=collection.update_many({'degree':oldDegreeId},{"$set": {  "degree": newDegreeId}})
                        message2 = 'Collection {} --> updated Degree count {}, modified count {} '.format(collectionName,upadatedDegrees.matched_count,upadatedDegrees.modified_count)
                        print(message2)
                        if collectionName == 'dhi_user':
                             upadatedEmployees=collection.update_many({'handlingDegreeAndDepartments.degreeId':oldDegreeId},{"$set": {  "handlingDegreeAndDepartments.$.degreeId": newDegreeId}})
                             messageemployee = 'Collection {} --> updated Employee {} , {}'.format(collectionName,upadatedEmployees.matched_count,upadatedEmployees.modified_count)
                             print(messageemployee)
                             upadatedStudents=collection.update_many({'yearDetails.degreeId':oldDegreeId},{"$set": {  "yearDetails.$.degreeId": newDegreeId}})
                             messagestudents = 'Collection {} --> updated Student {} , {}'.format(collectionName,upadatedStudents.matched_count,upadatedStudents.modified_count)
                             print(messagestudents)
                        if collectionName == 'dhi_reports_config':
                             updateReportConfig=collection.update_many({"degreeIds":{"$in":[oldDegreeId]}},{"$set":{"degreeIds.$":newDegreeId}})
                        if collectionName == 'dhi_university_detail':
                             updatedUniversityDetail=collection.update_many({'degrees.degreeId':oldDegreeId},{"$set": {  "degrees.$.degreeId": newDegreeId}})
                             updatedUniversityDetailmgs = 'Collection {} --> updated Employee {} , {}'.format(collectionName,updatedUniversityDetail.matched_count,updatedUniversityDetail.modified_count)
                             print(updatedUniversityDetailmgs)
   