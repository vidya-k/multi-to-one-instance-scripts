import yaml
# import csv
import os
from pymongo import MongoClient
import bson

stream = open("multi-tenancy.yml", "r")
docs = yaml.load_all(stream, yaml.FullLoader)
# print(docs)
for doc in docs:
         print(doc)
         for d in doc['tenantDataList']:
          #   print(d['uri'])
             mongoUri=d['uri']
            #  mongoUri='{}/{}'.format(d['uri'].split(',')[0],d['uri'].split(',')[2].split('/')[1].split('?')[0])
             print(mongoUri)
             client = MongoClient(mongoUri)
            #  dbName='{}'.format(d['uri'].split(',')[2].split('/')[1].split('?')[0])
             dbName = d['uri'].split('/')[3]
             data_base = client[dbName]
             print("***********************************************************")
             print(data_base.name)
             collectionsNames=data_base.list_collection_names()
             colleges=data_base['college'].find()
             collegeShortName=colleges[0]['shortName']
             print(collegeShortName)
             degrees=data_base['dhi_degree'].find()
             degreeIds=[]
             viewCollections=['system.views','dhi_feedback_view','dhi_internal_assessment_lab_view','dhi_internal_assessment_theory_view','dhi_other_assessment_view','dhi_university_exam_view','dhi_other_assessment_lab_view','dhi_internal_assessment_daily_labscore_view']
             for degree in degrees:
                   degreeIds.append(degree['degreeId'])
             for collectionName in collectionsNames:
               #    print("********** "+ collectionName)
                  if collectionName not in viewCollections :  
                     collection = data_base[collectionName]
                     # reader = csv.reader(open(os.getcwd() + "/yenapoya_degree_details.csv"))
                     for line_elem in degreeIds:
                        oldDegreeId = str(line_elem)
                        # newDegreeId= line_elem
                        newDegreeId= str(oldDegreeId).split(" - "+collegeShortName)[0] + ' - ' + collegeShortName
                        print(oldDegreeId + "  ------->   " + newDegreeId);          
                        upadatedDegreeIds=collection.update_many({'degreeId':oldDegreeId},{"$set": {  "degreeId": newDegreeId}})
                        message1 = 'Collection {} --> updated DegreeId matched count {} , modified count {}'.format(collectionName,upadatedDegreeIds.matched_count,upadatedDegreeIds.modified_count)
                        print(message1)
                        upadatedDegrees=collection.update_many({'degree':oldDegreeId},{"$set": {  "degree": newDegreeId}})
                        message2 = 'Collection {} --> updated Degree count {}, modified count {} '.format(collectionName,upadatedDegrees.matched_count,upadatedDegrees.modified_count)
                        print(message2)
                        if collectionName == 'dhi_user':
                             upadatedEmployees=collection.update_many({'handlingDegreeAndDepartments.degreeId':oldDegreeId},{"$set": {  "handlingDegreeAndDepartments.$.degreeId": newDegreeId}})
                             upadatedEmployees=collection.update_many({'handlingDegreeAndDepartments.concatenatedDegreeAndBatch':oldDegreeId},{"$set": {  "handlingDegreeAndDepartments.$.concatenatedDegreeAndBatch": newDegreeId}})
                             messageemployee = 'Collection {} --> updated Employee {} , {}'.format(collectionName,upadatedEmployees.matched_count,upadatedEmployees.modified_count)
                             print(messageemployee)
                             upadatedStudents=collection.update_many({'yearDetails.degreeId':oldDegreeId},{"$set": {  "yearDetails.$.degreeId": newDegreeId}})
                             messagestudents = 'Collection {} --> updated Student {} , {}'.format(collectionName,upadatedStudents.matched_count,upadatedStudents.modified_count)
                             print(messagestudents)
                        if collectionName == 'dhi_reports_config' or collectionName== 'dhi_student_registration_form_configuration' :
                             updateReportConfig=collection.update_many({"degreeIds":{"$in":[oldDegreeId]}},{"$set":{"degreeIds.$":newDegreeId}})
                        if collectionName == 'dhi_seats_configuration' :
                             updateReportConfig=collection.update_many({"degrees.degreeId":oldDegreeId},{"$set":{"degrees.$.degreeId":newDegreeId}})
                        if collectionName == 'pms_modeofadmission_masterdata' :
                             data= collection.find_one({'_id':oldDegreeId})
                             if data:
                               print(data)
                               data['_id']=newDegreeId
                               collection.find_one_and_delete({'_id':oldDegreeId})
                               collection.insert_one(data)
                        if collectionName == 'dhi_degree' :
                             data= collection.find_one({'degreeId':oldDegreeId})
                             if data:
                               print(data)
                               data['_id']= bson.ObjectId()
                               collection.find_one_and_delete({'degreeId':oldDegreeId})
                               collection.insert_one(data)
