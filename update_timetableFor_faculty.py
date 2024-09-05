import yaml
import csv
import os
from pymongo import MongoClient

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
             userCollection=data_base['dhi_user']
             results = list(userCollection.find({"employeeGivenId":{"$exists":True},"userType":{"$nin":["STUDENT","PARENT","ALUMNI","APPLICANT"]}}))
             collection = data_base["dhi_timetable"]
             timetables =list(collection.find({}))
             print(len(timetables))
             for timetable in timetables:
                       for data in timetable['timetable']:
                             for faculty in data['faculties']:
                                    filtersData=list(filter(lambda x: x['employeeGivenId'] == faculty['facultyGivenId'], results))
                                    if len(filtersData)>0:
                                        faculty['facultyId']=str(filtersData[0]['_id'])
                                    
                       collection.delete_one({"_id":timetable['_id']})
                       collection.insert_one(timetable)
                       