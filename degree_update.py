import yaml
import csv
import os
from pymongo import MongoClient

stream = open("multi-tenancy.yml", "r")
docs = yaml.load_all(stream, yaml.FullLoader)
for doc in docs:
         for d in doc['tenantDataList']:
            #  print(d['uri'])
             mongoUri='{}/{}'.format(d['uri'].split(',')[0],d['uri'].split(',')[2].split('/')[1].split('?')[0])
             print(mongoUri)
             client = MongoClient(mongoUri)
             dbName='{}'.format(d['uri'].split(',')[2].split('/')[1].split('?')[0])
            #  data_base = client[d['uri'].split('/')[3]]
             data_base = client[dbName]
             print("***********************************************************")
             print(data_base.name)
             collectionsNames=data_base.list_collection_names()
            #  print(collectionsNames)
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
