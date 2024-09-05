import yaml
# import csv
import os
from pymongo import MongoClient
import bson

stream = open("multi.yml", "r")
docs = yaml.load_all(stream, yaml.FullLoader)
for doc in docs:
         for d in doc['tenantDataList']:
            #  print(d['uri'])
             mongoUri=d['uri']
            #  mongoUri='{}/{}'.format(d['uri'].split(',')[0],d['uri'].split(',')[2].split('/')[1].split('?')[0])
            #  print(mongoUri)
             client = MongoClient(mongoUri)
            #  dbName='{}'.format(d['uri'].split(',')[2].split('/')[1].split('?')[0])
             dbName = d['uri'].split('/')[3]
             data_base = client[dbName]
             assessmentCollection=data_base['dhi_assessment']
             pipeline = [
               {"$match":{"courseTypeAlias":"THEORY","internalAssessmentQuestionPaper":{"$exists":True}}},
                {"$group":{"_id":"$internalAssessmentQuestionPaper._id","data":{"$sum":1}}},
                {"$match":{"data":{"$gt":50}}}
                ]
             results = list(assessmentCollection.aggregate(pipeline))
             if len(results)>0:
                print(results)
                print(dbName)