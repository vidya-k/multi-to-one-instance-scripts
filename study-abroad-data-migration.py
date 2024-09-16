import yaml
import csv
import os
from pymongo import MongoClient
import bson
import copy

stream = open("multi-tenancy.yml", "r")
toDBUri="mongodb://wizd%404dew%2A:hu9ba%40ds%26l@10.64.238.29:27017,10.64.238.27:27017,10.64.238.28:27017/dhi_wizdomed_wed?replicaSet=cluster4"
docs = yaml.load_all(stream, yaml.FullLoader)

def getDBConnect(uri):
      mongoUri='{}/{}'.format(uri.split(',')[0],uri.split(',')[2].split('/')[1].split('?')[0])
      #  print(mongoUri)
      client = MongoClient(mongoUri)
      dbName='{}'.format(uri.split(',')[2].split('/')[1].split('?')[0])
      # dbName = uri.split('/')[3]
      data_base = client[dbName]
      return data_base
def createInsiTution(college,degrees):
      intstution={}
      intstution["institutionId"]=college["registrationId"]
      intstution["institutionName"]=college["name"]
      intstution["institutionShortName"]=college["shortName"]
      intstution["degreeDetails"]=list(degrees)
      return intstution

for doc in docs:
         for d in doc['tenantDataList']:
             data_base = getDBConnect(d['uri'])
             to_data_base = getDBConnect(toDBUri)
             print("***********************************************************")
             print(data_base.name)
             collectionsNames=["dhi_degree","dhi_term_detail","dhi_yearwise_section_details","dhi_user","dhi_student_data_configuration"]
             colleges=data_base['college'].find()
             collegeShortName=colleges[0]['shortName']
             print(collegeShortName)
             degrees=list(data_base['dhi_degree'].find({'degreeType':"ACADEMIC"}))
             degreeIds=[]
             viewCollections=['system.views','dhi_feedback_view','dhi_internal_assessment_lab_view',"dhi_mcq_feedback_view",
             'dhi_ue_question_level_tabulation_data_theoryView','dhi_internal_assessment_theory_view','dhi_other_assessment_view',
             'dhi_university_exam_view','dhi_other_assessment_lab_view','dhi_internal_assessment_daily_labscore_view','dhi_ue_question_level_tabulation_data_labView','pms-notification']
             
             for degree in degrees:
               oldDegreeId=degree["degreeId"]
               degree["degreeId"] = degree["degreeId"]+ " - "+collegeShortName;
               newDegreeId=  degree["degreeId"]
               for collectionName in collectionsNames:
                  if collectionName not in viewCollections :  
                        collection = data_base[collectionName]
                        to_collection = to_data_base[collectionName]
                        print(collectionName + "   --------------------> " + oldDegreeId + "  ------->   " + newDegreeId)
                        if collectionName == 'dhi_degree' or collectionName == 'dhi_term_detail' or collectionName == 'dhi_student_data_configuration':
                             data= list(collection.find({'degreeId':oldDegreeId}))
                             newdata= list(to_collection.find({'degreeId':newDegreeId}))
                             if len(newdata)==0 and len(data) > 0:
                               for item in data:
                                item['_id']= bson.ObjectId()
                                item["degreeId"]=newDegreeId
                                to_collection.insert_one(item)            
                        if collectionName == 'dhi_user':
                               users= list(collection.find({"$or":[{"studentStatus": "PASS_OUT"},{"userType":"ALUMNI"}],"degreeId":oldDegreeId})) 
                               if len(users) > 0:
                                for user in users:
                                    newUser= list(to_collection.find({'_id':user["_id"]}))
                                    if len(newUser)==0:
                                     user["degreeId"]=newDegreeId
                                     if "yearDetails" in user and user["yearDetails"] is not None:
                                      for year in user["yearDetails"]:
                                         year["degreeId"]=newDegreeId
                                     to_collection.insert_one(user)
                                            
             institution=createInsiTution(colleges[0],degrees)
             college_collection = to_data_base["college"]
             collegeData=college_collection.find()[0];
             if collegeData["institutionalDetails"]==None:
                   collegeData["institutionalDetails"]=[]
             collegeData["institutionalDetails"].append(institution);
             college_collection.update_one({"_id":collegeData["_id"]},{"$set":{"institutionalDetails":collegeData["institutionalDetails"]}})