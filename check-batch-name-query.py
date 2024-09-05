import yaml
# import csv
import os
from pymongo import MongoClient
import bson
from datetime import datetime
stream = open("multi-tenancy.yml", "r")
docs = yaml.load_all(stream, yaml.FullLoader)
iso_date = datetime(2024, 8, 23, 18, 30)
for doc in docs:
         for d in doc['tenantDataList']:
            #  print(d['uri'])
            #  mongoUri=d['uri']
             mongoUri='{}/{}'.format(d['uri'].split(',')[0],d['uri'].split(',')[2].split('/')[1].split('?')[0])
            #  print(mongoUri)
             client = MongoClient(mongoUri)
             dbName='{}'.format(d['uri'].split(',')[2].split('/')[1].split('?')[0])
            #  dbName = d['uri'].split('/')[3]
             data_base = client[dbName]
             timetableCollection=data_base['dhi_timetable']
             timetablepipeline = [
               {"$match":{"batchName":{"$exists":True},"batchNumber":{"$exists":True}}},
                ]
             enrollments=data_base['dhi_course_enrollment']
             pipeline = [
               {"$match":{"enrolledCourses.enrolledStudents.batchName":{"$exists":True}}},
               {"$unwind":"$enrolledCourses"},
                {"$match":{"enrolledCourses.enrolledStudents.batchName":{"$exists":True}}}
                ]
             fcmDb=data_base['dhi_faculty_course_merge']
             fcmpipeline = [
               {"$match":{"modifiedAt":{"$gt":iso_date},"courseTypeAlias":"THEORY","courseType":"HYBRID"}}
                ]
             assessmentDb=data_base['dhi_assessment']
             assessmentpipeline = [
               {"$match":{"modifiedAt":{"$gt":iso_date},
               "courseTypeAlias":"THEORY","courseType":"HYBRID","assessmentEvaluationParameterConfig.questionsOrExperimentsApplicable":False,
              "studentScores.evaluationStatus" : {"$eq":False}}}
                ]

            #  enrollmentResults = list(enrollments.aggregate(pipeline))
            #  results = list(timetableCollection.aggregate(timetablepipeline))
            #  fcmResults = list(fcmDb.aggregate(fcmpipeline))
             assessmentResults = list(assessmentDb.aggregate(assessmentpipeline))
             if len(assessmentResults) > 0 :
               print(dbName + " ----> " +" Assessment "+ str(len(assessmentResults)))