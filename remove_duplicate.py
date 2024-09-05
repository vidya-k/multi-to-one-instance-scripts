import yaml
import csv
import os
from pymongo import MongoClient
from duplicate_pipeline import get_PipeLines

#Function Ends
stream = open("multi-tenancy.yml", "r")
docs = yaml.load_all(stream, yaml.FullLoader)
for doc in docs:
         print(doc)
         for d in doc['tenantDataList']:
            #  mongoUri=d['uri']
             mongoUri='{}/{}'.format(d['uri'].split(',')[0],d['uri'].split(',')[2].split('/')[1].split('?')[0])
             print(mongoUri)
             client = MongoClient(mongoUri)
             dbName='{}'.format(d['uri'].split(',')[2].split('/')[1].split('?')[0])
            #  dbName = d['uri'].split('/')[3]
             data_base = client[dbName]
             print("***********************************************************")
             print(data_base.name)
             collectionsNames=["dhi_nomenclature_configuration","dhi_feature",
                               "dhi_labia_parameter","dhi_lab_marks_parameters","dhi_routing_config","dhi_term_detail",
                               "dhi_ia_configuration","dhi_lesson_plan_config","dhi_class_transfer_configuration",
                               "dhi_student_data_configuration","dhi_student_attendance_configuration_refactored"
                               ,"dhi_scheme","dhi_holiday_detail","dhi_master_feedback","dhi_master_feedback_config","dhi_form_configuration"
                                ,"dhi_coursedata"
                               ]
             for collectionName in collectionsNames:
                    print("*********** "+collectionName+" *****************")
                    collection = data_base[collectionName]
                    # Step 1: Identify duplicates
                    pipeline=get_PipeLines(collectionName)
                    duplicates = list(collection.aggregate(pipeline))
                    print(duplicates.__len__())
                    for duplicate in duplicates:
                        if collectionName == 'dhi_coursedata' :
                            updated_data=duplicate['duplicates'][0]
                            print(updated_data)
                            updated_data
                            documents_to_keep = duplicate['duplicates'][1:]
                            print(documents_to_keep)
                            collection.update_one({"_id":updated_data},{"$set":{"departments":duplicate["departments"]}})
                            collection.delete_many({'_id': {'$in': documents_to_keep}})
                        else:
                            documents_to_keep = duplicate['duplicates'][1:]
                            collection.delete_many({'_id': {'$in': documents_to_keep}})
             client.close()

