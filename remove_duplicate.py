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
          #   print(d['uri'])
             mongoUri='{}/{}'.format(d['uri'].split(',')[0],d['uri'].split(',')[2].split('/')[1].split('?')[0])
             print(mongoUri)
             client = MongoClient(mongoUri)
             dbName='{}'.format(d['uri'].split(',')[2].split('/')[1].split('?')[0])
            #  dbName = d['uri'].split('/')[3]
             data_base = client[dbName]
             print("***********************************************************")
             print(data_base.name)
             collectionsNames=['pms-role','dhi_designation','dhi_usertype','dhi_component_wise_config','dhi_salary_template','dhi_financial_year_configuration'
                               ,'dhi_insurance_configuration','dhi_modeofentry','dhi_tax_configuration','dhi_deduction_schemes','dhi_payroll_leave_config',
                               'dhi_salary_structure_configuration','year-of-passing']
             for collectionName in collectionsNames:
                    print("*********** "+collectionName+" *****************")
                    collection = data_base[collectionName]
                    # Step 1: Identify duplicates
                    pipeline=get_PipeLines(collectionName)
                    duplicates = list(collection.aggregate(pipeline))
                    print(duplicates.__len__())
                    for duplicate in duplicates:
                        documents_to_keep = duplicate['duplicates'][1:]  # Keep all but the first
                        collection.delete_many({'_id': {'$in': documents_to_keep}})
             client.close()


