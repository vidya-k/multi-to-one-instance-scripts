import yaml
import csv
import os
from pymongo import MongoClient
from duplicate_pipeline import get_PipeLines

#Function Ends
stream = open("multi-tenancy.yml", "r")
docs = yaml.load_all(stream, yaml.FullLoader)
client = MongoClient('mongodb://cms:Mjd8cQ14M@101.53.134.130:47118/dhi_cms')
data_base = client['dhi_cms']
print("***********************************************************")
print(data_base.name)
collectionsNames=["dhi_coursedata"]
universityId='ganpat_university'
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
                            documents_to_keep = duplicate['duplicates'][1:]
                            print(documents_to_keep)
                            collection.update_one({"_id":updated_data},{"$set":{"departments":duplicate["departments"]}})
                            collection.delete_many({'_id': {'$in': documents_to_keep}})
                        else:
                            documents_to_keep = duplicate['duplicates'][1:]
                            collection.delete_many({'_id': {'$in': documents_to_keep}})

