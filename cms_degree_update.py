import yaml
import csv
import os
from pymongo import MongoClient


         
client = MongoClient('mongodb://snddeldjsorj:Mdjso39d*$dhbDZ1@101.53.134.223:47118/dhi_dev2')
data_base = client['dhi_dev2']
print("***********************************************************")
print(data_base.name)
collectionsNames=data_base.list_collection_names()
universityId='yenepoyagroup_university'
    
for collectionName in collectionsNames:
                     collection = data_base[collectionName]
                     reader = csv.reader(open(os.getcwd() + "/yenapoya_degree_details.csv"))
                     for line_elem in reader:
                        oldDegreeId= line_elem[0]
                        newDegreeId= line_elem[1]
                        print(oldDegreeId + "  ------->   "+newDegreeId);          
                        upadatedDegreeIds=collection.update_many({'universityId':universityId,'degreeId':oldDegreeId},{"$set": {  "degreeId": newDegreeId}})
                        message1 = 'Collection {} --> updated DegreeId matched count {} , modified count {}'.format(collectionName,upadatedDegreeIds.matched_count,upadatedDegreeIds.modified_count)
                        print(message1)
                        upadatedDegrees=collection.update_many({'universityId':universityId,'degree':oldDegreeId},{"$set": {  "degree": newDegreeId}})
                        message2 = 'Collection {} --> updated Degree count {}, modified count {} '.format(collectionName,upadatedDegrees.matched_count,upadatedDegrees.modified_count)
                        print(message2)
                        if collectionName == 'dhi_university_masterdata':
                             upadatedEmployees=collection.update_many({'universityId':universityId,'degrees.degreeId':oldDegreeId},{"$set": {  "degrees.$.degreeId": newDegreeId}})
                             messageemployee = 'Collection {} --> updated Employee {} , {}'.format(collectionName,upadatedEmployees.matched_count,upadatedEmployees.modified_count)
                             print(messageemployee)
                     
   