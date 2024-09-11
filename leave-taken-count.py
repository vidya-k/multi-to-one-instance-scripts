import yaml
import csv
import os
from pymongo import MongoClient
from duplicate_pipeline import get_PipeLines

#Function Ends
stream = open("multi.yml", "r")
docs = yaml.load_all(stream, yaml.FullLoader)
for doc in docs:
         print(doc)
         for d in doc['tenantDataList']:
             mongoUri=d['uri']
            #  mongoUri='{}/{}'.format(d['uri'].split(',')[0],d['uri'].split(',')[2].split('/')[1].split('?')[0])
             print(mongoUri)
             client = MongoClient(mongoUri)
            #  dbName='{}'.format(d['uri'].split(',')[2].split('/')[1].split('?')[0])
             dbName = d['uri'].split('/')[3]
             data_base = client[dbName]
             print("***********************************************************")
             print(data_base.name)
             collection = data_base["dhi_user_leaves"]
             data= collection.find({"year":"2024","employeeGivenId":{"$in":["AER949","AER884","AIML944","AIML968","AIML897","AIML890","AIML925","BCSCH941","BCSCH903","BCSCH875","CSE965","CSE926","CSE802","CSE961","CSE914","CSE915","CSE946","CSE900","IOT920","IOT918","IOT930","IOT937","ECE954","ECE945","ECE916","ECE936","ECE931","ECE917","ISE953","ISE958","ISE967","BCSMT966","BCSMT962","BCSMT964","BCSMT912","MEC922"
             ,"MEC896","MEC921","MEC947","mec940","MTR942","MTR902","MTR867","MTR871","MTR911","ADM910","LIB935"]}})
             for d in data:
                for category in d["categoryDetail"]:
                     category["takenCount"]=0
                for leave in d["leaves"]:
                    count=0
                    if leave["status"] == "APPROVED":
                        count=count+leave["noOfDays"]
                        if leave["cancelledLeaves"]!= None and len(leave["cancelledLeaves"])>0:
                            cancelLeaveCount=0
                            for cancelledLeave in leave["cancelledLeaves"]:
                                if cancelledLeave["status"] == "APPROVED":
                                    cancelLeaveCount=cancelLeaveCount+cancelledLeave["noOfDays"]
                        count=count-cancelLeaveCount
                        # print(leave["leaveCategory"])
                        # print(count)        
                    for category in d["categoryDetail"]:
                        if category["categoryName"] == leave["leaveCategory"]:
                            category["takenCount"]=category["takenCount"]+count
                for category in d["categoryDetail"]:
                     print(category["categoryName"])
                     print(category["takenCount"])
                print(d["_id"])     
                collection.update_one({"_id":d["_id"]},{"$set":{"categoryDetail":d["categoryDetail"]}})        
