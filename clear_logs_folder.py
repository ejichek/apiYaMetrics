import os
import glob
import json

with open('/home/bdataadmin/airflow/test_script/json_hits_constant.json', 'r', encoding="utf-8") as jsonFile:
    jsonData = json.load(jsonFile)

def clear_logs_folder(reqId_path, partSize_path, bigTxt_path, manyTxt_path):
    
    all_txt_files = glob.glob(os.path.join(manyTxt_path, "*.txt"))
    
    if os.path.exists(reqId_path) == True: 
        os.remove(reqId_path)
    if os.path.exists(partSize_path) == True: 
        os.remove(partSize_path)
    if os.path.exists(bigTxt_path) == True: 
        os.remove(bigTxt_path)
    if len(all_txt_files) > 0:
        for i in all_txt_files:
            os.remove(i)
    print("Успех!")
    return True

clear_logs_folder(reqId_path=jsonData['YaApiHitsConstant']["path_request_id"],
    partSize_path=jsonData['YaApiHitsConstant']["path_parts_and_partsizes_txt"],
    bigTxt_path=jsonData['YaApiHitsConstant']["path_txt_big"],
    manyTxt_path=jsonData['YaApiHitsConstant']["path_many_txt"])