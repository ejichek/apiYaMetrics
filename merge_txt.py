import os
import json
import glob

with open('/home/bdataadmin/airflow/test_script/json_hits_constant.json', 'r', encoding="utf-8") as jsonFile:
    jsonData = json.load(jsonFile)

def merge_txt(path_txt_many, path_txt_big):

    all_txt_files = glob.glob(os.path.join(path_txt_many, "*.txt"))

    with open(all_txt_files[0], 'r', encoding='utf-8') as f:
        data_zagolovok = f.read().splitlines(True)
        
        with open(path_txt_big, 'w', encoding='utf-8') as f1:
            f1 = f1.write(data_zagolovok[0].replace("ym:pv:", "").replace("ym:s:", ""))

    for i in all_txt_files:
        
        with open(i, 'r', encoding='utf-8') as f2:
            data = f2.read().splitlines(True)
        
            with open(path_txt_big, 'a', encoding='utf-8') as f_final:
                f_final.writelines(data[1:])  

merge_txt(path_txt_many=jsonData["YaApiHitsConstant"]["path_many_txt"],
    path_txt_big=jsonData["YaApiHitsConstant"]["path_txt_big"])
