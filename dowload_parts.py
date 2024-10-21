import requests
import json
import sys
import os
import re

with open('/home/bdataadmin/airflow/test_script/json_hits_constant.json', 'r', encoding="utf-8") as jsonFile:
    jsonData = json.load(jsonFile)
headers_v = {'Authorization': f"OAuth {jsonData['YaApiHitsConstant']['API_token']}", 'Accept-Encoding': 'gzip'}
proxy_v = {'https': f'{jsonData["YaApiHitsConstant"]["proxy"]}'}

# Функция нужна чтоб читать сохраненный request_id
def read_request_id(path_request_id):
    with open(path_request_id, "r") as f:
        request_id_prom = f.read()
        request_id = re.sub(r"[^a-zA-Z0-9]","",request_id_prom)
    return request_id

request_id_v = read_request_id(path_request_id=jsonData["YaApiHitsConstant"]["path_request_id"])

def dowload_parts(counter_id, headers, proxy, request_id, path_parts_and_partsizes_txt, path_many_txt):
    with open(path_parts_and_partsizes_txt, "r") as f:
        file_parts = f.readlines()
    #print(len(file_parts))
    for i in range(0, len(file_parts)):
        big_str = (file_parts[i].split('|'))
        part_number = str(big_str[0])
        size = int(re.sub('[^A-Za-z0-9]+', '', big_str[1]))
        #print(str(part) + '_' + str(size))

        get_dowload_part = f"https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequest/{request_id}/part/{part_number}/download" 
                                    
        r_dowload_part = requests.get(get_dowload_part, headers=headers, proxies=proxy)

        if r_dowload_part.status_code == 200:
            s1=r_dowload_part.content
            #print(len(s1.decode('utf-8')))

            s=str(r_dowload_part.content,'utf-8')
            #print(len(s.encode('utf-8')))
            
            if len(s.encode('utf-8')) >= size:
                s2 = s1.decode('utf-8')
                path_txt_final = path_many_txt + f'{request_id}_{part_number}.txt'
                #print(path_txt_final)
                with open(path_txt_final, 'w', encoding='utf-8') as f:
                    f.write(s2)
            else:
                sys.exit("искл2 " + str(r_dowload_part.status_code))
        else:
            sys.exit("искл1 " + str(r_dowload_part.status_code))

dowload_parts(counter_id=jsonData["YaApiHitsConstant"]["counter_id"],
    headers=headers_v,
    proxy=proxy_v,
    request_id=request_id_v,
    path_parts_and_partsizes_txt=jsonData["YaApiHitsConstant"]["path_parts_and_partsizes_txt"],
    path_many_txt=jsonData["YaApiHitsConstant"]["path_many_txt"])