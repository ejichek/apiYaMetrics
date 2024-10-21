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

# Функция используется в def get_status для записи "частей" выгрузки и размеров этих частей
def extract_parts_and_partsizes(data, path): 
    if os.path.exists(path) == True: 
        os.remove(path)
    else: 
        print("искл3")
    for i in range(0, len(data)): 
        #print(data[i])
        with open(path, 'a', encoding='utf-8') as f:
            f.write(str(data[i]['part_number']) + '|' + str(data[i]['size']) + '\n')


# Информация о запросе логов.
# Лог со статусом processed готов к выгрузке.
# Если ваш запрос находится в статусе created, дождитесь его выполнения.

def get_status(counter_id, headers, proxy, request_id, path_parts_and_partsizes_txt): 
    get_status = f"https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequest/{request_id}"

    r = requests.get(get_status, headers=headers, proxies=proxy)
    
    if r.status_code == 200: 
        if r.json()['log_request']['status'] == 'processed': 
            print('Нужно выгружать')
            extract_parts_and_partsizes(data=r.json()['log_request']['parts'], path=path_parts_and_partsizes_txt)
            #print(r.json())
        elif r.json()['log_request']['status'] == 'created':
            print('Нужно ждать, запрос в статусе created и уже готовится или в очереди на исполнение')
        else:
            sys.exit('искл2 ' + str(r.status_code))
    else:
        sys.exit("искл1 " + str(r.status_code))

get_status(counter_id=jsonData["YaApiHitsConstant"]["counter_id"],
    headers=headers_v,
    proxy=proxy_v,
    path_parts_and_partsizes_txt=jsonData["YaApiHitsConstant"]["path_parts_and_partsizes_txt"],
    request_id=request_id_v)