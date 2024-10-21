import os
import sys
import json
import requests
from datetime import datetime, date, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.python import PythonSensor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

# Константы, которые могут со временем изменится
with open('/tmp/zhalybin/yaApi_logs/json_hits_constant.json', 'r', encoding="utf-8") as jsonFile1:
    constantJsonData = json.load(jsonFile1)

# Даты для выборочной выгрузки в json,
# чтоб править файл на хосте а не репозитории 
with open('/tmp/zhalybin/yaApi_logs/json_hits_select_date.json', 'r', encoding="utf-8") as jsonFile2:
    selectDateJsonData = json.load(jsonFile2)

# Даты для выборочной выгрузки
start_date_v = selectDateJsonData['YaApiHitsDate']['startDate']
end_date_v = selectDateJsonData['YaApiHitsDate']['endDate']

# Праметры выгрузки 
col_tab_Basic = constantJsonData['YaApiHitsConstant']['col_tab_Basic']
col_tab_TrafficSource = constantJsonData['YaApiHitsConstant']['col_tab_TrafficSource']
col_tab_Ecommerce_EventParams_EventType = constantJsonData['YaApiHitsConstant']['col_tab_Ecommerce_EventParams_EventType']
col_tab_Device = constantJsonData['YaApiHitsConstant']['col_tab_Device']

counter_id_v = constantJsonData['YaApiHitsConstant']['counter_id']
API_token_v = constantJsonData['YaApiHitsConstant']['API_token']
headers_v = {'Authorization': f"OAuth {API_token_v}", 'Accept-Encoding': 'gzip'}

# BASH команды которые толкают скрипты на стороне 08 хоста  
bash_clear_tmp = '/usr/bin/python3 /home/bdataadmin/airflow/test_script/clear_logs_folder.py'
bash_clear_hdfs = '/usr/bin/bash /home/bdataadmin/airflow/test_script/сheck_hdfs_txt.sh '

bash_rm_4dir_hdfs = '/usr/bin/bash /home/bdataadmin/airflow/test_script/remove_4dir_hdfs.sh '

bash_command_extract_partsize = '/usr/bin/python3 /home/bdataadmin/airflow/test_script/extract_partsize.py'
bash_download_txt = '/usr/bin/python3 /home/bdataadmin/airflow/test_script/dowload_parts.py'
bash_merge_txt = '/usr/bin/python3 /home/bdataadmin/airflow/test_script/merge_txt.py'
bash_hdfs_put_bigTxt = '/usr/bin/bash hdfs dfs -put /home/bdataadmin/airflow/txt_big/final_txt.txt /user/azhalybin/airflow/test/txt '

bash_start_pySpark_basic_orc = '/usr/bin/bash /home/bdataadmin/airflow/test_script/start_pySpark_hits_basic_orc.sh '
bash_start_pySpark_trafficSource_orc = '/usr/bin/bash /home/bdataadmin/airflow/test_script/start_pySpark_hits_trafficSource_orc.sh '
bash_start_pySpark_3E_orc = '/usr/bin/bash /home/bdataadmin/airflow/test_script/start_pySpark_hits_3E_orc.sh '
bash_start_pySpark_Device_orc = '/usr/bin/bash /home/bdataadmin/airflow/test_script/start_pySpark_hits_device_orc.sh '
bash_start_pySpark_join = '/usr/bin/bash /home/bdataadmin/airflow/test_script/start_pySpark_hits_join.sh '

# Постоянные пути для файлов необходимых для работы DAG-а
# на стороне airflow хста  
path_request_id_v = '/tmp/zhalybin/yaApi_logs/request_id.txt'
path_faild_v = '/tmp/zhalybin/yaApi_logs/hits_faild.txt'


def read_request_id(path_request_id):
    """
    Функция нужна чтоб читать сохраненный request_id,
    она читает строку в файле который регулярно обновляется
    """
    with open(path_request_id, "r") as f:
        request_id = f.read()
    return request_id

# Записывем в переменную индентификатор выгрузки запроса
# из файла на хосте airflow - потому что могу и xcom не подходит
request_id_v = read_request_id(path_request_id=path_request_id_v)
# Дублируем индентификатор выгрузки запроса на 08 хосте
bash_command_request_id = f'echo "{request_id_v}" > /home/bdataadmin/airflow/logs/request_id.txt'


def ocenka_vozmozhnisti_sozd_zaprosa(counter_id, API_token, headers, start_date, end_date, col_tab):
    """
    Оценка возможности создания запроса на стороне ЯМ,
    функция шлет запрос с параметрами выгрузки которые нас интересуют
    и мы ожидаем ответ r.json()['log_request_evaluation']['possible'] == True
    """
    get_ocenka = f"https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequests/evaluate"

    params_ocenka = {
        'date1': start_date,
        'date2': end_date,
        'fields': col_tab,
        'source': 'hits'  # hits visits
    }

    r = requests.get(get_ocenka, params=params_ocenka, headers=headers)

    if r.status_code == 200:
        if r.json()['log_request_evaluation']['possible'] == True:
            print('Выгрузка возможна')
        elif r.json()['log_request_evaluation']['possible'] == False:
            print('YaApi ответил что выгрузка невозможна ' + str(r.status_code))
            return False
        else:
            print('Что-то пошло не так' + str(r.status_code))
            return False
    else:
        sys.exit('Запрос упал: ' + str(r.status_code))
        return False


def sozd_zaprosa_logov(counter_id, API_token, headers, start_date, end_date, col_tab, path_request_id):
    """
    Исходя из оценки возможности создания запроса
    мы отпрпавляем запрос на формирование выгрузки. 
    Похожий скрипт на стороне 08 хоста скачивает и
    записывает в файл множество пар знчений:
    номер части выгрузки (parts) и размер этой части (partSizes) 
    """
    post_sozdanie_zaprosa_logov = f"https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequests"

    params_sozdanie_zaprosa_logov = {
        'date1': start_date,
        'date2': end_date,
        'fields': col_tab,
        'source': 'hits'  # hits visits
    }

    r = requests.post(post_sozdanie_zaprosa_logov, params=params_sozdanie_zaprosa_logov, headers=headers)

    if os.path.exists(path_request_id) == True:
        os.remove(path_request_id)

    if r.status_code == 200:
        if r.json()['log_request']['status'] == 'created':
            print('Запрос создаётся')
            # print(r.json()['log_request']['request_id'])
            with open(path_request_id, 'w', encoding='utf-8') as f:
                f.write(str(r.json()['log_request']['request_id']))
        else:
            sys.exit('Что-то пошло не так' + str(r.status_code))
            return False
    else:
        sys.exit('Запрос упал: ' + str(r.status_code))
        return False


def get_status(counter_id, API_token, headers, request_id):
    """
    После создния запроса на стороне ЯМ, самому ЯМ 
    требуется время на формирование выгрузки
    и эта функция запрашивает информацию о том
    готов ли к выгрузке наш запрос
    """

    get_status = f"https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequest/{request_id}"

    r = requests.get(get_status, headers=headers)

    if r.status_code == 200:
        if r.json()['log_request']['status'] == 'processed':
            print('Успех')
            return True
        elif r.json()['log_request']['status'] == 'created':
            print('Нужно ждать, запрос в статусе created и уже готовится или в очереди на исполнение')
            return False
        else:
            sys.exit('Что-то пошло не так' + str(r.status_code))
            return False
    else:
        sys.exit('Запрос упал: ' + str(r.status_code))
        return False


def clear_YaApi_query(counter_id, API_token, headers, request_id):
    """
    После успешной выгрузки всех частей запроса 
    и проверки их на полноту (на строне 08 хоста)
    мы очищаем наше хранилище (в котором 10 Гб
    в бесплатной версии) для последующих выгрузок   
    """
    post_clean = f"https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequest/{request_id}/clean"
    r = requests.post(post_clean, headers=headers)
    if r.status_code == 200:
        if r.json()['log_request']['status'] == 'cleaned_by_user':
            print('Данные Яндекс метрики успешно очищены на стороне Яндекса')
        else:
            sys.exit('Что-то пошло не так' + str(r.status_code))
            return False
    else:
        sys.exit('Запрос упал: ' + str(r.status_code))
        return False


# DAG упал 🛑
# DAG отработал успешно ✅
def tlgrm(message):
    """
    Функция через телеграмм бот шлет в телеграмм беседу
    отчёты об успешности или не успешности выполнения DAG-а.
    Беседа называется: AFLT_BigDataReports
    """
    token = "7437864886:AAGbHErZlCVqycGfYhkrB14iYevybpqErjc"
    url = "https://api.telegram.org/bot"
    channel_id = "-1002335777312"
    url += token
    method = url + "/sendMessage"
    #    proxy = {'https': '192.168.41.118:1080'}
    #    proxy = {'https': 'http://192.168.41.118:1080'}

    try:
        r = requests.post(method,
                          data={
                              "chat_id": channel_id,
                              "text": message},
                          #                          proxies=proxy,
                          timeout=30)
        print(r.json())
    except requests.exceptions.ConnectionError:
        print("Искл_1")
        return False


def logging_faild(text, path_faild):
    """
    Записываем в файл факт неуспеха
    и индентификатор выгрузки запроса
    для чтоб очитить его вручную, если потребуется
    """
    with open(path_faild, 'a', encoding='utf-8') as f:
        f.write(text + '\n')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 25),
    'retries': 0
}

# В DAG-е реализована логика при которой выгрузка делится на 4 части
# и join-тся на финальном этапе с отравкой сообщений телеграм репортер.
# Если на каком-то узле DAG-а что-то пошло не так - считаем что выгрузка неуспешна
# и для данной даты нужно начинать все сначала. 
# Для визитов и хитов существуют пары DAG-ов: 1)с выбором даты для выгрузки
# 2) и поставленном на расписание - котррый лучше не трогать.
with DAG(dag_id='BigData_YaApi_hits_date_selection',
         tags=["ETL для разовых выгрузок"],
         default_args=default_args,
         schedule_interval='@once',
         start_date=days_ago(1)
         ) as dag:

    task_clear_tmp_1 = SSHOperator(
        task_id='task_clear_tmp_1',
        ssh_conn_id="ssh_08",
        command=bash_clear_tmp,
        dag=dag
    )

    task_clear_hdfs_1 = SSHOperator(
        task_id='task_clear_hdfs_1',
        ssh_conn_id="ssh_08",
        command=bash_clear_hdfs,
        dag=dag
    )

    task_rm_4dir_hdfs_1 = SSHOperator(
        task_id='task_rm_4dir_hdfs_1',
        ssh_conn_id="ssh_08",
        command=bash_rm_4dir_hdfs,
        dag=dag
    )

    ocenka_vozmozhnisti_sozd_zaprosa_1 = PythonOperator(
        task_id='ocenka_vozmozhnisti_sozd_zaprosa_1',
        python_callable=ocenka_vozmozhnisti_sozd_zaprosa,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Basic}
    )

    sozd_zaprosa_logov_1 = PythonOperator(
        task_id='sozd_zaprosa_logov_1',
        python_callable=sozd_zaprosa_logov,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Basic,
            'path_request_id': path_request_id_v}
    )

    get_status_1 = PythonSensor(
        task_id='get_status_1',
        python_callable=get_status,
        poke_interval=5 * 60,  # каждые 5 минут, Через какое время перезапускаться (5 * 60)
        timeout=12 * 60 * 60,  # время до принудительного падения, 12 часов
        # retries=720,
        soft_fail=False,
        mode="reschedule",
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'request_id': read_request_id(path_request_id=path_request_id_v)}
    )

    task_save_request_id_1 = SSHOperator(
        task_id='task_save_request_id_1',
        ssh_conn_id="ssh_08",
        command=bash_command_request_id,
        dag=dag
    )

    task_extract_partsize_1 = SSHOperator(
        task_id='task_extract_partsize_1',
        ssh_conn_id="ssh_08",
        command=bash_command_extract_partsize,
        dag=dag
    )

    task_download_txt_1 = SSHOperator(
        task_id='task_download_txt_1',
        ssh_conn_id="ssh_08",
        command=bash_download_txt,
        dag=dag
    )

    task_merge_txt_1 = SSHOperator(
        task_id='task_merge_txt_1',
        ssh_conn_id="ssh_08",
        command=bash_merge_txt,
        dag=dag
    )

    clear_YaApi_query_1 = PythonOperator(
        task_id='clear_YaApi_query_1',
        python_callable=clear_YaApi_query,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'request_id': read_request_id(path_request_id=path_request_id_v)}
    )

    task_move_bigTxt_1 = SSHOperator(
        task_id='task_move_bigTxt_1',
        ssh_conn_id="ssh_08",
        command=bash_hdfs_put_bigTxt,
        dag=dag
    )

    task_start_pySpark_Basic_orc = SSHOperator(
        task_id='task_start_pySpark_Basic_orc',
        ssh_conn_id="ssh_08",
        command=bash_start_pySpark_basic_orc,
        dag=dag
    )

    task_clear_tmp_2 = SSHOperator(
        task_id='task_clear_tmp_2',
        ssh_conn_id="ssh_08",
        command=bash_clear_tmp,
        dag=dag
    )

    task_clear_hdfs_2 = SSHOperator(
        task_id='task_clear_hdfs_2',
        ssh_conn_id="ssh_08",
        command=bash_clear_hdfs,
        dag=dag
    )

    ocenka_vozmozhnisti_sozd_zaprosa_2 = PythonOperator(
        task_id='ocenka_vozmozhnisti_sozd_zaprosa_2',
        python_callable=ocenka_vozmozhnisti_sozd_zaprosa,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_TrafficSource}
    )

    sozd_zaprosa_logov_2 = PythonOperator(
        task_id='sozd_zaprosa_logov_2',
        python_callable=sozd_zaprosa_logov,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_TrafficSource,
            'path_request_id': path_request_id_v}
    )

    get_status_2 = PythonSensor(
        task_id='get_status_2',
        python_callable=get_status,
        poke_interval=5 * 60,  # каждые 5 минут, Через какое время перезапускаться (5 * 60)
        timeout=12 * 60 * 60,  # время до принудительного падения, 12 часов
        # retries=720,
        soft_fail=False,
        mode="reschedule",
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'request_id': read_request_id(path_request_id=path_request_id_v)}
    )

    task_save_request_id_2 = SSHOperator(
        task_id='task_save_request_id_2',
        ssh_conn_id="ssh_08",
        command=bash_command_request_id,
        dag=dag
    )

    task_extract_partsize_2 = SSHOperator(
        task_id='task_extract_partsize_2',
        ssh_conn_id="ssh_08",
        command=bash_command_extract_partsize,
        dag=dag
    )

    task_download_txt_2 = SSHOperator(
        task_id='task_download_txt_2',
        ssh_conn_id="ssh_08",
        command=bash_download_txt,
        dag=dag
    )

    task_merge_txt_2 = SSHOperator(
        task_id='task_merge_txt_2',
        ssh_conn_id="ssh_08",
        command=bash_merge_txt,
        dag=dag
    )

    clear_YaApi_query_2 = PythonOperator(
        task_id='clear_YaApi_query_2',
        python_callable=clear_YaApi_query,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'request_id': read_request_id(path_request_id=path_request_id_v)}
    )

    task_move_bigTxt_2 = SSHOperator(
        task_id='task_move_bigTxt_2',
        ssh_conn_id="ssh_08",
        command=bash_hdfs_put_bigTxt,
        dag=dag
    )

    task_start_pySpark_TrafficSource_orc = SSHOperator(
        task_id='task_start_pySpark_TrafficSource_orc',
        ssh_conn_id="ssh_08",
        command=bash_start_pySpark_trafficSource_orc,
        dag=dag
    )

    task_clear_tmp_3 = SSHOperator(
        task_id='task_clear_tmp_3',
        ssh_conn_id="ssh_08",
        command=bash_clear_tmp,
        dag=dag
    )

    task_clear_hdfs_3 = SSHOperator(
        task_id='task_clear_hdfs_3',
        ssh_conn_id="ssh_08",
        command=bash_clear_hdfs,
        dag=dag
    )

    ocenka_vozmozhnisti_sozd_zaprosa_3 = PythonOperator(
        task_id='ocenka_vozmozhnisti_sozd_zaprosa_3',
        python_callable=ocenka_vozmozhnisti_sozd_zaprosa,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Ecommerce_EventParams_EventType}
    )

    sozd_zaprosa_logov_3 = PythonOperator(
        task_id='sozd_zaprosa_logov_3',
        python_callable=sozd_zaprosa_logov,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Ecommerce_EventParams_EventType,
            'path_request_id': path_request_id_v}
    )

    get_status_3 = PythonSensor(
        task_id='get_status_3',
        python_callable=get_status,
        poke_interval=5 * 60,  # каждые 5 минут, Через какое время перезапускаться (5 * 60)
        timeout=12 * 60 * 60,  # время до принудительного падения, 12 часов
        # retries=720,
        soft_fail=False,
        mode="reschedule",
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'request_id': read_request_id(path_request_id=path_request_id_v)}
    )

    task_save_request_id_3 = SSHOperator(
        task_id='task_save_request_id_3',
        ssh_conn_id="ssh_08",
        command=bash_command_request_id,
        dag=dag
    )

    task_extract_partsize_3 = SSHOperator(
        task_id='task_extract_partsize_3',
        ssh_conn_id="ssh_08",
        command=bash_command_extract_partsize,
        dag=dag
    )

    task_download_txt_3 = SSHOperator(
        task_id='task_download_txt_3',
        ssh_conn_id="ssh_08",
        command=bash_download_txt,
        dag=dag
    )

    task_merge_txt_3 = SSHOperator(
        task_id='task_merge_txt_3',
        ssh_conn_id="ssh_08",
        command=bash_merge_txt,
        dag=dag
    )

    clear_YaApi_query_3 = PythonOperator(
        task_id='clear_YaApi_query_3',
        python_callable=clear_YaApi_query,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'request_id': read_request_id(path_request_id=path_request_id_v)}
    )

    task_move_bigTxt_3 = SSHOperator(
        task_id='task_move_bigTxt_3',
        ssh_conn_id="ssh_08",
        command=bash_hdfs_put_bigTxt,
        dag=dag
    )

    task_start_pySpark_3E_orc = SSHOperator(
        task_id='task_start_pySpark_3E_orc',
        ssh_conn_id="ssh_08",
        command=bash_start_pySpark_3E_orc,
        dag=dag
    )

    task_clear_tmp_4 = SSHOperator(
        task_id='task_clear_tmp_4',
        ssh_conn_id="ssh_08",
        command=bash_clear_tmp,
        dag=dag
    )

    task_clear_hdfs_4 = SSHOperator(
        task_id='task_clear_hdfs_4',
        ssh_conn_id="ssh_08",
        command=bash_clear_hdfs,
        dag=dag
    )

    ocenka_vozmozhnisti_sozd_zaprosa_4 = PythonOperator(
        task_id='ocenka_vozmozhnisti_sozd_zaprosa_4',
        python_callable=ocenka_vozmozhnisti_sozd_zaprosa,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Device}
    )

    sozd_zaprosa_logov_4 = PythonOperator(
        task_id='sozd_zaprosa_logov_4',
        python_callable=sozd_zaprosa_logov,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Device,
            'path_request_id': path_request_id_v}
    )

    get_status_4 = PythonSensor(
        task_id='get_status_4',
        python_callable=get_status,
        poke_interval=5 * 60,  # каждые 5 минут, Через какое время перезапускаться (5 * 60)
        timeout=12 * 60 * 60,  # время до принудительного падения, 12 часов
        # retries=720,
        soft_fail=False,
        mode="reschedule",
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'request_id': read_request_id(path_request_id=path_request_id_v)}
    )

    task_save_request_id_4 = SSHOperator(
        task_id='task_save_request_id_4',
        ssh_conn_id="ssh_08",
        command=bash_command_request_id,
        dag=dag
    )

    task_extract_partsize_4 = SSHOperator(
        task_id='task_extract_partsize_4',
        ssh_conn_id="ssh_08",
        command=bash_command_extract_partsize,
        dag=dag
    )

    task_download_txt_4 = SSHOperator(
        task_id='task_download_txt_4',
        ssh_conn_id="ssh_08",
        command=bash_download_txt,
        dag=dag
    )

    task_merge_txt_4 = SSHOperator(
        task_id='task_merge_txt_4',
        ssh_conn_id="ssh_08",
        command=bash_merge_txt,
        dag=dag
    )

    clear_YaApi_query_4 = PythonOperator(
        task_id='clear_YaApi_query_4',
        python_callable=clear_YaApi_query,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'request_id': read_request_id(path_request_id=path_request_id_v)}
    )

    task_move_bigTxt_4 = SSHOperator(
        task_id='task_move_bigTxt_4',
        ssh_conn_id="ssh_08",
        command=bash_hdfs_put_bigTxt,
        dag=dag
    )

    task_start_pySpark_Device_orc = SSHOperator(
        task_id='task_start_pySpark_Device_orc',
        ssh_conn_id="ssh_08",
        command=bash_start_pySpark_Device_orc,
        dag=dag
    )

    task_clear_tmp_5 = SSHOperator(
        task_id='task_clear_tmp_5',
        ssh_conn_id="ssh_08",
        command=bash_clear_tmp,
        dag=dag
    )

    task_clear_hdfs_5 = SSHOperator(
        task_id='task_clear_hdfs_5',
        ssh_conn_id="ssh_08",
        command=bash_clear_hdfs,
        dag=dag
    )

    task_start_pySpark_join = SSHOperator(
        task_id='task_start_pySpark_join',
        ssh_conn_id="ssh_08",
        command=bash_start_pySpark_join,
        dag=dag
    )

    task_rm_4dir_hdfs_2 = SSHOperator(
        task_id='task_rm_4dir_hdfs_2',
        ssh_conn_id="ssh_08",
        command=bash_rm_4dir_hdfs,
        dag=dag
    )

    tlgrm_allSuccess = PythonOperator(
        task_id='tlgrm_allSuccess',
        python_callable=tlgrm,
        dag=dag,
        trigger_rule='all_success',
        op_kwargs={
            'message': f"Отработал успешно ✅ - DAG hits_selectDate_{start_date_v}_{end_date_v}"}
    )

    tlgrm_oneFailed = PythonOperator(
        task_id='tlgrm_oneFailed',
        python_callable=tlgrm,
        dag=dag,
        trigger_rule='one_failed',
        op_kwargs={
            'message': f"Упал 🛑 - DAG hits_selectDate_{request_id_v}_{start_date_v}_{end_date_v}"}
    )

    logging_faild = PythonOperator(
        task_id='logging_faild',
        python_callable=logging_faild,
        dag=dag,
        trigger_rule='one_failed',
        op_kwargs={
            'text': f"DAG hits_selectDate_{request_id_v}_{start_date_v}_{end_date_v} - faild" + "\n",
            'path_faild': path_faild_v}
    )

(task_clear_tmp_1 >> task_clear_hdfs_1 >> task_rm_4dir_hdfs_1 >> ocenka_vozmozhnisti_sozd_zaprosa_1 >> sozd_zaprosa_logov_1 >> get_status_1 >> task_save_request_id_1 >> task_extract_partsize_1 >> task_download_txt_1 >> task_merge_txt_1 >> clear_YaApi_query_1 >> task_move_bigTxt_1 >> task_start_pySpark_Basic_orc >> task_clear_tmp_2 >> task_clear_hdfs_2 >> ocenka_vozmozhnisti_sozd_zaprosa_2 >> sozd_zaprosa_logov_2 >> get_status_2 >> task_save_request_id_2 >> task_extract_partsize_2 >> task_download_txt_2 >> task_merge_txt_2 >> clear_YaApi_query_2 >> task_move_bigTxt_2 >> task_start_pySpark_TrafficSource_orc >> task_clear_tmp_3 >> task_clear_hdfs_3 >> ocenka_vozmozhnisti_sozd_zaprosa_3 >> sozd_zaprosa_logov_3 >> get_status_3 >> task_save_request_id_3 >> task_extract_partsize_3 >> task_download_txt_3 >> task_merge_txt_3 >> clear_YaApi_query_3 >> task_move_bigTxt_3 >> task_start_pySpark_3E_orc >> task_clear_tmp_4 >> task_clear_hdfs_4 >> ocenka_vozmozhnisti_sozd_zaprosa_4 >> sozd_zaprosa_logov_4 >> get_status_4 >> task_save_request_id_4 >> task_extract_partsize_4 >> task_download_txt_4 >> task_merge_txt_4 >> clear_YaApi_query_4 >> task_move_bigTxt_4 >> task_start_pySpark_Device_orc >> task_clear_tmp_5 >> task_clear_hdfs_5 >> task_start_pySpark_join >> task_rm_4dir_hdfs_2 >> [tlgrm_allSuccess, tlgrm_oneFailed, logging_faild])
