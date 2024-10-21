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
with open('/tmp/zhalybin/yaApi_logs/json_visits_constant.json', 'r', encoding="utf-8") as jsonFile1:
    constantJsonData = json.load(jsonFile1)

# Рудимент, но пусть тут полежит
with open('/tmp/zhalybin/yaApi_logs/json_visits_select_date.json', 'r', encoding="utf-8") as jsonFile2:
    selectDateJsonData = json.load(jsonFile2)

# Даты для выгрузки поставленной н расписание. 
# 3-4 дня чтоб большая часть сессий клиентов на стороне сайта были завершены
# start_date = end_date тк бесплатное хранилище на стороне ЯМ 10 Гб
start_date_v = str(datetime.date(datetime.today()) - timedelta(days=4))
end_date_v = str(datetime.date(datetime.today()) - timedelta(days=4))

# Праметры выгрузки
col_tab_Basic_Events_1 = constantJsonData['YaApiVisitsConstant']['col_tab_Basic_Events_1']
col_tab_Ecommerce_2 = constantJsonData['YaApiVisitsConstant']['col_tab_Ecommerce_2']
col_tab_Goals_3 = constantJsonData['YaApiVisitsConstant']['col_tab_Goals_3']
col_tab_Device_Source_4 = constantJsonData['YaApiVisitsConstant']['col_tab_Device_Source_4']
col_tab_Attribution_5 = constantJsonData['YaApiVisitsConstant']['col_tab_Attribution_5']
col_tab_Attribution_6 = constantJsonData['YaApiVisitsConstant']['col_tab_Attribution_6']
col_tab_Attribution_7 = constantJsonData['YaApiVisitsConstant']['col_tab_Attribution_7']
col_tab_Attribution_8 = constantJsonData['YaApiVisitsConstant']['col_tab_Attribution_8']
col_tab_Attribution_9 = constantJsonData['YaApiVisitsConstant']['col_tab_Attribution_9']
col_tab_Attribution_10 = constantJsonData['YaApiVisitsConstant']['col_tab_Attribution_10']
col_tab_Attribution_11 = constantJsonData['YaApiVisitsConstant']['col_tab_Attribution_11']
col_tab_Attribution_12 = constantJsonData['YaApiVisitsConstant']['col_tab_Attribution_12']
col_tab_Attribution_13 = constantJsonData['YaApiVisitsConstant']['col_tab_Attribution_13']

counter_id_v = constantJsonData['YaApiVisitsConstant']['counter_id']
API_token_v = constantJsonData['YaApiVisitsConstant']['API_token']
headers_v = {'Authorization': f"OAuth {API_token_v}", 'Accept-Encoding': 'gzip'}

# BASH команды которые толкают скрипты на стороне 08 хоста 
bash_clear_tmp = '/usr/bin/python3 /home/bdataadmin/airflow/test_script/clear_logs_folder.py'
bash_clear_hdfs = '/usr/bin/bash /home/bdataadmin/airflow/test_script/сheck_hdfs_txt.sh '

bash_rm_13dir_hdfs = '/usr/bin/bash /home/bdataadmin/airflow/test_script/remove_13dir_hdfs.sh '

bash_command_extract_partsize = '/usr/bin/python3 /home/bdataadmin/airflow/test_script/extract_partsize.py'
bash_download_txt = '/usr/bin/python3 /home/bdataadmin/airflow/test_script/dowload_parts.py'
bash_merge_txt = '/usr/bin/python3 /home/bdataadmin/airflow/test_script/merge_txt.py'
bash_hdfs_put_bigTxt = '/usr/bin/bash hdfs dfs -put /home/bdataadmin/airflow/txt_big/final_txt.txt /user/azhalybin/airflow/test/txt '

bash_start_pySpark_visits_1_basic_events_orc = '/usr/bin/bash /home/bdataadmin/airflow/test_script/start_pySpark_visits_1_basic_events_orc.sh '
bash_start_pySpark_visits_2_ecommerce_orc = '/usr/bin/bash /home/bdataadmin/airflow/test_script/start_pySpark_visits_2_ecommerce_orc.sh '
bash_start_pySpark_visits_3_goals_orc = '/usr/bin/bash /home/bdataadmin/airflow/test_script/start_pySpark_visits_3_goals_orc.sh '
bash_start_pySpark_visits_4_device_source = '/usr/bin/bash /home/bdataadmin/airflow/test_script/start_pySpark_visits_4_device_source.sh '
bash_start_pySpark_visits_5_attribution = '/usr/bin/bash /home/bdataadmin/airflow/test_script/start_pySpark_visits_5_attribution.sh '
bash_start_pySpark_visits_6_attribution = '/usr/bin/bash /home/bdataadmin/airflow/test_script/start_pySpark_visits_6_attribution.sh '
bash_start_pySpark_visits_7_attribution = '/usr/bin/bash /home/bdataadmin/airflow/test_script/start_pySpark_visits_7_attribution.sh '
bash_start_pySpark_visits_8_attribution = '/usr/bin/bash /home/bdataadmin/airflow/test_script/start_pySpark_visits_8_attribution.sh '
bash_start_pySpark_visits_9_attribution = '/usr/bin/bash /home/bdataadmin/airflow/test_script/start_pySpark_visits_9_attribution.sh '
bash_start_pySpark_visits_10_attribution = '/usr/bin/bash /home/bdataadmin/airflow/test_script/start_pySpark_visits_10_attribution.sh '
bash_start_pySpark_visits_11_attribution = '/usr/bin/bash /home/bdataadmin/airflow/test_script/start_pySpark_visits_11_attribution.sh '
bash_start_pySpark_visits_12_attribution = '/usr/bin/bash /home/bdataadmin/airflow/test_script/start_pySpark_visits_12_attribution.sh '
bash_start_pySpark_visits_13_attribution = '/usr/bin/bash /home/bdataadmin/airflow/test_script/start_pySpark_visits_13_attribution.sh '

bash_start_pySpark_join = '/usr/bin/bash /home/bdataadmin/airflow/test_script/start_pySpark_visits_join.sh '

# Постоянные пути для файлов необходимых для работы DAG-а
# на стороне airflow хста
path_request_id_v = '/tmp/zhalybin/yaApi_logs/request_id.txt'
path_faild_v = '/tmp/zhalybin/yaApi_logs/visits_faild.txt'


# Функция нужна чтоб читать сохраненный request_id
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
        'source': 'visits'  # hits visits
    }

    r = requests.get(get_ocenka, params=params_ocenka, headers=headers)

    if r.status_code == 200:
        if r.json()['log_request_evaluation']['possible'] == True:
            print('Выгрузка возможна')
        elif r.json()['log_request_evaluation']['possible'] == False:
            print('YaApi ответил что выгрузка невозможна' + str(r.status_code))
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
        'source': 'visits'  # hits visits
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
    token = "num:press_your_token"
    url = "https://api.telegram.org/bot"
    channel_id = "-100_channel_id"
    url += token
    method = url + "/sendMessage"
    #    proxy = {'https': 'ip:port'}
    #    proxy = {'https': 'http://ip:port'}

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

# В DAG-е реализована логика при которой выгрузка делится на 13 частей
# и join-тся на финальном этапе с отравкой сообщений телеграм репортер.
# Если на каком-то узле DAG-а что-то пошло не так - считаем что выгрузка неуспешна
# и для данной даты нужно начинать все сначала. 
# Для визитов и хитов существуют пары DAG-ов: 1)с выбором даты для выгрузки
# 2) и поставленном на расписание - котррый лучше не трогать.
with DAG(dag_id='BigData_YaApi_visits_schedule',
         tags=["ETL на расписании"],
         default_args=default_args,
         schedule_interval='30 21 * * * ',                  # каждый день в 21:30 утра, но +3 часа тк UTC, то есть 00:30
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

    task_rm_13dir_hdfs_1 = SSHOperator(
        task_id='task_rm_13dir_hdfs_1',
        ssh_conn_id="ssh_08",
        command=bash_rm_13dir_hdfs,
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
            'col_tab': col_tab_Basic_Events_1}
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
            'col_tab': col_tab_Basic_Events_1,
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

    task_start_pySpark_visits_1_basic_events_orc = SSHOperator(
        task_id='task_start_pySpark_visits_1_basic_events_orc',
        ssh_conn_id="ssh_08",
        command=bash_start_pySpark_visits_1_basic_events_orc,
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
            'col_tab': col_tab_Ecommerce_2}
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
            'col_tab': col_tab_Ecommerce_2,
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

    task_start_pySpark_visits_2_ecommerce_orc = SSHOperator(
        task_id='task_tart_pySpark_visits_2_ecommerce_orc',
        ssh_conn_id="ssh_08",
        command=bash_start_pySpark_visits_2_ecommerce_orc,
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
            'col_tab': col_tab_Goals_3}
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
            'col_tab': col_tab_Goals_3,
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

    task_start_pySpark_visits_3_goals_orc = SSHOperator(
        task_id='task_start_pySpark_visits_3_goals_orc',
        ssh_conn_id="ssh_08",
        command=bash_start_pySpark_visits_3_goals_orc,
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
            'col_tab': col_tab_Device_Source_4}
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
            'col_tab': col_tab_Device_Source_4,
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

    task_start_pySpark_visits_4_device_source = SSHOperator(
        task_id='task_start_pySpark_visits_4_device_source',
        ssh_conn_id="ssh_08",
        command=bash_start_pySpark_visits_4_device_source,
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

    ocenka_vozmozhnisti_sozd_zaprosa_5 = PythonOperator(
        task_id='ocenka_vozmozhnisti_sozd_zaprosa_5',
        python_callable=ocenka_vozmozhnisti_sozd_zaprosa,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Attribution_5}
    )

    sozd_zaprosa_logov_5 = PythonOperator(
        task_id='sozd_zaprosa_logov_5',
        python_callable=sozd_zaprosa_logov,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Attribution_5,
            'path_request_id': path_request_id_v}
    )

    get_status_5 = PythonSensor(
        task_id='get_status_5',
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

    task_save_request_id_5 = SSHOperator(
        task_id='task_save_request_id_5',
        ssh_conn_id="ssh_08",
        command=bash_command_request_id,
        dag=dag
    )

    task_extract_partsize_5 = SSHOperator(
        task_id='task_extract_partsize_5',
        ssh_conn_id="ssh_08",
        command=bash_command_extract_partsize,
        dag=dag
    )

    task_download_txt_5 = SSHOperator(
        task_id='task_download_txt_5',
        ssh_conn_id="ssh_08",
        command=bash_download_txt,
        dag=dag
    )

    task_merge_txt_5 = SSHOperator(
        task_id='task_merge_txt_5',
        ssh_conn_id="ssh_08",
        command=bash_merge_txt,
        dag=dag
    )

    clear_YaApi_query_5 = PythonOperator(
        task_id='clear_YaApi_query_5',
        python_callable=clear_YaApi_query,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'request_id': read_request_id(path_request_id=path_request_id_v)}
    )

    task_move_bigTxt_5 = SSHOperator(
        task_id='task_move_bigTxt_5',
        ssh_conn_id="ssh_08",
        command=bash_hdfs_put_bigTxt,
        dag=dag
    )

    task_start_pySpark_visits_5_attribution = SSHOperator(
        task_id='task_start_pySpark_visits_5_attribution',
        ssh_conn_id="ssh_08",
        command=bash_start_pySpark_visits_5_attribution,
        dag=dag
    )

    task_clear_tmp_6 = SSHOperator(
        task_id='task_clear_tmp_6',
        ssh_conn_id="ssh_08",
        command=bash_clear_tmp,
        dag=dag
    )

    task_clear_hdfs_6 = SSHOperator(
        task_id='task_clear_hdfs_6',
        ssh_conn_id="ssh_08",
        command=bash_clear_hdfs,
        dag=dag
    )

    ocenka_vozmozhnisti_sozd_zaprosa_6 = PythonOperator(
        task_id='ocenka_vozmozhnisti_sozd_zaprosa_6',
        python_callable=ocenka_vozmozhnisti_sozd_zaprosa,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Attribution_6}
    )

    sozd_zaprosa_logov_6 = PythonOperator(
        task_id='sozd_zaprosa_logov_6',
        python_callable=sozd_zaprosa_logov,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Attribution_6,
            'path_request_id': path_request_id_v}
    )

    get_status_6 = PythonSensor(
        task_id='get_status_6',
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

    task_save_request_id_6 = SSHOperator(
        task_id='task_save_request_id_6',
        ssh_conn_id="ssh_08",
        command=bash_command_request_id,
        dag=dag
    )

    task_extract_partsize_6 = SSHOperator(
        task_id='task_extract_partsize_6',
        ssh_conn_id="ssh_08",
        command=bash_command_extract_partsize,
        dag=dag
    )

    task_download_txt_6 = SSHOperator(
        task_id='task_download_txt_6',
        ssh_conn_id="ssh_08",
        command=bash_download_txt,
        dag=dag
    )

    task_merge_txt_6 = SSHOperator(
        task_id='task_merge_txt_6',
        ssh_conn_id="ssh_08",
        command=bash_merge_txt,
        dag=dag
    )

    clear_YaApi_query_6 = PythonOperator(
        task_id='clear_YaApi_query_6',
        python_callable=clear_YaApi_query,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'request_id': read_request_id(path_request_id=path_request_id_v)}
    )

    task_move_bigTxt_6 = SSHOperator(
        task_id='task_move_bigTxt_6',
        ssh_conn_id="ssh_08",
        command=bash_hdfs_put_bigTxt,
        dag=dag
    )

    task_start_pySpark_visits_6_attribution = SSHOperator(
        task_id='task_start_pySpark_visits_6_attribution',
        ssh_conn_id="ssh_08",
        command=bash_start_pySpark_visits_6_attribution,
        dag=dag
    )

    task_clear_tmp_7 = SSHOperator(
        task_id='task_clear_tmp_7',
        ssh_conn_id="ssh_08",
        command=bash_clear_tmp,
        dag=dag
    )

    task_clear_hdfs_7 = SSHOperator(
        task_id='task_clear_hdfs_7',
        ssh_conn_id="ssh_08",
        command=bash_clear_hdfs,
        dag=dag
    )

    ocenka_vozmozhnisti_sozd_zaprosa_7 = PythonOperator(
        task_id='ocenka_vozmozhnisti_sozd_zaprosa_7',
        python_callable=ocenka_vozmozhnisti_sozd_zaprosa,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Attribution_7}
    )

    sozd_zaprosa_logov_7 = PythonOperator(
        task_id='sozd_zaprosa_logov_7',
        python_callable=sozd_zaprosa_logov,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Attribution_7,
            'path_request_id': path_request_id_v}
    )

    get_status_7 = PythonSensor(
        task_id='get_status_7',
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

    task_save_request_id_7 = SSHOperator(
        task_id='task_save_request_id_7',
        ssh_conn_id="ssh_08",
        command=bash_command_request_id,
        dag=dag
    )

    task_extract_partsize_7 = SSHOperator(
        task_id='task_extract_partsize_7',
        ssh_conn_id="ssh_08",
        command=bash_command_extract_partsize,
        dag=dag
    )

    task_download_txt_7 = SSHOperator(
        task_id='task_download_txt_7',
        ssh_conn_id="ssh_08",
        command=bash_download_txt,
        dag=dag
    )

    task_merge_txt_7 = SSHOperator(
        task_id='task_merge_txt_7',
        ssh_conn_id="ssh_08",
        command=bash_merge_txt,
        dag=dag
    )

    clear_YaApi_query_7 = PythonOperator(
        task_id='clear_YaApi_query_7',
        python_callable=clear_YaApi_query,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'request_id': read_request_id(path_request_id=path_request_id_v)}
    )

    task_move_bigTxt_7 = SSHOperator(
        task_id='task_move_bigTxt_7',
        ssh_conn_id="ssh_08",
        command=bash_hdfs_put_bigTxt,
        dag=dag
    )

    task_start_pySpark_visits_7_attribution = SSHOperator(
        task_id='task_start_pySpark_visits_7_attribution',
        ssh_conn_id="ssh_08",
        command=bash_start_pySpark_visits_7_attribution,
        dag=dag
    )

    task_clear_tmp_8 = SSHOperator(
        task_id='task_clear_tmp_8',
        ssh_conn_id="ssh_08",
        command=bash_clear_tmp,
        dag=dag
    )

    task_clear_hdfs_8 = SSHOperator(
        task_id='task_clear_hdfs_8',
        ssh_conn_id="ssh_08",
        command=bash_clear_hdfs,
        dag=dag
    )

    ocenka_vozmozhnisti_sozd_zaprosa_8 = PythonOperator(
        task_id='ocenka_vozmozhnisti_sozd_zaprosa_8',
        python_callable=ocenka_vozmozhnisti_sozd_zaprosa,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Attribution_8}
    )

    sozd_zaprosa_logov_8 = PythonOperator(
        task_id='sozd_zaprosa_logov_8',
        python_callable=sozd_zaprosa_logov,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Attribution_8,
            'path_request_id': path_request_id_v}
    )

    get_status_8 = PythonSensor(
        task_id='get_status_8',
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

    task_save_request_id_8 = SSHOperator(
        task_id='task_save_request_id_8',
        ssh_conn_id="ssh_08",
        command=bash_command_request_id,
        dag=dag
    )

    task_extract_partsize_8 = SSHOperator(
        task_id='task_extract_partsize_8',
        ssh_conn_id="ssh_08",
        command=bash_command_extract_partsize,
        dag=dag
    )

    task_download_txt_8 = SSHOperator(
        task_id='task_download_txt_8',
        ssh_conn_id="ssh_08",
        command=bash_download_txt,
        dag=dag
    )

    task_merge_txt_8 = SSHOperator(
        task_id='task_merge_txt_8',
        ssh_conn_id="ssh_08",
        command=bash_merge_txt,
        dag=dag
    )

    clear_YaApi_query_8 = PythonOperator(
        task_id='clear_YaApi_query_8',
        python_callable=clear_YaApi_query,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'request_id': read_request_id(path_request_id=path_request_id_v)}
    )

    task_move_bigTxt_8 = SSHOperator(
        task_id='task_move_bigTxt_8',
        ssh_conn_id="ssh_08",
        command=bash_hdfs_put_bigTxt,
        dag=dag
    )

    task_start_pySpark_visits_8_attribution = SSHOperator(
        task_id='task_start_pySpark_visits_8_attribution',
        ssh_conn_id="ssh_08",
        command=bash_start_pySpark_visits_8_attribution,
        dag=dag
    )

    task_clear_tmp_9 = SSHOperator(
        task_id='task_clear_tmp_9',
        ssh_conn_id="ssh_08",
        command=bash_clear_tmp,
        dag=dag
    )

    task_clear_hdfs_9 = SSHOperator(
        task_id='task_clear_hdfs_9',
        ssh_conn_id="ssh_08",
        command=bash_clear_hdfs,
        dag=dag
    )

    ocenka_vozmozhnisti_sozd_zaprosa_9 = PythonOperator(
        task_id='ocenka_vozmozhnisti_sozd_zaprosa_9',
        python_callable=ocenka_vozmozhnisti_sozd_zaprosa,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Attribution_9}
    )

    sozd_zaprosa_logov_9 = PythonOperator(
        task_id='sozd_zaprosa_logov_9',
        python_callable=sozd_zaprosa_logov,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Attribution_9,
            'path_request_id': path_request_id_v}
    )

    get_status_9 = PythonSensor(
        task_id='get_status_9',
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

    task_save_request_id_9 = SSHOperator(
        task_id='task_save_request_id_9',
        ssh_conn_id="ssh_08",
        command=bash_command_request_id,
        dag=dag
    )

    task_extract_partsize_9 = SSHOperator(
        task_id='task_extract_partsize_9',
        ssh_conn_id="ssh_08",
        command=bash_command_extract_partsize,
        dag=dag
    )

    task_download_txt_9 = SSHOperator(
        task_id='task_download_txt_9',
        ssh_conn_id="ssh_08",
        command=bash_download_txt,
        dag=dag
    )

    task_merge_txt_9 = SSHOperator(
        task_id='task_merge_txt_9',
        ssh_conn_id="ssh_08",
        command=bash_merge_txt,
        dag=dag
    )

    clear_YaApi_query_9 = PythonOperator(
        task_id='clear_YaApi_query_9',
        python_callable=clear_YaApi_query,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'request_id': read_request_id(path_request_id=path_request_id_v)}
    )

    task_move_bigTxt_9 = SSHOperator(
        task_id='task_move_bigTxt_9',
        ssh_conn_id="ssh_08",
        command=bash_hdfs_put_bigTxt,
        dag=dag
    )

    task_start_pySpark_visits_9_attribution = SSHOperator(
        task_id='task_start_pySpark_visits_9_attribution',
        ssh_conn_id="ssh_08",
        command=bash_start_pySpark_visits_9_attribution,
        dag=dag
    )

    task_clear_tmp_10 = SSHOperator(
        task_id='task_clear_tmp_10',
        ssh_conn_id="ssh_08",
        command=bash_clear_tmp,
        dag=dag
    )

    task_clear_hdfs_10 = SSHOperator(
        task_id='task_clear_hdfs_10',
        ssh_conn_id="ssh_08",
        command=bash_clear_hdfs,
        dag=dag
    )

    ocenka_vozmozhnisti_sozd_zaprosa_10 = PythonOperator(
        task_id='ocenka_vozmozhnisti_sozd_zaprosa_10',
        python_callable=ocenka_vozmozhnisti_sozd_zaprosa,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Attribution_10}
    )

    sozd_zaprosa_logov_10 = PythonOperator(
        task_id='sozd_zaprosa_logov_10',
        python_callable=sozd_zaprosa_logov,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Attribution_10,
            'path_request_id': path_request_id_v}
    )

    get_status_10 = PythonSensor(
        task_id='get_status_10',
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

    task_save_request_id_10 = SSHOperator(
        task_id='task_save_request_id_10',
        ssh_conn_id="ssh_08",
        command=bash_command_request_id,
        dag=dag
    )

    task_extract_partsize_10 = SSHOperator(
        task_id='task_extract_partsize_10',
        ssh_conn_id="ssh_08",
        command=bash_command_extract_partsize,
        dag=dag
    )

    task_download_txt_10 = SSHOperator(
        task_id='task_download_txt_10',
        ssh_conn_id="ssh_08",
        command=bash_download_txt,
        dag=dag
    )

    task_merge_txt_10 = SSHOperator(
        task_id='task_merge_txt_10',
        ssh_conn_id="ssh_08",
        command=bash_merge_txt,
        dag=dag
    )

    clear_YaApi_query_10 = PythonOperator(
        task_id='clear_YaApi_query_10',
        python_callable=clear_YaApi_query,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'request_id': read_request_id(path_request_id=path_request_id_v)}
    )

    task_move_bigTxt_10 = SSHOperator(
        task_id='task_move_bigTxt_10',
        ssh_conn_id="ssh_08",
        command=bash_hdfs_put_bigTxt,
        dag=dag
    )

    task_start_pySpark_visits_10_attribution = SSHOperator(
        task_id='task_start_pySpark_visits_10_attribution',
        ssh_conn_id="ssh_08",
        command=bash_start_pySpark_visits_10_attribution,
        dag=dag
    )

    task_clear_tmp_11 = SSHOperator(
        task_id='task_clear_tmp_11',
        ssh_conn_id="ssh_08",
        command=bash_clear_tmp,
        dag=dag
    )

    task_clear_hdfs_11 = SSHOperator(
        task_id='task_clear_hdfs_11',
        ssh_conn_id="ssh_08",
        command=bash_clear_hdfs,
        dag=dag
    )

    ocenka_vozmozhnisti_sozd_zaprosa_11 = PythonOperator(
        task_id='ocenka_vozmozhnisti_sozd_zaprosa_11',
        python_callable=ocenka_vozmozhnisti_sozd_zaprosa,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Attribution_11}
    )

    sozd_zaprosa_logov_11 = PythonOperator(
        task_id='sozd_zaprosa_logov_11',
        python_callable=sozd_zaprosa_logov,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Attribution_11,
            'path_request_id': path_request_id_v}
    )

    get_status_11 = PythonSensor(
        task_id='get_status_11',
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

    task_save_request_id_11 = SSHOperator(
        task_id='task_save_request_id_11',
        ssh_conn_id="ssh_08",
        command=bash_command_request_id,
        dag=dag
    )

    task_extract_partsize_11 = SSHOperator(
        task_id='task_extract_partsize_11',
        ssh_conn_id="ssh_08",
        command=bash_command_extract_partsize,
        dag=dag
    )

    task_download_txt_11 = SSHOperator(
        task_id='task_download_txt_11',
        ssh_conn_id="ssh_08",
        command=bash_download_txt,
        dag=dag
    )

    task_merge_txt_11 = SSHOperator(
        task_id='task_merge_txt_11',
        ssh_conn_id="ssh_08",
        command=bash_merge_txt,
        dag=dag
    )

    clear_YaApi_query_11 = PythonOperator(
        task_id='clear_YaApi_query_11',
        python_callable=clear_YaApi_query,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'request_id': read_request_id(path_request_id=path_request_id_v)}
    )

    task_move_bigTxt_11 = SSHOperator(
        task_id='task_move_bigTxt_11',
        ssh_conn_id="ssh_08",
        command=bash_hdfs_put_bigTxt,
        dag=dag
    )

    task_start_pySpark_visits_11_attribution = SSHOperator(
        task_id='task_start_pySpark_visits_11_attribution',
        ssh_conn_id="ssh_08",
        command=bash_start_pySpark_visits_11_attribution,
        dag=dag
    )

    task_clear_tmp_12 = SSHOperator(
        task_id='task_clear_tmp_12',
        ssh_conn_id="ssh_08",
        command=bash_clear_tmp,
        dag=dag
    )

    task_clear_hdfs_12 = SSHOperator(
        task_id='task_clear_hdfs_12',
        ssh_conn_id="ssh_08",
        command=bash_clear_hdfs,
        dag=dag
    )

    ocenka_vozmozhnisti_sozd_zaprosa_12 = PythonOperator(
        task_id='ocenka_vozmozhnisti_sozd_zaprosa_12',
        python_callable=ocenka_vozmozhnisti_sozd_zaprosa,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Attribution_12}
    )

    sozd_zaprosa_logov_12 = PythonOperator(
        task_id='sozd_zaprosa_logov_12',
        python_callable=sozd_zaprosa_logov,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Attribution_12,
            'path_request_id': path_request_id_v}
    )

    get_status_12 = PythonSensor(
        task_id='get_status_12',
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

    task_save_request_id_12 = SSHOperator(
        task_id='task_save_request_id_12',
        ssh_conn_id="ssh_08",
        command=bash_command_request_id,
        dag=dag
    )

    task_extract_partsize_12 = SSHOperator(
        task_id='task_extract_partsize_12',
        ssh_conn_id="ssh_08",
        command=bash_command_extract_partsize,
        dag=dag
    )

    task_download_txt_12 = SSHOperator(
        task_id='task_download_txt_12',
        ssh_conn_id="ssh_08",
        command=bash_download_txt,
        dag=dag
    )

    task_merge_txt_12 = SSHOperator(
        task_id='task_merge_txt_12',
        ssh_conn_id="ssh_08",
        command=bash_merge_txt,
        dag=dag
    )

    clear_YaApi_query_12 = PythonOperator(
        task_id='clear_YaApi_query_12',
        python_callable=clear_YaApi_query,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'request_id': read_request_id(path_request_id=path_request_id_v)}
    )

    task_move_bigTxt_12 = SSHOperator(
        task_id='task_move_bigTxt_12',
        ssh_conn_id="ssh_08",
        command=bash_hdfs_put_bigTxt,
        dag=dag
    )

    task_start_pySpark_visits_12_attribution = SSHOperator(
        task_id='task_start_pySpark_visits_12_attribution',
        ssh_conn_id="ssh_08",
        command=bash_start_pySpark_visits_12_attribution,
        dag=dag
    )

    task_clear_tmp_13 = SSHOperator(
        task_id='task_clear_tmp_13',
        ssh_conn_id="ssh_08",
        command=bash_clear_tmp,
        dag=dag
    )

    task_clear_hdfs_13 = SSHOperator(
        task_id='task_clear_hdfs_13',
        ssh_conn_id="ssh_08",
        command=bash_clear_hdfs,
        dag=dag
    )

    ocenka_vozmozhnisti_sozd_zaprosa_13 = PythonOperator(
        task_id='ocenka_vozmozhnisti_sozd_zaprosa_13',
        python_callable=ocenka_vozmozhnisti_sozd_zaprosa,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Attribution_13}
    )

    sozd_zaprosa_logov_13 = PythonOperator(
        task_id='sozd_zaprosa_logov_13',
        python_callable=sozd_zaprosa_logov,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'start_date': start_date_v,
            'end_date': end_date_v,
            'col_tab': col_tab_Attribution_13,
            'path_request_id': path_request_id_v}
    )

    get_status_13 = PythonSensor(
        task_id='get_status_13',
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

    task_save_request_id_13 = SSHOperator(
        task_id='task_save_request_id_13',
        ssh_conn_id="ssh_08",
        command=bash_command_request_id,
        dag=dag
    )

    task_extract_partsize_13 = SSHOperator(
        task_id='task_extract_partsize_13',
        ssh_conn_id="ssh_08",
        command=bash_command_extract_partsize,
        dag=dag
    )

    task_download_txt_13 = SSHOperator(
        task_id='task_download_txt_13',
        ssh_conn_id="ssh_08",
        command=bash_download_txt,
        dag=dag
    )

    task_merge_txt_13 = SSHOperator(
        task_id='task_merge_txt_13',
        ssh_conn_id="ssh_08",
        command=bash_merge_txt,
        dag=dag
    )

    clear_YaApi_query_13 = PythonOperator(
        task_id='clear_YaApi_query_13',
        python_callable=clear_YaApi_query,
        dag=dag,
        op_kwargs={
            'counter_id': counter_id_v,
            'API_token': API_token_v,
            'headers': headers_v,
            'request_id': read_request_id(path_request_id=path_request_id_v)}
    )

    task_move_bigTxt_13 = SSHOperator(
        task_id='task_move_bigTxt_13',
        ssh_conn_id="ssh_08",
        command=bash_hdfs_put_bigTxt,
        dag=dag
    )

    task_start_pySpark_visits_13_attribution = SSHOperator(
        task_id='task_start_pySpark_visits_13_attribution',
        ssh_conn_id="ssh_08",
        command=bash_start_pySpark_visits_13_attribution,
        dag=dag
    )

    task_clear_tmp_14 = SSHOperator(
        task_id='task_clear_tmp_14',
        ssh_conn_id="ssh_08",
        command=bash_clear_tmp,
        dag=dag
    )

    task_clear_hdfs_14 = SSHOperator(
        task_id='task_clear_hdfs_14',
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

    task_rm_13dir_hdfs_2 = SSHOperator(
        task_id='task_rm_13dir_hdfs_2',
        ssh_conn_id="ssh_08",
        command=bash_rm_13dir_hdfs,
        dag=dag
    )

    tlgrm_allSuccess = PythonOperator(
        task_id='tlgrm_allSuccess',
        python_callable=tlgrm,
        dag=dag,
        trigger_rule='all_success',
        op_kwargs={
            'message': f"Отработал успешно ✅ - DAG visits_schedule_{start_date_v}_{end_date_v}"}
    )

    tlgrm_oneFailed = PythonOperator(
        task_id='tlgrm_oneFailed',
        python_callable=tlgrm,
        dag=dag,
        trigger_rule='one_failed',
        op_kwargs={
            'message': f"Упал 🛑 - DAG visits_schedule_{request_id_v}_{start_date_v}_{end_date_v}"}
    )

    logging_faild = PythonOperator(
        task_id='logging_faild',
        python_callable=logging_faild,
        dag=dag,
        trigger_rule='one_failed',
        op_kwargs={
            'text': f"DAG visits_schedule_{request_id_v}_{start_date_v}_{end_date_v} - faild" + "\n",
            'path_faild': path_faild_v}
    )

(task_clear_tmp_1 >> task_clear_hdfs_1 >> task_rm_13dir_hdfs_1 >> ocenka_vozmozhnisti_sozd_zaprosa_1 >> sozd_zaprosa_logov_1 >> get_status_1 >> task_save_request_id_1 >> task_extract_partsize_1 >> task_download_txt_1 >> task_merge_txt_1 >> clear_YaApi_query_1 >> task_move_bigTxt_1 >> task_start_pySpark_visits_1_basic_events_orc >> task_clear_tmp_2 >> task_clear_hdfs_2 >> ocenka_vozmozhnisti_sozd_zaprosa_2 >> sozd_zaprosa_logov_2 >> get_status_2 >> task_save_request_id_2 >> task_extract_partsize_2 >> task_download_txt_2 >> task_merge_txt_2 >> clear_YaApi_query_2 >> task_move_bigTxt_2 >> task_start_pySpark_visits_2_ecommerce_orc >> task_clear_tmp_3 >> task_clear_hdfs_3 >> ocenka_vozmozhnisti_sozd_zaprosa_3 >> sozd_zaprosa_logov_3 >> get_status_3 >> task_save_request_id_3 >> task_extract_partsize_3 >> task_download_txt_3 >> task_merge_txt_3 >> clear_YaApi_query_3 >> task_move_bigTxt_3 >> task_start_pySpark_visits_3_goals_orc >> task_clear_tmp_4 >> task_clear_hdfs_4 >> ocenka_vozmozhnisti_sozd_zaprosa_4 >> sozd_zaprosa_logov_4 >> get_status_4 >> task_save_request_id_4 >> task_extract_partsize_4 >> task_download_txt_4 >> task_merge_txt_4 >> clear_YaApi_query_4 >> task_move_bigTxt_4 >> task_start_pySpark_visits_4_device_source >> task_clear_tmp_5 >> task_clear_hdfs_5 >> ocenka_vozmozhnisti_sozd_zaprosa_5 >> sozd_zaprosa_logov_5 >> get_status_5 >> task_save_request_id_5 >> task_extract_partsize_5 >> task_download_txt_5 >> task_merge_txt_5 >> clear_YaApi_query_5 >> task_move_bigTxt_5 >> task_start_pySpark_visits_5_attribution >> task_clear_tmp_6 >> task_clear_hdfs_6 >> ocenka_vozmozhnisti_sozd_zaprosa_6 >> sozd_zaprosa_logov_6 >> get_status_6 >> task_save_request_id_6 >> task_extract_partsize_6 >> task_download_txt_6 >> task_merge_txt_6 >> clear_YaApi_query_6 >> task_move_bigTxt_6 >> task_start_pySpark_visits_6_attribution >> task_clear_tmp_7 >> task_clear_hdfs_7 >> ocenka_vozmozhnisti_sozd_zaprosa_7 >> sozd_zaprosa_logov_7 >> get_status_7 >> task_save_request_id_7 >> task_extract_partsize_7 >> task_download_txt_7 >> task_merge_txt_7 >> clear_YaApi_query_7 >> task_move_bigTxt_7 >> task_start_pySpark_visits_7_attribution >> task_clear_tmp_8 >> task_clear_hdfs_8 >> ocenka_vozmozhnisti_sozd_zaprosa_8 >> sozd_zaprosa_logov_8 >> get_status_8 >> task_save_request_id_8 >> task_extract_partsize_8 >> task_download_txt_8 >> task_merge_txt_8 >> clear_YaApi_query_8 >> task_move_bigTxt_8 >> task_start_pySpark_visits_8_attribution >> task_clear_tmp_9 >> task_clear_hdfs_9 >> ocenka_vozmozhnisti_sozd_zaprosa_9 >> sozd_zaprosa_logov_9 >> get_status_9 >> task_save_request_id_9 >> task_extract_partsize_9 >> task_download_txt_9 >> task_merge_txt_9 >> clear_YaApi_query_9 >> task_move_bigTxt_9 >> task_start_pySpark_visits_9_attribution >> task_clear_tmp_10 >> task_clear_hdfs_10 >> ocenka_vozmozhnisti_sozd_zaprosa_10 >> sozd_zaprosa_logov_10 >> get_status_10 >> task_save_request_id_10 >> task_extract_partsize_10 >> task_download_txt_10 >> task_merge_txt_10 >> clear_YaApi_query_10 >> task_move_bigTxt_10 >> task_start_pySpark_visits_10_attribution >> task_clear_tmp_11 >> task_clear_hdfs_11 >> ocenka_vozmozhnisti_sozd_zaprosa_11 >> sozd_zaprosa_logov_11 >> get_status_11 >> task_save_request_id_11 >> task_extract_partsize_11 >> task_download_txt_11 >> task_merge_txt_11 >> clear_YaApi_query_11 >> task_move_bigTxt_11 >> task_start_pySpark_visits_11_attribution >> task_clear_tmp_12 >> task_clear_hdfs_12 >> ocenka_vozmozhnisti_sozd_zaprosa_12 >> sozd_zaprosa_logov_12 >> get_status_12 >> task_save_request_id_12 >> task_extract_partsize_12 >> task_download_txt_12 >> task_merge_txt_12 >> clear_YaApi_query_12 >> task_move_bigTxt_12 >> task_start_pySpark_visits_12_attribution >> task_clear_tmp_13 >> task_clear_hdfs_13 >> ocenka_vozmozhnisti_sozd_zaprosa_13 >> sozd_zaprosa_logov_13 >> get_status_13 >> task_save_request_id_13 >> task_extract_partsize_13 >> task_download_txt_13 >> task_merge_txt_13 >> clear_YaApi_query_13 >> task_move_bigTxt_13 >> task_start_pySpark_visits_13_attribution >> task_clear_tmp_14 >> task_clear_hdfs_14 >> task_start_pySpark_join >> task_rm_13dir_hdfs_2 >> [tlgrm_allSuccess, tlgrm_oneFailed, logging_faild])
