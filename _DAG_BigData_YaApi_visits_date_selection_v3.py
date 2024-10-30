import sys
import requests
from datetime import datetime, date, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.sensors.python import PythonSensor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

# Даты для выгрузки поставленной н расписание.
# 3-4 дня чтоб большая часть сессий клиентов на стороне сайта были завершены
# start_date = end_date тк бесплатное хранилище на стороне ЯМ 10 Гб
start_date_v = str(Variable.get("startEndDate_YaMetrika"))
end_date_v = str(Variable.get("startEndDate_YaMetrika"))

# Праметры выгрузки, захардкожены и не читаются из файла потому что нет доступа на хост
col_tab_Basic_Events_1 = "ym:s:visitID,ym:s:date,ym:s:parsedParamsKey1,ym:s:parsedParamsKey10,ym:s:parsedParamsKey2,ym:s:parsedParamsKey3,ym:s:parsedParamsKey4,ym:s:parsedParamsKey5,ym:s:parsedParamsKey6,ym:s:parsedParamsKey7,ym:s:parsedParamsKey8,ym:s:parsedParamsKey9,ym:s:goalsCurrency,ym:s:goalsDateTime,ym:s:goalsID,ym:s:goalsOrder,ym:s:goalsPrice,ym:s:goalsSerialNumber,ym:s:bounce,ym:s:clientID,ym:s:counterID,ym:s:counterUserIDHash,ym:s:dateTime,ym:s:dateTimeUTC,ym:s:endURL,ym:s:ipAddress,ym:s:isNewUser,ym:s:pageViews,ym:s:regionCity,ym:s:regionCityID,ym:s:regionCountry,ym:s:regionCountryID,ym:s:startURL,ym:s:visitDuration,ym:s:watchIDs"
col_tab_Ecommerce_2 = "ym:s:visitID,ym:s:date,ym:s:impressionsProductVariant,ym:s:impressionsURL,ym:s:productsBrand,ym:s:productsCategory,ym:s:productsCategory1,ym:s:productsCategory2,ym:s:productsCategory3,ym:s:productsCategory4,ym:s:productsCoupon,ym:s:productsCurrency,ym:s:productsDiscount,ym:s:productsEventTime,ym:s:productsID,ym:s:productsList,ym:s:productsName,ym:s:productsPosition,ym:s:productsPrice,ym:s:productsPurchaseID,ym:s:productsQuantity,ym:s:productsVariant,ym:s:promotionCreative,ym:s:promotionCreativeSlot,ym:s:promotionEventTime,ym:s:promotionID,ym:s:promotionName,ym:s:promotionPosition,ym:s:promotionType,ym:s:purchaseAffiliation,ym:s:purchaseCoupon,ym:s:purchaseCurrency,ym:s:purchaseDateTime,ym:s:purchaseID,ym:s:purchaseProductQuantity,ym:s:purchaseRevenue"
col_tab_Goals_3 = "ym:s:visitID,ym:s:date,ym:s:automaticAdvEngine,ym:s:automaticClickBannerGroupName,ym:s:automaticCurrencyID,ym:s:automaticDirectBannerGroup,ym:s:automaticDirectClickBanner,ym:s:automaticDirectClickBannerName,ym:s:automaticDirectClickOrder,ym:s:automaticDirectClickOrderName,ym:s:automaticDirectConditionType,ym:s:automaticDirectPhraseOrCond,ym:s:automaticDirectPlatform,ym:s:automaticDirectPlatformType,ym:s:automaticGCLID,ym:s:automatichasGCLID,ym:s:automaticMessenger,ym:s:automaticRecommendationSystem,ym:s:automaticReferalSource,ym:s:automaticSearchEngine,ym:s:automaticSearchEngineRoot,ym:s:automaticSocialNetwork,ym:s:automaticSocialNetworkProfile,ym:s:automaticTrafficSource,ym:s:automaticUTMCampaign,ym:s:automaticUTMContent,ym:s:automaticUTMMedium,ym:s:automaticUTMSource,ym:s:automaticUTMTerm"
col_tab_Device_Source_4 = "ym:s:visitID,ym:s:date,ym:s:from,ym:s:referer,ym:s:browser,ym:s:browserCountry,ym:s:browserEngine,ym:s:browserEngineVersion1,ym:s:browserEngineVersion2,ym:s:browserEngineVersion3,ym:s:browserEngineVersion4,ym:s:browserLanguage,ym:s:browserMajorVersion,ym:s:browserMinorVersion,ym:s:clientTimeZone,ym:s:cookieEnabled,ym:s:deviceCategory,ym:s:javascriptEnabled,ym:s:mobilePhone,ym:s:mobilePhoneModel,ym:s:operatingSystem,ym:s:operatingSystemRoot,ym:s:physicalScreenHeight,ym:s:physicalScreenWidth,ym:s:screenColors,ym:s:screenFormat,ym:s:screenHeight,ym:s:screenOrientation,ym:s:screenWidth,ym:s:windowClientHeight,ym:s:windowClientWidth"
col_tab_Attribution_5 = "ym:s:visitID,ym:s:date,ym:s:cross_device_firstAdvEngine,ym:s:cross_device_firstClickBannerGroupName,ym:s:cross_device_firstCurrencyID,ym:s:cross_device_firstDirectBannerGroup,ym:s:cross_device_firstDirectClickBanner,ym:s:cross_device_firstDirectClickBannerName,ym:s:cross_device_firstDirectClickOrder,ym:s:cross_device_firstDirectClickOrderName,ym:s:cross_device_firstDirectConditionType,ym:s:cross_device_firstDirectPhraseOrCond,ym:s:cross_device_firstDirectPlatform,ym:s:cross_device_firstDirectPlatformType,ym:s:cross_device_firstGCLID,ym:s:cross_device_firsthasGCLID,ym:s:cross_device_firstMessenger,ym:s:cross_device_firstRecommendationSystem,ym:s:cross_device_firstReferalSource,ym:s:cross_device_firstSearchEngine,ym:s:cross_device_firstSearchEngineRoot,ym:s:cross_device_firstSocialNetwork,ym:s:cross_device_firstSocialNetworkProfile,ym:s:cross_device_firstTrafficSource,ym:s:cross_device_firstUTMCampaign,ym:s:cross_device_firstUTMContent,ym:s:cross_device_firstUTMMedium,ym:s:cross_device_firstUTMSource,ym:s:cross_device_firstUTMTerm"
col_tab_Attribution_6 = "ym:s:visitID,ym:s:date,ym:s:cross_device_lastAdvEngine,ym:s:cross_device_lastClickBannerGroupName,ym:s:cross_device_lastCurrencyID,ym:s:cross_device_lastDirectBannerGroup,ym:s:cross_device_lastDirectClickBanner,ym:s:cross_device_lastDirectClickBannerName,ym:s:cross_device_lastDirectClickOrder,ym:s:cross_device_lastDirectClickOrderName,ym:s:cross_device_lastDirectConditionType,ym:s:cross_device_lastDirectPhraseOrCond,ym:s:cross_device_lastDirectPlatform,ym:s:cross_device_lastDirectPlatformType,ym:s:cross_device_lastGCLID,ym:s:cross_device_lasthasGCLID,ym:s:cross_device_lastMessenger,ym:s:cross_device_lastRecommendationSystem,ym:s:cross_device_lastReferalSource,ym:s:cross_device_lastSearchEngine,ym:s:cross_device_lastSearchEngineRoot,ym:s:cross_device_lastSocialNetwork,ym:s:cross_device_lastSocialNetworkProfile,ym:s:cross_device_lastTrafficSource,ym:s:cross_device_lastUTMCampaign,ym:s:cross_device_lastUTMContent,ym:s:cross_device_lastUTMMedium,ym:s:cross_device_lastUTMSource,ym:s:cross_device_lastUTMTerm"
col_tab_Attribution_7 = "ym:s:visitID,ym:s:date,ym:s:cross_device_last_significantAdvEngine,ym:s:cross_device_last_significantClickBannerGroupName,ym:s:cross_device_last_significantCurrencyID,ym:s:cross_device_last_significantDirectBannerGroup,ym:s:cross_device_last_significantDirectClickBanner,ym:s:cross_device_last_significantDirectClickBannerName,ym:s:cross_device_last_significantDirectClickOrder,ym:s:cross_device_last_significantDirectClickOrderName,ym:s:cross_device_last_significantDirectConditionType,ym:s:cross_device_last_significantDirectPhraseOrCond,ym:s:cross_device_last_significantDirectPlatform,ym:s:cross_device_last_significantDirectPlatformType,ym:s:cross_device_last_significantGCLID,ym:s:cross_device_last_significanthasGCLID,ym:s:cross_device_last_significantMessenger,ym:s:cross_device_last_significantRecommendationSystem,ym:s:cross_device_last_significantReferalSource,ym:s:cross_device_last_significantSearchEngine,ym:s:cross_device_last_significantSearchEngineRoot,ym:s:cross_device_last_significantSocialNetwork,ym:s:cross_device_last_significantSocialNetworkProfile,ym:s:cross_device_last_significantTrafficSource,ym:s:cross_device_last_significantUTMCampaign,ym:s:cross_device_last_significantUTMContent,ym:s:cross_device_last_significantUTMMedium,ym:s:cross_device_last_significantUTMSource,ym:s:cross_device_last_significantUTMTerm"
col_tab_Attribution_8 = "ym:s:visitID,ym:s:date,ym:s:cross_device_last_yandex_direct_clickAdvEngine,ym:s:cross_device_last_yandex_direct_clickClickBannerGroupName,ym:s:cross_device_last_yandex_direct_clickCurrencyID,ym:s:cross_device_last_yandex_direct_clickDirectBannerGroup,ym:s:cross_device_last_yandex_direct_clickDirectClickBanner,ym:s:cross_device_last_yandex_direct_clickDirectClickBannerName,ym:s:cross_device_last_yandex_direct_clickDirectClickOrder,ym:s:cross_device_last_yandex_direct_clickDirectClickOrderName,ym:s:cross_device_last_yandex_direct_clickDirectConditionType,ym:s:cross_device_last_yandex_direct_clickDirectPhraseOrCond,ym:s:cross_device_last_yandex_direct_clickDirectPlatform,ym:s:cross_device_last_yandex_direct_clickDirectPlatformType,ym:s:cross_device_last_yandex_direct_clickGCLID,ym:s:cross_device_last_yandex_direct_clickhasGCLID,ym:s:cross_device_last_yandex_direct_clickMessenger,ym:s:cross_device_last_yandex_direct_clickRecommendationSystem,ym:s:cross_device_last_yandex_direct_clickReferalSource,ym:s:cross_device_last_yandex_direct_clickSearchEngine,ym:s:cross_device_last_yandex_direct_clickSearchEngineRoot,ym:s:cross_device_last_yandex_direct_clickSocialNetwork,ym:s:cross_device_last_yandex_direct_clickSocialNetworkProfile,ym:s:cross_device_last_yandex_direct_clickTrafficSource,ym:s:cross_device_last_yandex_direct_clickUTMCampaign,ym:s:cross_device_last_yandex_direct_clickUTMContent,ym:s:cross_device_last_yandex_direct_clickUTMMedium,ym:s:cross_device_last_yandex_direct_clickUTMSource,ym:s:cross_device_last_yandex_direct_clickUTMTerm"
col_tab_Attribution_9 = "ym:s:visitID,ym:s:date,ym:s:firstAdvEngine,ym:s:firstClickBannerGroupName,ym:s:firstCurrencyID,ym:s:firstDirectBannerGroup,ym:s:firstDirectClickBanner,ym:s:firstDirectClickBannerName,ym:s:firstDirectClickOrder,ym:s:firstDirectClickOrderName,ym:s:firstDirectConditionType,ym:s:firstDirectPhraseOrCond,ym:s:firstDirectPlatform,ym:s:firstDirectPlatformType,ym:s:firstGCLID,ym:s:firsthasGCLID,ym:s:firstMessenger,ym:s:firstRecommendationSystem,ym:s:firstReferalSource,ym:s:firstSearchEngine,ym:s:firstSearchEngineRoot,ym:s:firstSocialNetwork,ym:s:firstSocialNetworkProfile,ym:s:firstTrafficSource,ym:s:firstUTMCampaign,ym:s:firstUTMContent,ym:s:firstUTMMedium,ym:s:firstUTMSource,ym:s:firstUTMTerm"
col_tab_Attribution_10 = "ym:s:visitID,ym:s:date,ym:s:lastAdvEngine,ym:s:lastClickBannerGroupName,ym:s:lastCurrencyID,ym:s:lastDirectBannerGroup,ym:s:lastDirectClickBanner,ym:s:lastDirectClickBannerName,ym:s:lastDirectClickOrder,ym:s:lastDirectClickOrderName,ym:s:lastDirectConditionType,ym:s:lastDirectPhraseOrCond,ym:s:lastDirectPlatform,ym:s:lastDirectPlatformType,ym:s:lastGCLID,ym:s:lasthasGCLID,ym:s:lastMessenger,ym:s:lastRecommendationSystem,ym:s:lastReferalSource,ym:s:lastSearchEngine,ym:s:lastSearchEngineRoot,ym:s:lastSocialNetwork,ym:s:lastSocialNetworkProfile,ym:s:lastTrafficSource,ym:s:lastUTMCampaign,ym:s:lastUTMContent,ym:s:lastUTMMedium,ym:s:lastUTMSource,ym:s:lastUTMTerm"
col_tab_Attribution_11 = "ym:s:visitID,ym:s:date,ym:s:last_yandex_direct_clickAdvEngine,ym:s:last_yandex_direct_clickClickBannerGroupName,ym:s:last_yandex_direct_clickCurrencyID,ym:s:last_yandex_direct_clickDirectBannerGroup,ym:s:last_yandex_direct_clickDirectClickBanner,ym:s:last_yandex_direct_clickDirectClickBannerName,ym:s:last_yandex_direct_clickDirectClickOrder,ym:s:last_yandex_direct_clickDirectClickOrderName,ym:s:last_yandex_direct_clickDirectConditionType,ym:s:last_yandex_direct_clickDirectPhraseOrCond,ym:s:last_yandex_direct_clickDirectPlatform,ym:s:last_yandex_direct_clickDirectPlatformType,ym:s:last_yandex_direct_clickGCLID,ym:s:last_yandex_direct_clickhasGCLID,ym:s:last_yandex_direct_clickMessenger,ym:s:last_yandex_direct_clickRecommendationSystem,ym:s:last_yandex_direct_clickReferalSource,ym:s:last_yandex_direct_clickSearchEngine,ym:s:last_yandex_direct_clickSearchEngineRoot,ym:s:last_yandex_direct_clickSocialNetwork,ym:s:last_yandex_direct_clickSocialNetworkProfile,ym:s:last_yandex_direct_clickTrafficSource,ym:s:last_yandex_direct_clickUTMCampaign,ym:s:last_yandex_direct_clickUTMContent,ym:s:last_yandex_direct_clickUTMMedium,ym:s:last_yandex_direct_clickUTMSource,ym:s:last_yandex_direct_clickUTMTerm"
col_tab_Attribution_12 = "ym:s:visitID,ym:s:date,ym:s:lastsignAdvEngine,ym:s:lastsignClickBannerGroupName,ym:s:lastsignCurrencyID,ym:s:lastsignDirectBannerGroup,ym:s:lastsignDirectClickBanner,ym:s:lastsignDirectClickBannerName,ym:s:lastsignDirectClickOrder,ym:s:lastsignDirectClickOrderName,ym:s:lastsignDirectConditionType,ym:s:lastsignDirectPhraseOrCond,ym:s:lastsignDirectPlatform,ym:s:lastsignDirectPlatformType,ym:s:lastsignGCLID,ym:s:lastsignhasGCLID,ym:s:lastsignMessenger,ym:s:lastsignRecommendationSystem,ym:s:lastsignReferalSource,ym:s:lastsignSearchEngine,ym:s:lastsignSearchEngineRoot,ym:s:lastsignSocialNetwork,ym:s:lastsignSocialNetworkProfile,ym:s:lastsignTrafficSource,ym:s:lastsignUTMCampaign,ym:s:lastsignUTMContent,ym:s:lastsignUTMMedium,ym:s:lastsignUTMSource,ym:s:lastsignUTMTerm"
col_tab_Attribution_13 = "ym:s:visitID,ym:s:date,ym:s:eventsProductBrand,ym:s:eventsProductCategory,ym:s:eventsProductCategory1,ym:s:eventsProductCategory2,ym:s:eventsProductCategory3,ym:s:eventsProductCategory4,ym:s:eventsProductCoupon,ym:s:eventsProductCurrency,ym:s:eventsProductDiscount,ym:s:eventsProductEventTime,ym:s:eventsProductID,ym:s:eventsProductList,ym:s:eventsProductName,ym:s:eventsProductPosition,ym:s:eventsProductPrice,ym:s:eventsProductQuantity,ym:s:eventsProductType,ym:s:eventsProductVariant,ym:s:impressionsDateTime,ym:s:impressionsProductBrand,ym:s:impressionsProductCategory,ym:s:impressionsProductCategory1,ym:s:impressionsProductCategory2,ym:s:impressionsProductCategory3,ym:s:impressionsProductCategory4,ym:s:impressionsProductCoupon,ym:s:impressionsProductCurrency,ym:s:impressionsProductDiscount,ym:s:impressionsProductEventTime,ym:s:impressionsProductID,ym:s:impressionsProductList,ym:s:impressionsProductName,ym:s:impressionsProductPrice,ym:s:impressionsProductQuantity"

counter_id_v = "11111111"
API_token_v = "put_your_token"
headers_v = {'Authorization': f"OAuth {API_token_v}", 'Accept-Encoding': 'gzip'}

# BASH команды которые толкают скрипты на стороне 08 хоста 
bash_clear_tmp = '/usr/bin/python3 /home/bdataadmin/airflow/test_script/clear_logs_folder.py'
bash_clear_hdfs = '/usr/bin/bash /home/bdataadmin/airflow/test_script/сheck_hdfs_txt.sh '

bash_rm_13dir_hdfs = '/usr/bin/bash /home/bdataadmin/airflow/test_script/remove_13dir_hdfs.sh '

bash_command_extract_partsize = '/usr/bin/python3 /home/bdataadmin/airflow/test_script/extract_partsize.py'
bash_download_txt = '/usr/bin/python3 /home/bdataadmin/airflow/test_script/dowload_parts.py'
bash_merge_txt = '/usr/bin/python3 /home/bdataadmin/airflow/test_script/merge_txt.py'
bash_hdfs_put_bigTxt = '/usr/bin/bash hdfs dfs -put /home/bdataadmin/airflow/txt_big/final_txt.txt /data/yaMetrika/txt '

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

def read_request_id():
    """
    Функция нужна чтоб читать сохраненный request_id
    """
    request_id = Variable.get("request_id_YaMetrika")
    return request_id

# Дублируем индентификатор выгрузки запроса на 08 хосте
bash_command_request_id = f'echo "{read_request_id()}" > /home/bdataadmin/airflow/logs/request_id.txt'


def ocenka_vozmozhnisti_sozd_zaprosa(counter_id, headers, start_date, end_date, col_tab):
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


def sozd_zaprosa_logov(counter_id, headers, start_date, end_date, col_tab):
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

    if r.status_code == 200:
        if r.json()['log_request']['status'] == 'created':
            print('Запрос создаётся')
            # print(r.json()['log_request']['request_id'])
            Variable.set("request_id_YaMetrika", str(r.json()['log_request']['request_id']))
        else:
            sys.exit('Что-то пошло не так' + str(r.status_code))
            return False
    else:
        sys.exit('Запрос упал: ' + str(r.status_code))
        return False


def get_status(counter_id, headers, request_id):
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


def clear_YaApi_query(counter_id, headers, request_id):
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
    token = "bot_ID:bot_token"
    url = "https://api.telegram.org/bot"
    channel_id = "-100ChatID"
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
with DAG(dag_id='BigData_YaApi_visits_date_selection_v3',
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
            'col_tab': col_tab_Basic_Events_1}
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
            'request_id': read_request_id()}
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
            'request_id': read_request_id()}
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
            'col_tab': col_tab_Ecommerce_2}
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
            'request_id': read_request_id()}
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
            'request_id': read_request_id()}
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
            'col_tab': col_tab_Goals_3}
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
            'request_id': read_request_id()}
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
            'request_id': read_request_id()}
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
            'col_tab': col_tab_Device_Source_4}
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
            'request_id': read_request_id()}
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
            'request_id': read_request_id()}
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
            'col_tab': col_tab_Attribution_5}
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
            'request_id': read_request_id()}
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
            'request_id': read_request_id()}
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
            'col_tab': col_tab_Attribution_6}
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
            'request_id': read_request_id()}
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
            'request_id': read_request_id()}
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
            'col_tab': col_tab_Attribution_7}
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
            'request_id': read_request_id()}
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
            'request_id': read_request_id()}
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
            'col_tab': col_tab_Attribution_8}
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
            'request_id': read_request_id()}
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
            'request_id': read_request_id()}
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
            'col_tab': col_tab_Attribution_9}
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
            'request_id': read_request_id()}
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
            'request_id': read_request_id()}
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
            'col_tab': col_tab_Attribution_10}
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
            'request_id': read_request_id()}
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
            'request_id': read_request_id()}
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
            'col_tab': col_tab_Attribution_11}
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
            'request_id': read_request_id()}
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
            'request_id': read_request_id()}
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
            'col_tab': col_tab_Attribution_12}
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
            'request_id': read_request_id()}
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
            'request_id': read_request_id()}
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
            'col_tab': col_tab_Attribution_13}
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
            'request_id': read_request_id()}
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
            'request_id': read_request_id()}
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
            'message': f"Упал 🛑 - DAG visits_schedule_{read_request_id()}_{start_date_v}_{end_date_v}"}
    )

(task_clear_tmp_1 >> task_clear_hdfs_1 >> task_rm_13dir_hdfs_1 >> ocenka_vozmozhnisti_sozd_zaprosa_1 >> sozd_zaprosa_logov_1 >> get_status_1 >> task_save_request_id_1 >> task_extract_partsize_1 >> task_download_txt_1 >> task_merge_txt_1 >> clear_YaApi_query_1 >> task_move_bigTxt_1 >> task_start_pySpark_visits_1_basic_events_orc >> task_clear_tmp_2 >> task_clear_hdfs_2 >> ocenka_vozmozhnisti_sozd_zaprosa_2 >> sozd_zaprosa_logov_2 >> get_status_2 >> task_save_request_id_2 >> task_extract_partsize_2 >> task_download_txt_2 >> task_merge_txt_2 >> clear_YaApi_query_2 >> task_move_bigTxt_2 >> task_start_pySpark_visits_2_ecommerce_orc >> task_clear_tmp_3 >> task_clear_hdfs_3 >> ocenka_vozmozhnisti_sozd_zaprosa_3 >> sozd_zaprosa_logov_3 >> get_status_3 >> task_save_request_id_3 >> task_extract_partsize_3 >> task_download_txt_3 >> task_merge_txt_3 >> clear_YaApi_query_3 >> task_move_bigTxt_3 >> task_start_pySpark_visits_3_goals_orc >> task_clear_tmp_4 >> task_clear_hdfs_4 >> ocenka_vozmozhnisti_sozd_zaprosa_4 >> sozd_zaprosa_logov_4 >> get_status_4 >> task_save_request_id_4 >> task_extract_partsize_4 >> task_download_txt_4 >> task_merge_txt_4 >> clear_YaApi_query_4 >> task_move_bigTxt_4 >> task_start_pySpark_visits_4_device_source >> task_clear_tmp_5 >> task_clear_hdfs_5 >> ocenka_vozmozhnisti_sozd_zaprosa_5 >> sozd_zaprosa_logov_5 >> get_status_5 >> task_save_request_id_5 >> task_extract_partsize_5 >> task_download_txt_5 >> task_merge_txt_5 >> clear_YaApi_query_5 >> task_move_bigTxt_5 >> task_start_pySpark_visits_5_attribution >> task_clear_tmp_6 >> task_clear_hdfs_6 >> ocenka_vozmozhnisti_sozd_zaprosa_6 >> sozd_zaprosa_logov_6 >> get_status_6 >> task_save_request_id_6 >> task_extract_partsize_6 >> task_download_txt_6 >> task_merge_txt_6 >> clear_YaApi_query_6 >> task_move_bigTxt_6 >> task_start_pySpark_visits_6_attribution >> task_clear_tmp_7 >> task_clear_hdfs_7 >> ocenka_vozmozhnisti_sozd_zaprosa_7 >> sozd_zaprosa_logov_7 >> get_status_7 >> task_save_request_id_7 >> task_extract_partsize_7 >> task_download_txt_7 >> task_merge_txt_7 >> clear_YaApi_query_7 >> task_move_bigTxt_7 >> task_start_pySpark_visits_7_attribution >> task_clear_tmp_8 >> task_clear_hdfs_8 >> ocenka_vozmozhnisti_sozd_zaprosa_8 >> sozd_zaprosa_logov_8 >> get_status_8 >> task_save_request_id_8 >> task_extract_partsize_8 >> task_download_txt_8 >> task_merge_txt_8 >> clear_YaApi_query_8 >> task_move_bigTxt_8 >> task_start_pySpark_visits_8_attribution >> task_clear_tmp_9 >> task_clear_hdfs_9 >> ocenka_vozmozhnisti_sozd_zaprosa_9 >> sozd_zaprosa_logov_9 >> get_status_9 >> task_save_request_id_9 >> task_extract_partsize_9 >> task_download_txt_9 >> task_merge_txt_9 >> clear_YaApi_query_9 >> task_move_bigTxt_9 >> task_start_pySpark_visits_9_attribution >> task_clear_tmp_10 >> task_clear_hdfs_10 >> ocenka_vozmozhnisti_sozd_zaprosa_10 >> sozd_zaprosa_logov_10 >> get_status_10 >> task_save_request_id_10 >> task_extract_partsize_10 >> task_download_txt_10 >> task_merge_txt_10 >> clear_YaApi_query_10 >> task_move_bigTxt_10 >> task_start_pySpark_visits_10_attribution >> task_clear_tmp_11 >> task_clear_hdfs_11 >> ocenka_vozmozhnisti_sozd_zaprosa_11 >> sozd_zaprosa_logov_11 >> get_status_11 >> task_save_request_id_11 >> task_extract_partsize_11 >> task_download_txt_11 >> task_merge_txt_11 >> clear_YaApi_query_11 >> task_move_bigTxt_11 >> task_start_pySpark_visits_11_attribution >> task_clear_tmp_12 >> task_clear_hdfs_12 >> ocenka_vozmozhnisti_sozd_zaprosa_12 >> sozd_zaprosa_logov_12 >> get_status_12 >> task_save_request_id_12 >> task_extract_partsize_12 >> task_download_txt_12 >> task_merge_txt_12 >> clear_YaApi_query_12 >> task_move_bigTxt_12 >> task_start_pySpark_visits_12_attribution >> task_clear_tmp_13 >> task_clear_hdfs_13 >> ocenka_vozmozhnisti_sozd_zaprosa_13 >> sozd_zaprosa_logov_13 >> get_status_13 >> task_save_request_id_13 >> task_extract_partsize_13 >> task_download_txt_13 >> task_merge_txt_13 >> clear_YaApi_query_13 >> task_move_bigTxt_13 >> task_start_pySpark_visits_13_attribution >> task_clear_tmp_14 >> task_clear_hdfs_14 >> task_start_pySpark_join >> task_rm_13dir_hdfs_2 >> [tlgrm_allSuccess, tlgrm_oneFailed])
