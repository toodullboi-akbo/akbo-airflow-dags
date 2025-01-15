import sys
import os
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)

from __init__ import *
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
import datetime
import time
from multiprocessing import Process, Manager

DYNAMIC_SLEEP_TIME = CONST_SLEEP_TIME
#####


def pitcher_situation_work(shared_number_list, index : int, attempt : int):
    '''
    multiprocessing 돌리는 함수
    '''
    global DYNAMIC_SLEEP_TIME

    try:
        while(len(shared_number_list) >= 1):
            pitcherID = shared_number_list[0]
            print(f"Process-{index} processing: {pitcherID}")
            get_n_save_pitcher_situation_data(pitcherID)
            shared_number_list.pop(0)
        exit(0)
    except Exception as e:
        if attempt < MAX_RETRIES:
            DYNAMIC_SLEEP_TIME = DYNAMIC_SLEEP_TIME * 2
            print(f"ERROR while Process-{index} doing {shared_number_list[0]}")
            for item in traceback.format_exception(e):
                print(item)
            print("let's retry")
            time.sleep(SLEEP_TIME_BEFORE_RETRY)
            pitcher_situation_work(shared_number_list, index, attempt=attempt+1)
        else:
            print("exceed retry limit")
            exit(1)


def get_n_save_pitcher_situation_data(pitcherID : int):
    '''
    현재부터 MIN_YEAR+1 년까지 상황별 기록 가져오기
    '''
    def set_initial_page_setting() :
        driver.get(f'https://www.koreabaseball.com/Record/Player/PitcherDetail/Situation.aspx?playerId={pitcherID}')
        driver.implicitly_wait(DYNAMIC_SLEEP_TIME)
        cat_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries'))
        cat_selector.select_by_index(0) # 0 -> 정규시즌
        driver.implicitly_wait(DYNAMIC_SLEEP_TIME)

    result = []

    # driver.get(f'https://www.koreabaseball.com/Record/Player/PitcherDetail/Situation.aspx?playerId={pitcherID}')
    # driver.implicitly_wait(3)
    set_initial_page_setting()
    year_selector = Select(driver.find_element(by=By.NAME, value="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlYear"))

    for year_idx in range(0,len(year_selector.options)):
        year_selector = Select(driver.find_element(by=By.NAME, value="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlYear"))
        year_selector.select_by_index(year_idx)
        driver.implicitly_wait(DYNAMIC_SLEEP_TIME)

        year_data = driver.find_element(by=By.XPATH, value='//*[@id="contents"]/div[2]/div[2]/h6')
        year = year_data.text.split(' ')[0]

        if year == MIN_YEAR : break # MIN_YEAR+1 부터는 안 가져와

        td_data = driver.find_elements(by=By.TAG_NAME, value="td")

        if(len(td_data) >= 11):
            i = 0
            while i < len(td_data):
                while "기록" in td_data[i].text:
                    i += 1
                stat_category = td_data[i].text # 구분
                stat_H = td_data[i+1].text # 안타
                stat_2B = td_data[i+2].text # 2루타
                stat_3B = td_data[i+3].text # 3루타
                stat_HR = td_data[i+4].text # 홈런
                stat_BB = td_data[i+5].text # 볼넷
                stat_HBP = td_data[i+6].text # 사구
                stat_SO = td_data[i+7].text # 삼진
                stat_WP = td_data[i+8].text # 폭투
                stat_BK = td_data[i+9].text # 보크
                stat_AVG = td_data[i+10].text # 피안타율


                result.append(
                    [pitcherID]+
                    [year]+
                    [stat_category]+
                    [stat_H]+
                    [stat_2B]+
                    [stat_3B]+
                    [stat_HR]+
                    [stat_BB]+
                    [stat_HBP]+
                    [stat_SO]+
                    [stat_WP]+
                    [stat_BK]+
                    [stat_AVG]
                )

                i += 11

    df = pd.DataFrame(result,columns=('id', 'year','category', 'H', '2B','3B','HR','BB','HBP',
                                    'SO','WP','BK','AVG'))


    df = df.replace("-",np.nan)
    df['id'] = df['id'].astype(int)
    df['year'] = df['year'].astype(int)
    df['H'] = df['H'].astype(int)
    df['2B'] = df['2B'].astype(int)
    df['3B'] = df['3B'].astype(int)
    df['HR'] = df['HR'].astype(int)
    df['BB'] = df['BB'].astype(int)
    df['HBP'] = df['HBP'].astype(int)
    df['SO'] = df['SO'].astype(int)
    df['WP'] = df['WP'].astype(int)
    df['BK'] = df['BK'].astype(int)
    df['AVG'] = df['AVG'].astype(float)

    if IS_BLOB:
        blob_name_path = os.path.join(DATASET_NAME,PITCHER_DATASET_NAME,"pitcher_situation",f"{pitcherID}_Situation.parquet")
        parquet_data = df.to_parquet(engine="pyarrow", index=False)

        wasb_hook.load_string(
            string_data=parquet_data,
            container_name=container_name,
            blob_name=blob_name_path,
            overwrite=True
        )
    else:
        situation_dir_path = os.path.join(PITCHER_DATASET_DIR, "pitcher_situation")
        situation_file_path = os.path.join(situation_dir_path, f"{pitcherID}_Situation.parquet")

        df.to_parquet(situation_file_path, engine="pyarrow",index=False)


if __name__ == "__main__":
    try:
        if not IS_BLOB:
            situation_dir_path = os.path.join(PITCHER_DATASET_DIR, "pitcher_situation")
            if not os.path.exists(situation_dir_path):
                os.mkdir(situation_dir_path)

        st_time = time.time()

        if IS_BLOB:
            csv_data = wasb_hook.read_file(
                container_name=container_name,
                blob_name=ENTIRE_PITCHER_NUMBER_NAME_PATH
            )
            csv_file = StringIO(csv_data)
            df = pd.read_csv(csv_file)
        else:
            df = pd.read_csv(ENTIRE_PITCHER_NUMBER_PATH,encoding="utf-8")
        
        pitcher_number_list = df["Numbers"].to_list()

        print(f"number of distinct pitcher ::: {len(pitcher_number_list)}")
        
        manager = Manager()
        shared_number_list = [] # number_list 쪼개서 보관 예정

        split_index = len(pitcher_number_list)//NUM_PROCESS
        process_list = []

        for i in range(0, NUM_PROCESS):
            if i == NUM_PROCESS-1 : 
                shared_number_list.append(manager.list(pitcher_number_list[split_index*i:]))
            else : 
                shared_number_list.append(manager.list(pitcher_number_list[split_index*i:split_index*(i+1)]))
        
        for i in range(0, NUM_PROCESS):
            if i == NUM_PROCESS-1 : 
                process_list.append(Process(target=pitcher_situation_work, args=(shared_number_list[i],i,1)))
            else : 
                process_list.append(Process(target=pitcher_situation_work, args=(shared_number_list[i],i,1)))

        for process in process_list:
            process.start()
        
        for process in process_list:
            process.join()

        end_time = time.time()

        print(f"{end_time-st_time} s")
    finally:
        driver.quit()
