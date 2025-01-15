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

def batter_daily_work(shared_number_list, index : int, attempt : int):
    '''
    multiprocessing 돌리는 함수
    '''
    global DYNAMIC_SLEEP_TIME   
    try:
        while(len(shared_number_list) >= 1):
            batter_ID = shared_number_list[0]
            print(f"Process-{index} processing: {batter_ID}")
            get_n_save_batter_daily_data(batter_ID)
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
            batter_daily_work(shared_number_list, index, attempt=attempt+1)
        else:
            print("exceed retry limit")
            exit(1)


def get_n_save_batter_daily_data(batterID : int):
    '''
    현재부터 MIN_YEAR+1 까지 일자별 기록 가져오기
    '''
    def set_initial_page_setting() :
        driver.get(f'https://www.koreabaseball.com/Record/Player/HitterDetail/Daily.aspx?playerId={batterID}')
        driver.implicitly_wait(DYNAMIC_SLEEP_TIME)
        cat_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries'))
        cat_selector.select_by_index(0) # 0 -> 정규시즌
        driver.implicitly_wait(DYNAMIC_SLEEP_TIME)
    
    result = []

    # driver.get(f'https://www.koreabaseball.com/Record/Player/HitterDetail/Daily.aspx?playerId={batterID}')
    # driver.implicitly_wait(3)
    set_initial_page_setting()
    
    year_selector = Select(driver.find_element(by=By.NAME, value="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlYear"))

    for year_idx in range(0,len(year_selector.options)):
        year_selector = Select(driver.find_element(by=By.NAME, value="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlYear"))
        year_selector.select_by_index(year_idx)
        driver.implicitly_wait(DYNAMIC_SLEEP_TIME)

        year_data = driver.find_element(by=By.XPATH, value='//*[@id="contents"]/div[2]/div[2]/h6')
        year = year_data.text.split(' ')[0][:-1]
        
        if year == MIN_YEAR : break # MIN_YEAR 부터는 안 가져와
        
        td_data = driver.find_elements(by=By.TAG_NAME, value="td")
        format = '%Y.%m.%d'
        
        if(len(td_data) >= 18):
            for i in range(0,len(td_data),18):
                while "기록" in td_data[i].text:
                    i += 1

                temp_date = year+"."+td_data[i].text 
                stat_date = datetime.datetime.strptime(temp_date,format) # 게임 일자
                stat_opp = td_data[i+1].text # 상대
                stat_PA = td_data[i+3].text # 타석
                stat_AB = td_data[i+4].text # 타수
                stat_R = td_data[i+5].text # 득점
                stat_H = td_data[i+6].text # 안타
                stat_2B = td_data[i+7].text # 2루타
                stat_3B = td_data[i+8].text # 3루타
                stat_HR = td_data[i+9].text # 홈런
                stat_RBI = td_data[i+10].text # 타점
                stat_SB = td_data[i+11].text # 도루
                stat_CS = td_data[i+12].text # 도루 실패
                stat_BB = td_data[i+13].text # 볼넷
                stat_HBP = td_data[i+14].text # 사구
                stat_SO = td_data[i+15].text # 삼진
                stat_GDP = td_data[i+16].text # 병살타
                stat_season_AVG = td_data[i+17].text # 시즌 누적 타율

                result.append(
                    [batterID]+
                    [stat_date]+
                    [stat_opp]+
                    [stat_PA]+
                    [stat_AB]+
                    [stat_R]+
                    [stat_H]+
                    [stat_2B]+
                    [stat_3B]+
                    [stat_HR]+
                    [stat_RBI]+
                    [stat_SB]+
                    [stat_CS]+
                    [stat_BB]+
                    [stat_HBP]+
                    [stat_SO]+
                    [stat_GDP]+
                    [stat_season_AVG]
                )


    df = pd.DataFrame(result,columns=('id', 'date', 'opp', 'PA','AB','R','H','2B',
                                    '3B','HR','RBI','SB','CS','BB','HBP','SO','GDP','seasonAVG'))

    df = df.replace("-",value=np.nan)
    df['id'] = df['id'].astype(int)
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d').dt.date
    df['PA'] = df['PA'].astype(int)
    df['AB'] = df['AB'].astype(int)
    df['R'] = df['R'].astype(int)
    df['H'] = df['H'].astype(int)
    df['2B'] = df['2B'].astype(int)
    df['3B'] = df['3B'].astype(int)
    df['HR'] = df['HR'].astype(int)
    df['RBI'] = df['RBI'].astype(int)
    df['SB'] = df['SB'].astype(int)
    df['CS'] = df['CS'].astype(int)
    df['BB'] = df['BB'].astype(int)
    df['HBP'] = df['HBP'].astype(int)
    df['SO'] = df['SO'].astype(int)
    df['GDP'] = df['GDP'].astype(int)
    df['seasonAVG'] = df['seasonAVG'].astype(float)

    if IS_BLOB:
        blob_name_path = os.path.join(DATASET_NAME,BATTER_DATASET_NAME,"batter_daily",f"{batterID}_Daily.parquet")
        parquet_data = df.to_parquet(engine="pyarrow", index=False)

        wasb_hook.load_string(
            string_data=parquet_data,
            container_name=container_name,
            blob_name=blob_name_path,
            overwrite=True
        )

    else:
        daily_dir_path = os.path.join(BATTER_DATASET_DIR, "batter_daily")
        daily_file_path = os.path.join(daily_dir_path, f"{batterID}_Daily.parquet")

        df.to_parquet(daily_file_path, engine="pyarrow",index=False)



if __name__ == "__main__":
    try:
        if not IS_BLOB:
            daily_dir_path = os.path.join(BATTER_DATASET_DIR, "batter_daily")
            if not os.path.exists(daily_dir_path):
                os.mkdir(daily_dir_path)
        
        st_time = time.time()

        if IS_BLOB:
            csv_data = wasb_hook.read_file(
                container_name=container_name,
                blob_name=ENTIRE_BATTER_NUMBER_NAME_PATH
            )
            csv_file = StringIO(csv_data)
            df = pd.read_csv(csv_file)
        else:
            df = pd.read_csv(ENTIRE_BATTER_NUMBER_PATH, encoding="utf-8")
        batter_number_list = df["Numbers"].to_list()

        print(f"number of distinct batter ::: {len(batter_number_list)}")

        manager = Manager()
        shared_number_list = [] # number_list 쪼개서 보관 예정

        split_index = len(batter_number_list)//NUM_PROCESS
        process_list = []

        for i in range(0, NUM_PROCESS):
            if i == NUM_PROCESS-1 : 
                shared_number_list.append(manager.list(batter_number_list[split_index*i:]))
            else : 
                shared_number_list.append(manager.list(batter_number_list[split_index*i:split_index*(i+1)]))
        

        for i in range(0, NUM_PROCESS):
            if i == NUM_PROCESS-1 : 
                process_list.append(Process(target=batter_daily_work, args=(shared_number_list[i],i,1)))
            else : 
                process_list.append(Process(target=batter_daily_work, args=(shared_number_list[i],i,1)))

        for process in process_list:
            process.start()
        
        for process in process_list:
            process.join()

        end_time = time.time()

        print(f"{end_time-st_time} s")
    finally:
        driver.quit()
