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
from fractions import Fraction

DYNAMIC_SLEEP_TIME = CONST_SLEEP_TIME
#####

def pitcher_daily_work(shared_number_list, index : int, attempt : int, driver):
    '''
    multiprocessing 돌리는 함수
    '''
    global DYNAMIC_SLEEP_TIME    

    try:
        while(len(shared_number_list) >= 1):
            pitcherID = shared_number_list[0]
            print(f"Process-{index} processing: {pitcherID}")
            get_n_save_pitcher_daily_data(pitcherID,driver)
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
            pitcher_daily_work(shared_number_list, index, attempt=attempt+1, driver=driver)
        else:
            print("exceed retry limit")
            exit(1)



def get_n_save_pitcher_daily_data(pitcherID : int, driver):
    '''
    현재부터 MIN_YEAR +1 년까지 일자별 기록 가져오기
    '''
    def set_initial_page_setting() :
        driver.get(f'https://www.koreabaseball.com/Record/Player/PitcherDetail/Daily.aspx?playerId={pitcherID}')
        driver.implicitly_wait(DYNAMIC_SLEEP_TIME)
        cat_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries'))
        cat_selector.select_by_index(0) # 0 -> 정규시즌
        driver.implicitly_wait(DYNAMIC_SLEEP_TIME)

    result = []
    # driver.get(f'https://www.koreabaseball.com/Record/Player/PitcherDetail/Daily.aspx?playerId={pitcherID}')
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
        
        if(len(td_data) >= 15):
            for i in range(0,len(td_data),15):
                temp_date = year+"."+td_data[i].text 
                stat_date = datetime.datetime.strptime(temp_date,format) # 게임 일자
                stat_opp = td_data[i+1].text # 상대
                stat_cat = td_data[i+2].text # 구분 (선발, 구원)
                stat_res = td_data[i+3].text # 결과 (승,패,홀,세)
                stat_game_ERA = td_data[i+4].text # 경기 ERA
                stat_TBF = td_data[i+5].text # 타자 수
                stat_IP = td_data[i+6].text # 이닝 수
                stat_IP_float = 0
                for item in stat_IP.split(" "):
                    fraction = Fraction(item)
                    stat_IP_float += float(fraction)
                stat_H = td_data[i+7].text # 피안타
                stat_HR = td_data[i+8].text # 피홈런
                stat_BB = td_data[i+9].text # 볼넷
                stat_HBP = td_data[i+10].text # 사구
                stat_SO = td_data[i+11].text # 삼진
                stat_R = td_data[i+12].text # 실점
                stat_ER = td_data[i+13].text # 자책점
                stat_season_ERA = td_data[i+14].text # 시즌 누적 ERA

                result.append(
                    [pitcherID]+
                    [stat_date]+
                    [stat_opp]+
                    [stat_cat]+
                    [stat_res]+
                    [stat_game_ERA]+
                    [stat_TBF]+
                    [stat_IP_float]+
                    [stat_H]+
                    [stat_HR]+
                    [stat_BB]+
                    [stat_HBP]+
                    [stat_SO]+
                    [stat_R]+
                    [stat_ER]+
                    [stat_season_ERA]
                )


    df = pd.DataFrame(result,columns=('id', 'date', 'opp', 'cat','res','gameERA','TBF','IP',
                                    'H','HR','BB','HBP','SO','R','ER','seasonERA'))

    df = df.replace("-",np.nan)
    df['id'] = df['id'].astype(int)
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d').dt.date
    df['gameERA'] = df['gameERA'].astype(float)
    df['TBF'] = df['TBF'].astype(int)
    df['IP'] = df['IP'].astype(float)
    df['H'] = df['H'].astype(int)
    df['HR'] = df['HR'].astype(int)
    df['BB'] = df['BB'].astype(int)
    df['HBP'] = df['HBP'].astype(int)
    df['SO'] = df['SO'].astype(int)
    df['R'] = df['R'].astype(int)
    df['ER'] = df['ER'].astype(int)
    df['seasonERA'] = df['seasonERA'].astype(float)

    daily_dir_path = os.path.join(PITCHER_DATASET_DIR, "pitcher_daily")
    save_df(
        df,
        os.path.join(DATASET_NAME,PITCHER_DATASET_NAME,"pitcher_daily",f"{pitcherID}_Daily.parquet"),
        os.path.join(daily_dir_path, f"{pitcherID}_Daily.parquet")
    )


if __name__ == "__main__":
    drivers = [driver]
    try:
        if NUM_PROCESS > 1:
            if IS_BLOB:
                for i in range(NUM_PROCESS-1):
                    d = webdriver.Remote(
                        command_executor=command_executor_url,
                        options=options
                    )
                    drivers.append(d)
            else:
                for i in range(NUM_PROCESS-1):
                    d = webdriver.Chrome(options=options)
                    drivers.append(d)

        if not IS_BLOB:
            daily_dir_path = os.path.join(PITCHER_DATASET_DIR, "pitcher_daily")
            if not os.path.exists(daily_dir_path):
                os.mkdir(daily_dir_path)

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
                process_list.append(Process(target=pitcher_daily_work, args=(shared_number_list[i],i,1,drivers[i])))
            else : 
                process_list.append(Process(target=pitcher_daily_work, args=(shared_number_list[i],i,1,drivers[i])))
                
        for process in process_list:
            process.start()
        
        for process in process_list:
            process.join()

        end_time = time.time()

        print(f"{end_time-st_time} s")
        
    finally:
        for d in drivers:
            d.quit()
