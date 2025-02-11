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
from selenium.common.exceptions import NoSuchElementException
import datetime
import time
from multiprocessing import Process
from fractions import Fraction


def save_current_year_team_runner_data():
    '''
    현재 팀 수비 기록 가져오기
    '''
    def set_initial_page_setting() :
        driver.get('https://www.koreabaseball.com/Record/Team/Runner/Basic.aspx')
        driver.implicitly_wait(3)


    #############################
    # driver.implicitly_wait(3)
    set_initial_page_setting()

    year_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason'))
    year_selector.select_by_value(CURRENT_YEAR) # 최신부터 내려오기
    time.sleep(CONST_SLEEP_TIME)
    year_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason'))
    selected_year = [ option for option in year_selector.options if option.get_attribute("selected")]
    year = selected_year[0].text

    assert(year == CURRENT_YEAR)

    
    ########
    # BASIC
    ########
    basic_data = []



    td_data = driver.find_elements(by=By.TAG_NAME, value="td")
    # len(td_data_element) == 9

    for i in range(0,len(td_data),9):
        if((len(td_data)-i) < 9) : break

        team_name = td_data[i+1].text
        team_G = td_data[i+2].text
        team_SBA = td_data[i+3].text
        team_SB = td_data[i+4].text
        team_CS = td_data[i+5].text
        team_SB_PER = td_data[i+6].text
        team_OOB = td_data[i+7].text
        team_PKO = td_data[i+8].text

        basic_data.append(
            [year] +
            [team_name]+
            [team_G] +
            [team_SBA] +
            [team_SB] +
            [team_CS] +
            [team_SB_PER] +
            [team_OOB] +
            [team_PKO]
        )



    # saving df
    df = pd.DataFrame(basic_data,columns=('year','team_name', 'team_G', 'team_SBA', 'team_SB','team_CS','team_SB_PER','team_OOB','team_PKO'))

    df = df.replace("-",np.nan)
    df['year'] = df['year'].astype(int)
    df['team_G'] = df['team_G'].astype(int)
    df['team_SBA'] = df['team_SBA'].astype(int)
    df['team_SB'] = df['team_SB'].astype(int)
    df['team_CS'] = df['team_CS'].astype(int)
    df['team_SB_PER'] = df['team_SB_PER'].astype(float)
    df['team_OOB'] = df['team_OOB'].astype(int)
    df['team_PKO'] = df['team_PKO'].astype(int)

    save_df(
        df,
        os.path.join(DATASET_NAME,TEAM_DATASET_NAME,"runner",f"Team_Runner_{year}.parquet"),
        os.path.join(TEAM_DATASET_DIR,"runner",f"Team_Runner_{year}.parquet")
    )


    return 0

if __name__ == "__main__":
    try :
        # making directory if not existed
        if not IS_BLOB:
            runner_dir = os.path.join(TEAM_DATASET_DIR, "runner")
            if not os.path.exists(runner_dir):
                os.mkdir(runner_dir)

        st_time = time.time()
        save_current_year_team_runner_data()

            
        end_time = time.time()
        print(f"{end_time-st_time} s")


    finally:
        driver.quit()
