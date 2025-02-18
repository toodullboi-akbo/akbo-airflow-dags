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


def save_whole_year_team_fielder_data():
    '''
    현재부터 TEAM_MIN_YEAR +1까지 팀 수비 기록 가져오기
    '''
    def set_initial_page_setting() :
        driver.get('https://www.koreabaseball.com/Record/Team/Defense/Basic.aspx')
        driver.implicitly_wait(3)


    #############################
    # driver.implicitly_wait(3)
    set_initial_page_setting()

    year_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason'))
    for year_idx in range(len(year_selector.options)-1,-1,-1):
        year_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason'))
        year_selector.select_by_index(year_idx) # 최신부터 내려오기
        time.sleep(CONST_SLEEP_TIME)
        year_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason'))
        selected_year = [ option for option in year_selector.options if option.get_attribute("selected")]
        year = selected_year[0].text
        if(year == TEAM_MIN_YEAR) : break

        
        ########
        # BASIC
        ########
        basic_data = []



        td_data = driver.find_elements(by=By.TAG_NAME, value="td")
        # len(td_data_element) == 13

        for i in range(0,len(td_data),13):
            if((len(td_data)-i) < 13) : break

            team_name = td_data[i+1].text
            team_G = td_data[i+2].text
            team_E = td_data[i+3].text
            team_PKO = td_data[i+4].text
            team_PO = td_data[i+5].text
            team_A = td_data[i+6].text
            team_DP = td_data[i+7].text
            team_FPCT = td_data[i+8].text
            team_PB = td_data[i+9].text
            team_SB = td_data[i+10].text
            team_CS = td_data[i+11].text
            team_CS_PER = td_data[i+12].text

            basic_data.append(
                [year] +
                [team_name]+
                [team_G] +
                [team_E] +
                [team_PKO] +
                [team_PO] +
                [team_A] +
                [team_DP] +
                [team_FPCT] +
                [team_PB] +
                [team_SB] +
                [team_CS] +
                [team_CS_PER]
            )



        # saving df
        df = pd.DataFrame(basic_data,columns=('year','team_name', 'team_G', 'team_E', 'team_PKO','team_PO','team_A','team_DP','team_FPCT',
                                        'team_PB','team_SB','team_CS','team_CS_PER'))

        df = df.replace("-",np.nan)
        df['year'] = df['year'].astype('int32')
        df['team_G'] = df['team_G'].astype('int32')
        df['team_E'] = df['team_E'].astype('int32')
        df['team_PKO'] = df['team_PKO'].astype('int32')
        df['team_PO'] = df['team_PO'].astype('int32')
        df['team_A'] = df['team_A'].astype('int32')
        df['team_DP'] = df['team_DP'].astype('int32')
        df['team_FPCT'] = df['team_FPCT'].astype('float64')
        df['team_PB'] = df['team_PB'].astype('int32')
        df['team_SB'] = df['team_SB'].astype('int32')
        df['team_CS'] = df['team_CS'].astype('int32')
        df['team_CS_PER'] = df['team_CS_PER'].astype('float64')

        save_df(
            df,
            os.path.join(DATASET_NAME,TEAM_DATASET_NAME,"fielder",f"Team_Fielder_{year}.parquet"),
            os.path.join(TEAM_DATASET_DIR,"fielder",f"Team_Fielder_{year}.parquet")
        )


    return 0

if __name__ == "__main__":
    try :
        # making directory if not existed
        if not IS_BLOB:
            fielder_dir = os.path.join(TEAM_DATASET_DIR, "fielder")
            if not os.path.exists(fielder_dir):
                os.mkdir(fielder_dir)

        st_time = time.time()
        save_whole_year_team_fielder_data()

            
        end_time = time.time()
        print(f"{end_time-st_time} s")


    finally:
        driver.quit()
