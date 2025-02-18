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


def save_current_year_team_pitcher_data():
    '''
    현재 팀 투수 기록 가져오기
    '''
    def set_initial_page_setting() :
        driver.get('https://www.koreabaseball.com/Record/Team/Pitcher/Basic1.aspx')
        driver.implicitly_wait(3)
        cat_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries$ddlSeries'))
        cat_selector.select_by_index(0) # 0 th idx -> 정규시즌
        time.sleep(CONST_SLEEP_TIME)


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
    # BASIC-1
    ########
    basic_1_data = []



    td_data = driver.find_elements(by=By.TAG_NAME, value="td")
    # len(td_data_element) == 18

    for i in range(0,len(td_data),18):
        if((len(td_data)-i) < 18) : break

        team_name = td_data[i+1].text
        team_ERA = td_data[i+2].text
        team_G = td_data[i+3].text
        team_W = td_data[i+4].text
        team_L = td_data[i+5].text
        team_SV = td_data[i+6].text
        team_HLD = td_data[i+7].text
        team_WPCT = td_data[i+8].text
        team_IP = td_data[i+9].text
        team_IP_float = 0
        for item in team_IP.split(" "):
            fraction = Fraction(item)
            team_IP_float += float(fraction)

        team_H = td_data[i+10].text
        team_HR = td_data[i+11].text
        team_BB = td_data[i+12].text
        team_HBP = td_data[i+13].text
        team_SO = td_data[i+14].text
        team_R = td_data[i+15].text
        team_ER = td_data[i+16].text
        team_WHIP = td_data[i+17].text

        basic_1_data.append(
            [year] +
            [team_name]+
            [team_ERA] +
            [team_G] +
            [team_W] +
            [team_L] +
            [team_SV]+
            [team_HLD] +
            [team_WPCT]+
            [team_IP_float]+
            [team_H] +
            [team_HR]+
            [team_BB]+
            [team_HBP] +
            [team_SO]+
            [team_R] +
            [team_ER]+
            [team_WHIP]
        )



    ########
    # BASIC-2
    ########
    next_button = driver.find_element(by=By.CLASS_NAME, value="next")
    next_button.send_keys(Keys.RETURN)
    driver.implicitly_wait(10)

    basic_2_data = []
        
    td_data = driver.find_elements(by=By.TAG_NAME, value="td")
    # len(td_data_element) == 17

    for i in range(0,len(td_data),17):
        if((len(td_data)-i) < 17) : break

        team_CG = td_data[i+3].text
        team_SHO = td_data[i+4].text
        team_QS = td_data[i+5].text
        team_BSV = td_data[i+6].text
        team_TBF = td_data[i+7].text
        team_NP = td_data[i+8].text
        team_AVG = td_data[i+9].text
        team_2B = td_data[i+10].text
        team_3B = td_data[i+11].text
        team_SAC = td_data[i+12].text
        team_SF = td_data[i+13].text
        team_IBB = td_data[i+14].text
        team_WP = td_data[i+15].text
        team_BK = td_data[i+16].text

        basic_2_data.append(
            [team_CG] +
            [team_SHO]+
            [team_QS] +
            [team_BSV]+
            [team_TBF]+
            [team_NP] +
            [team_AVG]+
            [team_2B] +
            [team_3B] +
            [team_SAC]+
            [team_SF] +
            [team_IBB]+
            [team_WP] +
            [team_BK] 
        )

    # saving df

    df = pd.DataFrame({
        "year" : list(map(lambda x : x[0], basic_1_data)),
        "team_name" : list(map(lambda x : x[1], basic_1_data)),
        "team_ERA" : list(map(lambda x : x[2], basic_1_data)),
        "team_G" : list(map(lambda x : x[3], basic_1_data)),
        "team_W" : list(map(lambda x : x[4], basic_1_data)),
        "team_L" : list(map(lambda x : x[5], basic_1_data)),
        "team_SV" : list(map(lambda x : x[6], basic_1_data)),
        "team_HLD" : list(map(lambda x : x[7], basic_1_data)),
        "team_WPCT" : list(map(lambda x : x[8], basic_1_data)),
        "team_IP" : list(map(lambda x : x[9], basic_1_data)),
        "team_H" : list(map(lambda x : x[10], basic_1_data)),
        "team_HR" : list(map(lambda x : x[11], basic_1_data)) ,
        "team_BB" : list(map(lambda x : x[12], basic_1_data)),
        "team_HBP" : list(map(lambda x : x[13], basic_1_data)),
        "team_SO" : list(map(lambda x : x[14], basic_1_data)),
        "team_R" : list(map(lambda x : x[15], basic_1_data)),
        "team_ER" : list(map(lambda x : x[16], basic_1_data)),
        "team_WHIP" : list(map(lambda x : x[17], basic_1_data)),

        "team_CG" : list(map(lambda x : x[0], basic_2_data)),
        "team_SHO" : list(map(lambda x : x[1], basic_2_data)),
        "team_QS" : list(map(lambda x : x[2], basic_2_data)),
        "team_BSV" : list(map(lambda x : x[3], basic_2_data)),
        "team_TBF" : list(map(lambda x : x[4], basic_2_data)),
        "team_NP" : list(map(lambda x : x[5], basic_2_data)),
        "team_AVG" : list(map(lambda x : x[6], basic_2_data)),
        "team_2B" : list(map(lambda x : x[7], basic_2_data)),
        "team_3B" : list(map(lambda x : x[8], basic_2_data)),
        "team_SAC" : list(map(lambda x : x[9], basic_2_data)),
        "team_SF" : list(map(lambda x : x[10], basic_2_data)),
        "team_IBB" : list(map(lambda x : x[11], basic_2_data)),
        "team_WP" : list(map(lambda x : x[12], basic_2_data)),
        "team_BK" : list(map(lambda x : x[13], basic_2_data)),

    })

    df = df.replace("-",np.nan)
    df['year'] = df['year'].astype('int32')
    df['team_ERA'] = df['team_ERA'].astype('float64')
    df['team_G'] = df['team_G'].astype('int32')
    df['team_W'] = df['team_W'].astype('int32')
    df['team_L'] = df['team_L'].astype('int32')
    df['team_SV'] = df['team_SV'].astype('int32')
    df['team_HLD'] = df['team_HLD'].astype('int32')
    df['team_WPCT'] = df['team_WPCT'].astype('float64')
    df['team_IP'] = df['team_IP'].astype('float64')
    df['team_H'] = df['team_H'].astype('int32')
    df['team_HR'] = df['team_HR'].astype('int32')
    df['team_BB'] = df['team_BB'].astype('int32')
    df['team_HBP'] = df['team_HBP'].astype('int32')
    df['team_SO'] = df['team_SO'].astype('int32')
    df['team_R'] = df['team_R'].astype('int32')
    df['team_ER'] = df['team_ER'].astype('int32')
    df['team_WHIP'] = df['team_WHIP'].astype('float64')
    df['team_CG'] = df['team_CG'].astype('int32')
    df['team_SHO'] = df['team_SHO'].astype('int32')
    df['team_QS'] = df['team_QS'].astype('int32')
    df['team_BSV'] = df['team_BSV'].astype('int32')
    df['team_TBF'] = df['team_TBF'].astype('int32')
    df['team_NP'] = df['team_NP'].astype('int32')
    df['team_AVG'] = df['team_AVG'].astype('float64')
    df['team_2B'] = df['team_2B'].astype('int32')
    df['team_3B'] = df['team_3B'].astype('int32')
    df['team_SAC'] = df['team_SAC'].astype('int32')
    df['team_SF'] = df['team_SF'].astype('int32')
    df['team_IBB'] = df['team_IBB'].astype('int32')
    df['team_WP'] = df['team_WP'].astype('int32')
    df['team_BK'] = df['team_BK'].astype('int32')

    save_df(
        df,
        os.path.join(DATASET_NAME,TEAM_DATASET_NAME,"pitcher",f"Team_Pitcher_{year}.parquet"),
        os.path.join(TEAM_DATASET_DIR,"pitcher",f"Team_Pitcher_{year}.parquet")
    )

    prev_button = driver.find_element(by=By.CLASS_NAME, value="prev")
    prev_button.send_keys(Keys.RETURN)
    driver.implicitly_wait(10)

    return 0

if __name__ == "__main__":
    try :
        # making directory if not existed
        if not IS_BLOB:
            pitcher_dir = os.path.join(TEAM_DATASET_DIR, "pitcher")
            if not os.path.exists(pitcher_dir):
                os.mkdir(pitcher_dir)

        st_time = time.time()
        save_current_year_team_pitcher_data()

            
        end_time = time.time()
        print(f"{end_time-st_time} s")


    finally:
        driver.quit()
