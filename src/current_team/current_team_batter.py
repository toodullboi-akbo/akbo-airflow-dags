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


def save_current_year_team_batter_data():
    '''
    현재 팀 타자 기록 가져오기
    '''
    def set_initial_page_setting() :
        driver.get('https://www.koreabaseball.com/Record/Team/Hitter/Basic1.aspx')
        driver.implicitly_wait(3)
        cat_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries$ddlSeries'))
        cat_selector.select_by_index(0) # 0 th idx -> 정규시즌
        time.sleep(CONST_SLEEP_TIME)


    #############################
    # driver.get('https://www.koreabaseball.com/Record/Player/HitterBasic/Detail1.aspx')
    # driver.implicitly_wait(3)
    set_initial_page_setting()

    year_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason'))
    year_selector.select_by_value(CURRENT_YEAR) 
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
    # len(td_data_element) == 15

    for i in range(0,len(td_data),15):
        if((len(td_data)-i) < 15) : break

        team_name = td_data[i+1].text
        team_AVG = td_data[i+2].text
        team_G = td_data[i+3].text
        team_PA = td_data[i+4].text
        team_AB = td_data[i+5].text
        team_R = td_data[i+6].text
        team_H = td_data[i+7].text
        team_2B = td_data[i+8].text
        team_3B = td_data[i+9].text
        team_HR = td_data[i+10].text
        team_TB = td_data[i+11].text
        team_RBI = td_data[i+12].text
        team_SAC = td_data[i+13].text
        team_SF = td_data[i+14].text
        
        basic_1_data.append(
            [year] +
            [team_name] +
            [team_AVG]+
            [team_G]+
            [team_PA]+
            [team_AB]+
            [team_R] +
            [team_H] +
            [team_2B]+
            [team_3B] +
            [team_HR]+
            [team_TB]+
            [team_RBI]+
            [team_SAC]+
            [team_SF]   
        )



    ########
    # BASIC-2
    ########
    next_button = driver.find_element(by=By.CLASS_NAME, value="next")
    next_button.send_keys(Keys.RETURN)
    driver.implicitly_wait(10)

    basic_2_data = []
        
    td_data = driver.find_elements(by=By.TAG_NAME, value="td")
    # len(td_data_element) == 14

    for i in range(0,len(td_data),14):
        if((len(td_data)-i) < 14) : break

        team_BB = td_data[i+3].text
        team_IBB = td_data[i+4].text
        team_HBP = td_data[i+5].text
        team_SO = td_data[i+6].text
        team_GDP = td_data[i+7].text
        team_SLG = td_data[i+8].text
        team_OBP = td_data[i+9].text
        team_OPS = td_data[i+10].text
        team_MH = td_data[i+11].text
        team_RISP = td_data[i+12].text
        team_PH_BA = td_data[i+13].text

        basic_2_data.append(
            [team_BB]+
            [team_IBB]+
            [team_HBP]+
            [team_SO]+
            [team_GDP]+
            [team_SLG] +
            [team_OBP] +
            [team_OPS]+
            [team_MH] +
            [team_RISP]+
            [team_PH_BA]
        )

    # saving df

    df = pd.DataFrame({
        "year" : list(map(lambda x : x[0], basic_1_data)),
        "team_name" : list(map(lambda x : x[1], basic_1_data)),
        "team_AVG" : list(map(lambda x : x[2], basic_1_data)),
        "team_G" : list(map(lambda x : x[3], basic_1_data)),
        "team_PA" : list(map(lambda x : x[4], basic_1_data)),
        "team_AB" : list(map(lambda x : x[5], basic_1_data)),
        "team_R" : list(map(lambda x : x[6], basic_1_data)),
        "team_H" : list(map(lambda x : x[7], basic_1_data)),
        "team_2B" : list(map(lambda x : x[8], basic_1_data)),
        "team_3B" : list(map(lambda x : x[9], basic_1_data)),
        "team_HR" : list(map(lambda x : x[10], basic_1_data)),
        "team_TB" : list(map(lambda x : x[11], basic_1_data)) ,
        "team_RBI" : list(map(lambda x : x[12], basic_1_data)),
        "team_SAC" : list(map(lambda x : x[13], basic_1_data)),
        "team_SF" : list(map(lambda x : x[14], basic_1_data)),

        "team_BB" : list(map(lambda x : x[0], basic_2_data)),
        "team_IBB" : list(map(lambda x : x[1], basic_2_data)),
        "team_HBP" : list(map(lambda x : x[2], basic_2_data)),
        "team_SO" : list(map(lambda x : x[3], basic_2_data)),
        "team_GDP" : list(map(lambda x : x[4], basic_2_data)),
        "team_SLG" : list(map(lambda x : x[5], basic_2_data)),
        "team_OBP" : list(map(lambda x : x[6], basic_2_data)),
        "team_OPS" : list(map(lambda x : x[7], basic_2_data)),
        "team_MH" : list(map(lambda x : x[8], basic_2_data)),
        "team_RISP" : list(map(lambda x : x[9], basic_2_data)),
        "team_PH_BA" : list(map(lambda x : x[10], basic_2_data)),

    })

    df = df.replace("-",np.nan)
    df['year'] = df['year'].astype('int32')
    df['team_AVG'] = df['team_AVG'].astype('float64')
    df['team_G'] = df['team_G'].astype('int32')
    df['team_PA'] = df['team_PA'].astype('int32')
    df['team_AB'] = df['team_AB'].astype('int32')
    df['team_R'] = df['team_R'].astype('int32')
    df['team_H'] = df['team_H'].astype('int32')
    df['team_2B'] = df['team_2B'].astype('int32')
    df['team_3B'] = df['team_3B'].astype('int32')
    df['team_HR'] = df['team_HR'].astype('int32')
    df['team_TB'] = df['team_TB'].astype('int32')
    df['team_RBI'] = df['team_RBI'].astype('int32')
    df['team_SAC'] = df['team_SAC'].astype('int32')
    df['team_SF'] = df['team_SF'].astype('int32')
    df['team_BB'] = df['team_BB'].astype('int32')
    df['team_IBB'] = df['team_IBB'].astype('int32')
    df['team_HBP'] = df['team_HBP'].astype('int32')
    df['team_SO'] = df['team_SO'].astype('int32')
    df['team_GDP'] = df['team_GDP'].astype('int32')
    df['team_SLG'] = df['team_SLG'].astype('float64')
    df['team_OBP'] = df['team_OBP'].astype('float64')
    df['team_OPS'] = df['team_OPS'].astype('float64')
    df['team_MH'] = df['team_MH'].astype('int32')
    df['team_RISP'] = df['team_RISP'].astype('float64')
    df['team_PH_BA'] = df['team_PH_BA'].astype('float64')

    save_df(
        df,
        os.path.join(DATASET_NAME,CURRENT_DATASET_NAME,CURRENT_YEAR,TEAM_DATASET_NAME,"batter",f"Team_Batter_{year}.parquet"),
        os.path.join(TEAM_DATASET_DIR,"batter",f"Team_Batter_{year}.parquet")
    )

    prev_button = driver.find_element(by=By.CLASS_NAME, value="prev")
    prev_button.send_keys(Keys.RETURN)
    driver.implicitly_wait(10)

    return 0

if __name__ == "__main__":
    try :
        # making directory if not existed
        if not IS_BLOB:
            batter_dir = os.path.join(TEAM_DATASET_DIR, "batter")
            if not os.path.exists(batter_dir):
                os.mkdir(batter_dir)

        st_time = time.time()
        save_current_year_team_batter_data()

            
        end_time = time.time()
        print(f"{end_time-st_time} s")


    finally:
        driver.quit()
