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

def get_n_save_legacy_pitcher_data() -> set:
    '''
    과거 스탯 투수 기록 가져오기

    return set ::: 선수들의 고유번호가 담긴 set
    '''
    def set_initial_page_setting() :
        driver.get('https://www.koreabaseball.com/Record/Player/PitcherBasic/BasicOld.aspx')
        driver.implicitly_wait(3)
        cat_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries$ddlSeries'))
        cat_selector.select_by_index(0) # 0 th idx -> 정규시즌
        time.sleep(CONST_SLEEP_TIME)

    def move_to_page(page : int) :
        if page == -1:
            # 제일 처음으로 가
            first_page_button = driver.find_element(by=By.ID, value='cphContents_cphContents_cphContents_ucPager_btnFirst')
            first_page_button.send_keys(Keys.RETURN)
        else : 
            go_to_page = 5 if page == 0 else page
            move_page_button = driver.find_element(by=By.ID, value=f"cphContents_cphContents_cphContents_ucPager_btnNo{go_to_page}")
            move_page_button.click()
        # TODO : sleep 하지 않고 impicitly_wait 하고 싶다 ..
        time.sleep(CONST_SLEEP_TIME)

    def get_max_page() -> int:
        try:
            last_page_button = driver.find_element(by=By.ID, value='cphContents_cphContents_cphContents_ucPager_btnLast')
            last_page_button.send_keys(Keys.RETURN)
            time.sleep(CONST_SLEEP_TIME)
            paging_div = driver.find_element(by=By.CLASS_NAME, value="paging")
            paging_buttons = paging_div.find_elements(by=By.XPATH, value="./child::a")
            max_page = paging_buttons[-2].text
            paging_buttons[0].send_keys(Keys.RETURN)
            time.sleep(CONST_SLEEP_TIME)

        except NoSuchElementException:
            # len(page) == 1
            return 1

        return int(max_page)



    pitcher_number_list = set([])
    max_page = -1

    #############################
    set_initial_page_setting()
    
    year_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason'))
    for year_idx in range(len(year_selector.options)-20,-1,-1):
        year_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason'))
        year_selector.select_by_index(year_idx) 
        time.sleep(CONST_SLEEP_TIME)
        year_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason'))
        selected_year = [ option for option in year_selector.options if option.get_attribute("selected")]
        year = selected_year[0].text
        if(int(year) > int(LEGACY_YEAR)) : continue
        if(int(year) == int(LEGACY_YEAR)) :
            sort_button = driver.find_element(by=By.XPATH, value='//*[@id="cphContents_cphContents_cphContents_udpContent"]/div[2]/table/thead/tr/th[5]/a')
            sort_button.send_keys(Keys.RETURN)
            time.sleep(CONST_SLEEP_TIME)       

        ########
        # BASIC
        ########
        basic_data = []
        max_page = get_max_page()
        for current_page in range(1,max_page+1):
            modulared_current_page = current_page % 5
            if modulared_current_page == 1 and current_page != 1:
                next_page_button = driver.find_element(by=By.ID, value='cphContents_cphContents_cphContents_ucPager_btnNext')
                next_page_button.send_keys(Keys.RETURN)
                time.sleep(CONST_SLEEP_TIME)

            if modulared_current_page != 1 :
                move_to_page(modulared_current_page)
            
            td_data = driver.find_elements(by=By.TAG_NAME, value="td")
            # len(td_data_element) == 21

            for i in range(0,len(td_data),21):
                # 여기서 이름과 번호도 기록
                name_a = td_data[i+1].find_element(by=By.XPATH, value="./child::a")
                name = name_a.text
                number = name_a.get_attribute('href').split('=')[-1]
                team_name = td_data[i+2].text # 팀명
                stat_ERA = td_data[i+3].text # ERA
                stat_G = td_data[i+4].text # 경기수
                stat_CG = td_data[i+5].text # 완투
                stat_SHO = td_data[i+6].text # 완봉
                stat_W = td_data[i+7].text # 승리
                stat_L = td_data[i+8].text # 패배
                stat_SV = td_data[i+9].text # 세이브
                stat_HLD = td_data[i+10].text # 홀드
                stat_WPCT = td_data[i+11].text # 승률
                stat_TBF = td_data[i+12].text # 타자수
                stat_IP = td_data[i+13].text # 이닝
                stat_IP_float = 0
                for item in stat_IP.split(" "):
                    fraction = Fraction(item)
                    stat_IP_float += float(fraction)
                stat_H = td_data[i+14].text # 피안타
                stat_HR = td_data[i+15].text # 피홈런
                stat_BB = td_data[i+16].text # 볼넷
                stat_HBP = td_data[i+17].text # 사구
                stat_SO = td_data[i+18].text # 삼진
                stat_R = td_data[i+19].text # 실점
                stat_ER = td_data[i+20].text # 자책점

                basic_data.append(
                    [year]+
                    [name]+
                    [number]+
                    [team_name]+
                    [stat_ERA] +
                    [stat_G] +
                    [stat_CG]+
                    [stat_SHO] +
                    [stat_W] +
                    [stat_L] +
                    [stat_SV]+
                    [stat_HLD] +
                    [stat_WPCT]+
                    [stat_TBF] +
                    [stat_IP_float]+
                    [stat_H] +
                    [stat_HR]+
                    [stat_BB]+
                    [stat_HBP] +
                    [stat_SO]+
                    [stat_R]+
                    [stat_ER]
                )

                pitcher_number_list.add(number)




        df = pd.DataFrame({
            "is_legacy" : ["Y"] * len(basic_data),
            "year" : list(map(lambda x : x[0], basic_data)),
            "name" : list(map(lambda x : x[1], basic_data)),
            "id" : list(map(lambda x : x[2], basic_data)),
            "team" : list(map(lambda x : x[3], basic_data)),
            "ERA" : list(map(lambda x : x[4], basic_data)),
            "G" : list(map(lambda x : x[5], basic_data)),
            "CG" : list(map(lambda x : x[6], basic_data)),
            "SHO" : list(map(lambda x : x[7], basic_data)),
            "W" : list(map(lambda x : x[8], basic_data)),
            "L" : list(map(lambda x : x[9], basic_data)),
            "SV" : list(map(lambda x : x[10], basic_data)),
            "HLD" : list(map(lambda x : x[11], basic_data)),
            "WPCT" : list(map(lambda x : x[12], basic_data)),
            "TBF" : list(map(lambda x : x[13], basic_data)),
            "IP" : list(map(lambda x : x[14], basic_data)),
            "H" : list(map(lambda x : x[15], basic_data)),
            "HR" : list(map(lambda x : x[16], basic_data)),
            "BB" : list(map(lambda x : x[17], basic_data)),
            "HBP" : list(map(lambda x : x[18], basic_data)),
            "SO" : list(map(lambda x : x[19], basic_data)),
            "R" : list(map(lambda x : x[20], basic_data)),
            "ER" : list(map(lambda x : x[21], basic_data)),
        })

    

        df = df.replace("-",np.nan)
        df['year'] = df['year'].astype('int32')
        df['id'] = df['id'].astype('int32')
        df['ERA'] = df['ERA'].astype('float64')
        df['G'] = df['G'].astype('int32')
        df['CG'] = df['CG'].astype('int32')
        df['SHO'] = df['SHO'].astype('int32')
        df['W'] = df['W'].astype('int32')
        df['L'] = df['L'].astype('int32')
        df['SV'] = df['SV'].astype('int32')
        df['HLD'] = df['HLD'].astype('int32')
        df['WPCT'] = df['WPCT'].astype('float64')
        df['TBF'] = df['TBF'].astype('int32')
        df['IP'] = df['IP'].astype('float64')
        df['H'] = df['H'].astype('int32')
        df['HR'] = df['HR'].astype('int32')
        df['BB'] = df['BB'].astype('int32')
        df['HBP'] = df['HBP'].astype('int32')
        df['SO'] = df['SO'].astype('int32')
        df['R'] = df['R'].astype('int32')
        df['ER'] = df['ER'].astype('int32')

        save_df(
            df,
            os.path.join(DATASET_NAME,PITCHER_DATASET_NAME,LEGACY_DATASET_NAME,f"Pitcher_{year}.parquet"),
            os.path.join(PITCHER_LEGACY_DATASET_DIR,f"Pitcher_{year}.parquet")
        )

        
        if max_page != 1 : move_to_page(-1)


    return pitcher_number_list


if __name__ == "__main__":
    try :
        st_time = time.time()

        pitcher_number_list = list(get_n_save_legacy_pitcher_data())

        df = pd.DataFrame({
            "Numbers" : pitcher_number_list
        })

        end_time = time.time()
        print(f"{end_time-st_time} s")


    finally:
        driver.quit()
