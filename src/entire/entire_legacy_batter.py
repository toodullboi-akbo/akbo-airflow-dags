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


def get_n_save_legacy_batter_data() -> set:
    '''
    과거 스탯 타자 기록 가져오기

    return set ::: 선수들의 고유번호가 담긴 set
    '''
    def set_initial_page_setting() :
        driver.get('https://www.koreabaseball.com/Record/Player/HitterBasic/BasicOld.aspx')
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


    batter_number_list = set([])
    max_page = -1

    #############################
    # driver.get('https://www.koreabaseball.com/Record/Player/HitterBasic/Detail1.aspx')
    # driver.implicitly_wait(3)
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
            # len(td_data_element) == 19

            for i in range(0,len(td_data),19):
                # 여기서 이름과 번호도 기록
                name_a = td_data[i+1].find_element(by=By.XPATH, value="./child::a")
                name = name_a.text
                number = name_a.get_attribute('href').split('=')[-1]
                team_name = td_data[i+2].text # 팀명
                stat_AVG = td_data[i+3].text # 타율
                stat_G = td_data[i+4].text # 경기수
                stat_PA = td_data[i+5].text # 타석
                stat_AB = td_data[i+6].text # 타수
                stat_H = td_data[i+7].text # 안타
                stat_2B = td_data[i+8].text # 2루타
                stat_3B = td_data[i+9].text # 3루타
                stat_HR = td_data[i+10].text # 홈런
                stat_RBI = td_data[i+11].text # 타점
                stat_SB = td_data[i+12].text # 도루
                stat_CS = td_data[i+13].text # 도루실패
                stat_BB = td_data[i+14].text # 볼넷
                stat_HBP = td_data[i+15].text # 사구
                stat_SO = td_data[i+16].text # 삼진
                stat_GDP = td_data[i+17].text # 병살타
                stat_E = td_data[i+18].text # 실책

                
                basic_data.append(
                    [year]+
                    [name]+
                    [number]+
                    [team_name]+
                    [stat_AVG]+
                    [stat_G]+
                    [stat_PA] +
                    [stat_AB] +
                    [stat_H]+
                    [stat_2B] +
                    [stat_3B] +
                    [stat_HR] +
                    [stat_RBI]+
                    [stat_SB] +
                    [stat_CS] +
                    [stat_BB] +
                    [stat_HBP]+
                    [stat_SO]+ 
                    [stat_GDP]+
                    [stat_E]
                )

                batter_number_list.add(number)



        df = pd.DataFrame({
            "is_legacy" : ["Y"] * len(basic_data),
            "year" : list(map(lambda x : x[0], basic_data)),
            "name" : list(map(lambda x : x[1], basic_data)),
            "id" : list(map(lambda x : x[2], basic_data)),
            "team" : list(map(lambda x : x[3], basic_data)),
            "AVG" : list(map(lambda x : x[4], basic_data)), 
            "G" : list(map(lambda x : x[5], basic_data)), 
            "PA" : list(map(lambda x : x[6], basic_data)), 
            "AB" : list(map(lambda x : x[7], basic_data)), 
            "H" : list(map(lambda x : x[8], basic_data)), 
            "2B" : list(map(lambda x : x[9], basic_data)), 
            "3B" : list(map(lambda x : x[10], basic_data)), 
            "HR" : list(map(lambda x : x[11], basic_data)), 
            "RBI" : list(map(lambda x : x[12], basic_data)), 
            "SB" : list(map(lambda x : x[13], basic_data)), 
            "CS" : list(map(lambda x : x[14], basic_data)), 
            "BB" : list(map(lambda x : x[15], basic_data)), 
            "HBP" : list(map(lambda x : x[16], basic_data)), 
            "SO" : list(map(lambda x : x[17], basic_data)), 
            "GDP" : list(map(lambda x : x[18], basic_data)), 
            "E" : list(map(lambda x : x[19], basic_data)), 
        })

        df = df.replace("-",np.nan)
        df['year'] = df['year'].astype('int32')
        df['id'] = df['id'].astype('int32')
        df['AVG'] = df['AVG'].astype('float64')
        df['G'] = df['G'].astype('int32')
        df['PA'] = df['PA'].astype('int32')
        df['AB'] = df['AB'].astype('int32')
        df['H'] = df['H'].astype('int32')
        df['2B'] = df['2B'].astype('int32')
        df['3B'] = df['3B'].astype('int32')
        df['HR'] = df['HR'].astype('int32')
        df['RBI'] = df['RBI'].astype('int32')
        df['SB'] = df['SB'].astype('int32')
        df['CS'] = df['CS'].astype('int32')
        df['BB'] = df['BB'].astype('int32')
        df['HBP'] = df['HBP'].astype('int32')
        df['SO'] = df['SO'].astype('int32')
        df['GDP'] = df['GDP'].astype('int32')
        df['E'] = df['E'].astype('int32')

        save_df(
            df,
            os.path.join(DATASET_NAME,BATTER_DATASET_NAME,LEGACY_DATASET_NAME,f"Batter_{year}.parquet"),
            os.path.join(BATTER_LEGACY_DATASET_DIR,f"Batter_{year}.parquet")
        )
        
        if max_page != 1: move_to_page(-1)


    return batter_number_list

if __name__ == "__main__":
    try :
        st_time = time.time()
        batter_number_list = list(get_n_save_legacy_batter_data())

        df = pd.DataFrame({
            "Numbers" : batter_number_list
        })

        end_time = time.time()
        print(f"{end_time-st_time} s")


    finally:
        driver.quit()
 