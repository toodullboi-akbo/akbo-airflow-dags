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

def get_n_save_whole_year_pitcher_data() -> set:
    '''
    현재부터 MIN_YEAR+1 까지 전체 투수 기록 가져오기

    return set ::: 선수들의 고유번호가 담긴 set
    '''
    def set_initial_page_setting() :
        driver.get('https://www.koreabaseball.com/Record/Player/PitcherBasic/Basic1.aspx')
        driver.implicitly_wait(3)
        cat_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries$ddlSeries'))
        cat_selector.select_by_index(0) # 0 th idx -> 정규시즌
        time.sleep(CONST_SLEEP_TIME)

        detail_button = driver.find_element(by=By.XPATH, value='//*[@id="cphContents_cphContents_cphContents_udpContent"]/div[2]/div[1]/ul/li[2]/a')
        detail_button.send_keys(Keys.RETURN)
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

    def move_to_basic_detail(category : str):
        if category == "basic" :
            basic_button = driver.find_element(by=By.XPATH, value = '//*[@id="cphContents_cphContents_cphContents_udpContent"]/div[2]/div[1]/ul/li[1]/a')
            basic_button.send_keys(Keys.RETURN)
        elif category == "detail":
            detail_button = driver.find_element(by=By.XPATH, value='//*[@id="cphContents_cphContents_cphContents_udpContent"]/div[2]/div[1]/ul/li[2]/a')
            detail_button.send_keys(Keys.RETURN)
        
        driver.implicitly_wait(10)
        sort_button = driver.find_element(by=By.XPATH, value='//*[@id="cphContents_cphContents_cphContents_udpContent"]/div[3]/table/thead/tr/th[5]/a')
        sort_button.send_keys(Keys.RETURN)
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
    # driver.get('https://www.koreabaseball.com/Record/Player/PitcherBasic/Detail1.aspx')
    # driver.implicitly_wait(3)
    set_initial_page_setting()
    
    year_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason'))
    year_selector.select_by_value(CURRENT_YEAR) # 최신부터 내려오기
    time.sleep(CONST_SLEEP_TIME)
    year_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason'))
    selected_year = [ option for option in year_selector.options if option.get_attribute("selected")]
    year = selected_year[0].text

    assert(year == CURRENT_YEAR)

    move_to_basic_detail("basic")
    ########
    # BASIC
    ########

    # BASIC 1 but skip
    max_page = get_max_page()

    # BASIC 2
    move_to_page(-1)
    next_button = driver.find_element(by=By.CLASS_NAME, value="next")
    next_button.send_keys(Keys.RETURN)
    driver.implicitly_wait(10)
    sort_button = driver.find_element(by=By.XPATH, value='//*[@id="cphContents_cphContents_cphContents_udpContent"]/div[3]/table/thead/tr/th[5]/a')
    sort_button.send_keys(Keys.RETURN)
    time.sleep(CONST_SLEEP_TIME)

    basic_2_data = []
    for current_page in range(1,max_page+1):
        modulared_current_page = current_page % 5
        if modulared_current_page == 1 and current_page != 1:
            next_page_button = driver.find_element(by=By.ID, value='cphContents_cphContents_cphContents_ucPager_btnNext')
            next_page_button.send_keys(Keys.RETURN)
            time.sleep(CONST_SLEEP_TIME)

        if modulared_current_page != 1 :
            move_to_page(modulared_current_page)
        
        td_data = driver.find_elements(by=By.TAG_NAME, value="td")
        # len(td_data_element) == 18

        for i in range(0,len(td_data),18):
            name_a = td_data[i+1].find_element(by=By.XPATH, value="./child::a")
            name = name_a.text
            number = name_a.get_attribute('href').split('=')[-1]
            team_name = td_data[i+2].text 
            stat_CG = td_data[i+4].text # 완투
            stat_SHO = td_data[i+5].text # 완봉
            stat_QS = td_data[i+6].text # 퀄스
            stat_BSV = td_data[i+7].text # 블론세이브
            stat_TBF = td_data[i+8].text # 타자수
            stat_NP = td_data[i+9].text # 투구수
            stat_2B = td_data[i+11].text # 2루타
            stat_3B = td_data[i+12].text # 3루타
            stat_SAC = td_data[i+13].text # 희생번트
            stat_SF = td_data[i+14].text # 희생플라이
            stat_IBB = td_data[i+15].text # 고의사구
            stat_WP = td_data[i+16].text # 폭투
            stat_BK = td_data[i+17].text # 보크

            basic_2_data.append(
                [year]+
                [name]+
                [number]+
                [team_name]+
                [stat_CG]+
                [stat_SHO]+
                [stat_QS]+
                [stat_BSV]+
                [stat_TBF]+
                [stat_NP]+
                [stat_2B]+
                [stat_3B]+
                [stat_SAC]+
                [stat_SF]+
                [stat_IBB]+
                [stat_WP]+
                [stat_BK]
            )

            pitcher_number_list.add(number)

    df = pd.DataFrame({
        "is_legacy" : ["N"] * len(basic_2_data),
        "year" : list(map(lambda x : x[0], basic_2_data)),
        "name" : list(map(lambda x : x[1], basic_2_data)),
        "id" : list(map(lambda x : x[2], basic_2_data)),
        "team" : list(map(lambda x : x[3], basic_2_data)),
        "CG" : list(map(lambda x : x[4], basic_2_data)),
        "SHO" : list(map(lambda x : x[5], basic_2_data)),
        "QS" : list(map(lambda x : x[6], basic_2_data)),
        "BSV" : list(map(lambda x : x[7], basic_2_data)),
        "TBF" : list(map(lambda x : x[8], basic_2_data)),
        "NP" : list(map(lambda x : x[9], basic_2_data)),
        "2B" : list(map(lambda x : x[10], basic_2_data)),
        "3B" : list(map(lambda x : x[11], basic_2_data)),
        "SAC" : list(map(lambda x : x[12], basic_2_data)),
        "SF" : list(map(lambda x : x[13], basic_2_data)),
        "IBB" : list(map(lambda x : x[14], basic_2_data)),
        "WP" : list(map(lambda x : x[15], basic_2_data)),
        "BK" : list(map(lambda x : x[16], basic_2_data))
    })

    df = df.replace("-",np.nan)
    df['year'] = df['year'].astype('int32')
    df['id'] = df['id'].astype('int32')
    df['CG'] = df['CG'].astype('int32')
    df['SHO'] = df['SHO'].astype('int32')
    df['QS'] = df['QS'].astype('int32')
    df['BSV'] = df['BSV'].astype('int32')
    df['TBF'] = df['TBF'].astype('int32')
    df['NP'] = df['NP'].astype('int32')
    df['2B'] = df['2B'].astype('int32')
    df['3B'] = df['3B'].astype('int32')
    df['SAC'] = df['SAC'].astype('int32')
    df['SF'] = df['SF'].astype('int32')
    df['IBB'] = df['IBB'].astype('int32')
    df['WP'] = df['WP'].astype('int32')
    df['BK'] = df['BK'].astype('int32')

    save_df(
        df,
        os.path.join(DATASET_NAME,PITCHER_DATASET_NAME,YEARLY_DATASET_NAME,"basic_2",f"Pitcher_basic_2_{year}.parquet"),
        os.path.join(PITCHER_YEARLY_DATASET_DIR,"basic_2",f"Pitcher_basic_2_{year}.parquet")
    )

    ########
    # DETAIL
    ########
    move_to_page(-1)
    move_to_basic_detail("detail")
    
    # DETAIL 1
    detail_1_data = []

    for current_page in range(1,max_page+1):
        modulared_current_page = current_page % 5
        if modulared_current_page == 1 and current_page != 1:
            next_page_button = driver.find_element(by=By.ID, value='cphContents_cphContents_cphContents_ucPager_btnNext')
            next_page_button.send_keys(Keys.RETURN)
            time.sleep(CONST_SLEEP_TIME)

        if modulared_current_page != 1 :
            move_to_page(modulared_current_page)
        
        td_data = driver.find_elements(by=By.TAG_NAME, value="td")
        # len(td_data_element) == 14

        for i in range(0,len(td_data),14):
            name_a = td_data[i+1].find_element(by=By.XPATH, value="./child::a")
            name = name_a.text
            number = name_a.get_attribute('href').split('=')[-1]
            # 세부기록
            stat_GS = td_data[i+4].text # 선발수
            stat_Wgs = td_data[i+5].text # 선발승
            stat_Wgr  = td_data[i+6].text # 구원승
            stat_GF = td_data[i+7].text # 경기종료
            stat_SVO = td_data[i+8].text # 세이브기회
            stat_TS = td_data[i+9].text # 터프세이브
            stat_GDP = td_data[i+10].text # 병살타
            stat_GO = td_data[i+11].text # 땅볼
            stat_AO = td_data[i+12].text # 뜬공

            detail_1_data.append(
                [year]+
                [number]+
                [stat_GS]+
                [stat_Wgs]+
                [stat_Wgr]+
                [stat_GF]+
                [stat_SVO]+
                [stat_TS]+
                [stat_GDP]+
                [stat_GO]+
                [stat_AO]
            )

    df = pd.DataFrame({
        "year" : list(map(lambda x : x[0], detail_1_data)),
        "id" : list(map(lambda x : x[1], detail_1_data)),
        "GS" : list(map(lambda x : x[2], detail_1_data)),
        "Wgs" : list(map(lambda x : x[3], detail_1_data)),
        "Wgr" : list(map(lambda x : x[4], detail_1_data)),
        "GF" : list(map(lambda x : x[5], detail_1_data)),
        "SVO" : list(map(lambda x : x[6], detail_1_data)),
        "TS" : list(map(lambda x : x[7], detail_1_data)),
        "GDP" : list(map(lambda x : x[8], detail_1_data)),
        "GO" : list(map(lambda x : x[9], detail_1_data)),
        "AO" : list(map(lambda x : x[10], detail_1_data)),
    })



    df = df.replace("-",np.nan)
    df['year'] = df['year'].astype('int32')
    df['id'] = df['id'].astype('int32')
    df['GS'] = df['GS'].astype('int32')
    df['Wgs'] = df['Wgs'].astype('int32')
    df['Wgr'] = df['Wgr'].astype('int32')
    df['GF'] = df['GF'].astype('int32')
    df['SVO'] = df['SVO'].astype('int32')
    df['TS'] = df['TS'].astype('int32')
    df['GDP'] = df['GDP'].astype('int32')
    df['GO'] = df['GO'].astype('int32')
    df['AO'] = df['AO'].astype('int32')

    save_df(
        df,
        os.path.join(DATASET_NAME,PITCHER_DATASET_NAME,YEARLY_DATASET_NAME,"detail_1",f"Pitcher_detail_1_{year}.parquet"),
        os.path.join(PITCHER_YEARLY_DATASET_DIR,"detail_1",f"Pitcher_detail_1_{year}.parquet")
    )

    
    if max_page != 1 : move_to_page(-1)


    return pitcher_number_list


if __name__ == "__main__":
    try :
        # making directory if not existed
        if not IS_BLOB:
            basic_2_dir_path = os.path.join(PITCHER_YEARLY_DATASET_DIR, "basic_2")
            detail_1_dir_path = os.path.join(PITCHER_YEARLY_DATASET_DIR, "detail_1")
            if not os.path.exists(basic_2_dir_path):
                os.mkdir(basic_2_dir_path)
            if not os.path.exists(detail_1_dir_path):
                os.mkdir(detail_1_dir_path)

        st_time = time.time()
        pitcher_number_list = list(get_n_save_whole_year_pitcher_data())


        df = pd.DataFrame({
            "Numbers" : pitcher_number_list
        })

        if IS_BLOB:
            blob_name_path = ENTIRE_PITCHER_NUMBER_NAME_PATH
            csv_data = df.to_csv(encoding='utf-8',mode='w',index=False)
            wasb_hook.load_string(
                string_data=csv_data,
                container_name=container_name,
                blob_name=blob_name_path,
                overwrite=True
            )
        else:
            df.to_csv(ENTIRE_PITCHER_NUMBER_PATH,encoding='utf-8',mode='w',index=False)



        end_time = time.time()
        print(f"{end_time-st_time} s")


    finally:
        driver.quit()
