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


def get_n_save_whole_year_batter_data() -> set:
    '''
    현재부터 MIN_YEAR +1까지 전체 타자 기록 가져오기

    return set ::: 선수들의 고유번호가 담긴 set
    '''
    def set_initial_page_setting() :
        driver.get('https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx')
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
            basic_button = driver.find_element(by=By.XPATH, value = '//*[@id="cphContents_cphContents_cphContents_udpContent"]/div[2]/ul/li[1]/a')
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


    batter_number_list = set([])
    max_page = -1

    #############################
    # driver.get('https://www.koreabaseball.com/Record/Player/HitterBasic/Detail1.aspx')
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
    basic_1_data = []

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
        # len(td_data_element) == 16

        for i in range(0,len(td_data),16):
            name_a = td_data[i+1].find_element(by=By.XPATH, value="./child::a")
            name = name_a.text
            number = name_a.get_attribute('href').split('=')[-1]
            team_name = td_data[i+2].text
            stat_SAC = td_data[i+14].text # 희생번트
            stat_SF = td_data[i+15].text # 희생플라이
            
            basic_1_data.append(
                [year]+
                [name]+
                [number]+
                [team_name]+
                [stat_SAC]+
                [stat_SF]
            )

            batter_number_list.add(number)

    # saving df
    df = pd.DataFrame({
        "is_legacy" : ["N"] * len(basic_1_data),
        "year" : list(map(lambda x : x[0], basic_1_data)),
        "name" : list(map(lambda x : x[1], basic_1_data)),
        "id" : list(map(lambda x : x[2], basic_1_data)),
        "team" : list(map(lambda x : x[3], basic_1_data)),
        "SAC" : list(map(lambda x : x[4], basic_1_data)),
        "SF" : list(map(lambda x : x[5], basic_1_data))
    })

    df = df.replace("-",np.nan)
    df['year'] = df['year'].astype('int32')
    df['id'] = df['id'].astype('int32')
    df['SAC'] = df['SAC'].astype('int32')
    df['SF'] = df['SF'].astype('int32')

    save_df(
        df,
        os.path.join(DATASET_NAME,BATTER_DATASET_NAME,YEARLY_DATASET_NAME,"basic_1",f"Batter_basic_1_{year}.parquet"),
        os.path.join(BATTER_YEARLY_DATASET_DIR,"basic_1",f"Batter_basic_1_{year}.parquet")
    )

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
        # len(td_data_element) == 15

        for i in range(0,len(td_data),15):
            name_a = td_data[i+1].find_element(by=By.XPATH, value="./child::a")
            name = name_a.text
            number = name_a.get_attribute('href').split('=')[-1]
            stat_IBB = td_data[i+5].text # 고의사구
            stat_MH = td_data[i+12].text # 멀티히트
            stat_RISP = td_data[i+13].text # 득점권 타율
            stat_PH_BA = td_data[i+14].text # 대타 타율

            basic_2_data.append(
                [year]+
                [number]+
                [stat_IBB]+
                [stat_MH]+
                [stat_RISP]+
                [stat_PH_BA]
            )

    # saving df
    df = pd.DataFrame({
        "year" : list(map(lambda x : x[0], basic_2_data)),
        "id" : list(map(lambda x : x[1], basic_2_data)),
        "IBB" : list(map(lambda x : x[2], basic_2_data)),
        "MH" : list(map(lambda x : x[3], basic_2_data)),
        "RISP" : list(map(lambda x : x[4], basic_2_data)),
        "PHBA" : list(map(lambda x : x[5], basic_2_data)),
    })

    df = df.replace("-",np.nan)
    df['year'] = df['year'].astype('int32')
    df['id'] = df['id'].astype('int32')
    df['IBB'] = df['IBB'].astype('int32')
    df['MH'] = df['MH'].astype('int32')
    df['RISP'] = df['RISP'].astype('float64')
    df['PHBA'] = df['PHBA'].astype('float64')

    save_df(
        df,
        os.path.join(DATASET_NAME,BATTER_DATASET_NAME,YEARLY_DATASET_NAME,"basic_2",f"Batter_basic_2_{year}.parquet"),
        os.path.join(BATTER_YEARLY_DATASET_DIR,"basic_2",f"Batter_basic_2_{year}.parquet")
    )

    ########
    # DETAIL
    ########
    move_to_page(-1)
    move_to_basic_detail("detail")
    detail_data = []
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
            stat_GO = td_data[i+5].text # 땅볼
            stat_AO = td_data[i+6].text # 뜬공
            stat_GW_RBI = td_data[i+8].text # 결승타
            stat_P_PA = td_data[i+10].text # 투구수/타석
            stat_XR = td_data[i+12].text # 추정득점

            detail_data.append(
                [year]+
                [number]+
                [stat_GO]+
                [stat_AO]+
                [stat_GW_RBI]+
                [stat_P_PA]+
                [stat_XR]
            )


    df = pd.DataFrame({
        "year" : list(map(lambda x : x[0], detail_data)),
        "id" : list(map(lambda x : x[1], detail_data)),
        "GO" : list(map(lambda x : x[2], detail_data)),
        "AO" : list(map(lambda x : x[3], detail_data)),
        "GWRBI" : list(map(lambda x : x[4], detail_data)),
        "PPA" : list(map(lambda x : x[5], detail_data)),
        "XR" : list(map(lambda x : x[6], detail_data)),
    })

    df = df.replace("-",np.nan)
    df['year'] = df['year'].astype('int32')
    df['id'] = df['id'].astype('int32')
    df['GO'] = df['GO'].astype('int32')
    df['AO'] = df['AO'].astype('int32')
    df['GWRBI'] = df['GWRBI'].astype('int32')
    df['PPA'] = df['PPA'].astype('float64')
    df['XR'] = df['XR'].astype('float64')

    save_df(
        df,
        os.path.join(DATASET_NAME,BATTER_DATASET_NAME,YEARLY_DATASET_NAME,"detail",f"Batter_detail_{year}.parquet"),
        os.path.join(BATTER_YEARLY_DATASET_DIR,"detail",f"Batter_detail_{year}.parquet")
    )


    if max_page != 1 : move_to_page(-1)


    return batter_number_list

if __name__ == "__main__":
    try :
        # making directory if not existed
        if not IS_BLOB:
            basic_1_dir_path = os.path.join(BATTER_YEARLY_DATASET_DIR, "basic_1")
            basic_2_dir_path = os.path.join(BATTER_YEARLY_DATASET_DIR, "basic_2")
            detail_dir_path = os.path.join(BATTER_YEARLY_DATASET_DIR, "detail")
            if not os.path.exists(basic_1_dir_path):
                os.mkdir(basic_1_dir_path)
            if not os.path.exists(basic_2_dir_path):
                os.mkdir(basic_2_dir_path)
            if not os.path.exists(detail_dir_path):
                os.mkdir(detail_dir_path)

        st_time = time.time()
        batter_number_list = list(get_n_save_whole_year_batter_data())



        df = pd.DataFrame({
            "Numbers" : batter_number_list
        })

        if IS_BLOB:
            blob_name_path = ENTIRE_BATTER_NUMBER_NAME_PATH
            csv_data = df.to_csv(encoding='utf-8',mode='w',index=False)
            wasb_hook.load_string(
                string_data=csv_data,
                container_name=container_name,
                blob_name=blob_name_path,
                overwrite=True
            )
        else:
            df.to_csv(ENTIRE_BATTER_NUMBER_PATH,encoding='utf-8',mode='w',index=False)
        
        end_time = time.time()
        print(f"{end_time-st_time} s")


    finally:
        driver.quit()
