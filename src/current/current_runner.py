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
from selenium.common.exceptions import NoSuchElementException, InvalidSessionIdException
import datetime
import time


#################

def get_n_save_whole_year_runner_data() -> set:
    '''
    현재부터 MIN_YEAR + 1 까지 전체 주루 기록 가져오기

    return set ::: 선수들의 고유번호가 담긴 set
    '''
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


    runner_number_list = set([])
    max_page = -1

    #############################
    driver.get('https://www.koreabaseball.com/Record/Player/Runner/Basic.aspx')
    driver.implicitly_wait(3)

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
        # len(td_data_element) == 10

        for i in range(0,len(td_data),10):
            # 여기서 이름과 번호도 기록
            name_a = td_data[i+1].find_element(by=By.XPATH, value="./child::a")
            name = name_a.text
            number = name_a.get_attribute('href').split('=')[-1]
            team_name = td_data[i+2].text
            stat_G = td_data[i+3].text # 경기수
            stat_SB = td_data[i+5].text # 도루 성공
            stat_CS = td_data[i+6].text # 도루 실패
            stat_OOB = td_data[i+8].text # 주루사
            stat_PKO = td_data[i+9].text # 견제사

            basic_data.append(
                [year]+
                [name]+
                [number]+
                [team_name]+
                [stat_G]+
                [stat_SB]+
                [stat_CS]+
                [stat_OOB]+
                [stat_PKO]
            )

            runner_number_list.add(number)

    df = pd.DataFrame(basic_data,columns=('year','name', 'id','team', 'G','SB','CS','OOB','PKO'))



    df = df.replace("-",value=np.nan)
    df['year'] = df['year'].astype('int32')
    df['id'] = df['id'].astype('int32')
    df['G'] = df['G'].astype('int32')
    df['SB'] = df['SB'].astype('int32')
    df['CS'] = df['CS'].astype('int32')
    df['OOB'] = df['OOB'].astype('int32')
    df['PKO'] = df['PKO'].astype('int32')

    save_df(
        df,
        os.path.join(DATASET_NAME,CURRENT_DATASET_NAME,CURRENT_YEAR,RUNNER_DATASET_NAME,f"Runner_{year}.parquet"),
        os.path.join(RUNNER_DATASET_DIR,f"Runner_{year}.parquet")
    )

    if max_page != 1 : move_to_page(-1)


    return runner_number_list


if __name__ == "__main__":
    try:
        st_time = time.time()
        runner_number_list = list(get_n_save_whole_year_runner_data())
        print(f"number of distinct runner ::: {len(runner_number_list)}")
        
        end_time = time.time()

        print(f"{end_time-st_time} s")
    finally:
        try:
            driver.quit()
        except InvalidSessionIdException:
            print("session already closed, skip. .. ")
