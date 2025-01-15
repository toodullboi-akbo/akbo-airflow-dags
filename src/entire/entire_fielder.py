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
from fractions import Fraction

#################

def get_n_save_whole_year_fielding_data() -> set:
    '''
    현재부터 MIN_YEAR +1 년까지 전체 수비 기록 가져오기

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


    fielder_number_list = set([])
    max_page = -1

    #############################
    driver.get('https://www.koreabaseball.com/Record/Player/Defense/Basic.aspx')
    driver.implicitly_wait(3)

    year_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason'))
    for year_idx in range(len(year_selector.options)-1,-1,-1):
        year_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason'))
        year_selector.select_by_index(year_idx) # 최신부터 내려오기
        time.sleep(CONST_SLEEP_TIME)
        year_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason'))
        selected_year = [ option for option in year_selector.options if option.get_attribute("selected")]
        year = selected_year[0].text
        if(year == MIN_YEAR) : break
 
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
            # len(td_data_element) == 17

            for i in range(0,len(td_data),17):
                # 여기서 이름과 번호도 기록
                name_a = td_data[i+1].find_element(by=By.XPATH, value="./child::a")
                name = name_a.text
                number = name_a.get_attribute('href').split('=')[-1]
                team_name = td_data[i+2].text

                stat_POS = td_data[i+3].text # 포지션
                stat_G  = td_data[i+4].text # 경기
                stat_GS  = td_data[i+5].text # 선발경기
                stat_IP = td_data[i+6].text # 수비이닝
                stat_IP_float = 0
                for item in stat_IP.split(" "):
                    fraction = Fraction(item)
                    stat_IP_float += float(fraction)
                stat_E = td_data[i+7].text # 실책
                stat_PKO = td_data[i+8].text # 견제사
                stat_PO = td_data[i+9].text # 풋아웃
                stat_A = td_data[i+10].text # 어시스트
                stat_DP = td_data[i+11].text # 병살타
                stat_PB = td_data[i+13].text # 포일
                stat_SB = td_data[i+14].text # 도루 허용
                stat_CS = td_data[i+15].text # 도루 저지

                basic_data.append(
                    [year]+
                    [name]+
                    [number]+
                    [team_name]+
                    [stat_POS]+
                    [stat_G]+
                    [stat_GS]+
                    [stat_IP_float]+
                    [stat_E]+
                    [stat_PKO]+
                    [stat_PO]+
                    [stat_A]+
                    [stat_DP]+
                    [stat_PB]+
                    [stat_SB]+
                    [stat_CS]
                )

                fielder_number_list.add(number)

        df = pd.DataFrame(basic_data,columns=('year','name', 'id', 'team', 'POS','G','GS','IP','E',
                                        'PKO','PO','A','DP','PB','SB','CS'))



        df = df.replace("-",value=np.nan)
        df['year'] = df['year'].astype(int)
        df['id'] = df['id'].astype(int)
        df['G'] = df['G'].astype(int)
        df['GS'] = df['GS'].astype(int)
        df['IP'] = df['IP'].astype(float)
        df['E'] = df['E'].astype(int)
        df['PKO'] = df['PKO'].astype(int)
        df['PO'] = df['PO'].astype(int)
        df['A'] = df['A'].astype(int)
        df['DP'] = df['DP'].astype(int)
        df['PB'] = df['PB'].astype(int)
        df['SB'] = df['SB'].astype(int)
        df['CS'] = df['CS'].astype(int)

        if IS_BLOB:
            blob_name_path = os.path.join(DATASET_NAME,FIELDER_DATASET_NAME,f"Fielding_{year}.parquet")
            parquet_data = df.to_parquet(engine="pyarrow", index=False)
            wasb_hook.load_string(
                string_data=parquet_data,
                container_name=container_name,
                blob_name=blob_name_path,
                overwrite=True
            )
        else:
            fielding_file_path = os.path.join(FIELDING_DATASET_DIR,f"Fielding_{year}.parquet")
            df.to_parquet(fielding_file_path,engine='pyarrow',index=False)

        if max_page != 1 : move_to_page(-1)

    return fielder_number_list


if __name__ == "__main__":
    try:
        st_time = time.time()
        batter_number_list = list(get_n_save_whole_year_fielding_data())
        print(f"number of distinct fielder ::: {len(batter_number_list)}")
        
        end_time = time.time()

        print(f"{end_time-st_time} s")
    finally:
        driver.quit()
