import sys
import os
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)

from __init__ import *
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
import datetime
import time
from multiprocessing import Process
WAIT = WebDriverWait(driver, 10)
def wait_element_for_click(BY, VALUE):
    return WAIT.until(EC.element_to_be_clickable((BY, VALUE)))

def select_dropdown_wait(BY, VALUE, index):
    dropdown = Select(wait_element_for_click(BY, VALUE))
    old_element = dropdown.first_selected_option  # Store reference to the old selection
    dropdown.select_by_index(index)
    wait_for_page_reload(old_element)  # Wait for page reload

    return Select(wait_element_for_click(BY, VALUE))  # Re-locate dropdown

def wait_for_page_reload(old_element):
    try :
        WAIT.until(EC.staleness_of(old_element))
    except TimeoutException:
        return
 

def wait_element_for_selected_attribute(BY, VALUE):
    return WAIT.until(EC.element_attribute_to_include((BY, VALUE),"selected"))

def wait_for_all_select_to_be_populated():
    objct = WAIT.until(EC.presence_of_all_elements_located((By.TAG_NAME, "select")))
    print(objct)
    exit(0)
def wait_for_all_option_to_be_populated():
     WAIT.until(EC.presence_of_all_elements_located((By.TAG_NAME, "option")))



def pitcher_versus_batter_work(team_start_idx : int, team_end_idx : int, attempt : int):
    try:
        save_whole_pitcher_versus_batter_data(team_start_idx, team_end_idx)
        exit(0)
    except Exception as e:
        if attempt < MAX_RETRIES:
            for item in traceback.format_exception(e):
                print(item)
            print("let's retry")
            time.sleep(SLEEP_TIME_BEFORE_RETRY)
            pitcher_versus_batter_work(team_start_idx, team_end_idx, attempt=attempt+1)
        else:
            print("exceed retry limit")
            exit(1)



def save_whole_pitcher_versus_batter_data(team_start_idx : int, team_end_idx : int):
    def set_initial_page_setting() :
        driver.get('https://www.koreabaseball.com/Record/Etc/HitVsPit.aspx')
        driver.implicitly_wait(3)


    #############################
    # driver.get('https://www.koreabaseball.com/Record/Player/HitterBasic/Detail1.aspx')
    # driver.implicitly_wait(3)
    set_initial_page_setting()

    # pitcher_team_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlPitcherTeam'))
    pitcher_team_selector = Select(wait_element_for_click(By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlPitcherTeam'))
    for pitcher_team_idx in range(team_start_idx, team_end_idx):
        # pitcher_team_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlPitcherTeam'))
        pitcher_team_selector = select_dropdown_wait(By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlPitcherTeam', pitcher_team_idx)
        # pitcher_team_selector = Select(wait_element_for_click(By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlPitcherTeam'))
        # pitcher_team_selector.select_by_index(pitcher_team_idx)

        # pitcher_team_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlPitcherTeam'))
        # driver.implicitly_wait(10)
        # time.sleep(SLEEP_TIME)
        # pitcher_team_selector = Select(wait_element_for_click(By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlPitcherTeam'))
        # selected_pitcher_team_element = [ option for option in pitcher_team_selector.options if option.get_attribute("selected")]
        selected_pitcher_team = pitcher_team_selector.first_selected_option.text

        print(f"process with {team_start_idx} selected_pitcher_team :: {selected_pitcher_team}")

        # pitcher_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlPitcherPlayer'))
        pitcher_selector = Select(wait_element_for_click(By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlPitcherPlayer'))
        for pitcher_idx in range(1, len(pitcher_selector.options)):
            # time.sleep(SLEEP_TIME)
            # pitcher_selector = select_dropdown_wait(By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlPitcherPlayer', pitcher_idx)
            pitcher_selector = Select(wait_element_for_click(By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlPitcherPlayer'))
            pitcher_selector.select_by_index(pitcher_idx)
            # time.sleep(SLEEP_TIME)
            # pitcher_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlPitcherPlayer'))
            # driver.implicitly_wait(10)
            # time.sleep(SLEEP_TIME)
            # pitcher_selector = Select(wait_element_for_click(By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlPitcherPlayer'))
            # selected_pitcher_element = [ option for option in pitcher_selector.options if option.get_attribute("selected")]
            # selected_pitcher = selected_pitcher_element[0].text
            selected_pitcher = pitcher_selector.first_selected_option.text
            selected_pitcher_id = pitcher_selector.first_selected_option.get_attribute("value")

            print(f"process with {team_start_idx} selected_pitcher :: {selected_pitcher}")


            pitcher_versus_data = []

            # batter_team_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlHitterTeam'))
            batter_team_selector = Select(wait_element_for_click(By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlHitterTeam'))
            for batter_team_idx in range(1, len(batter_team_selector.options)):
                # time.sleep(SLEEP_TIME)
                # batter_team_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlHitterTeam'))
                batter_team_selector = select_dropdown_wait(By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlHitterTeam',batter_team_idx)
                # batter_team_selector = Select(wait_element_for_click(By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlHitterTeam'))
                # batter_team_selector.select_by_index(batter_team_idx)
                # time.sleep(SLEEP_TIME)

                # batter_team_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlHitterTeam'))
                # driver.implicitly_wait(10)

                # time.sleep(SLEEP_TIME)
                # batter_team_selector = Select(wait_element_for_click(By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlHitterTeam'))
                # selected_batter_team_element = [ option for option in batter_team_selector.options if option.get_attribute("selected")]


                selected_batter_team = batter_team_selector.first_selected_option.text
                print(f"process with {team_start_idx} selected_batter_team :: {selected_batter_team}")

                if(selected_batter_team == selected_pitcher_team) : continue
                batter_selector = Select(wait_element_for_click(By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlHitterPlayer'))
                old_element_batter = batter_selector.first_selected_option

                # batter_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlHitterPlayer'))
                batter_len = len(batter_selector.options)
                for batter_idx in range(1, batter_len):
                    # time.sleep(SLEEP_TIME)

                    # batter_selector = select_dropdown_wait(By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlHitterPlayer', batter_idx)
                    # WAIT.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, r"select[name='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlHitterPlayer'] option")))
                    # batter_selector = Select(wait_element_for_click(By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlHitterPlayer'))
                    batter_selector = Select(wait_element_for_click(By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlHitterPlayer'))
                    batter_selector.select_by_index(batter_idx)
                    # time.sleep(SLEEP_TIME)
                    # batter_selector = Select(driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlHitterPlayer'))

                    # time.sleep(SLEEP_TIME)
                    # driver.implicitly_wait(10)

                    # batter_selector = Select(wait_element_for_click(By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlHitterPlayer'))
                    # selected_batter_element = [ option for option in batter_selector.options if option.get_attribute("selected")]
                    old_element_batter = batter_selector.first_selected_option
                    selected_batter = old_element_batter.text
                    print(f"process with {team_start_idx} selected_batter :: {selected_batter}")

                    selected_batter_id = batter_selector.first_selected_option.get_attribute("value")
                    
                    search_button = WAIT.until(EC.presence_of_element_located((By.NAME,'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$btnSearch')))
                    # search_button = driver.find_element(by=By.NAME, value='ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$btnSearch')
                    search_button.send_keys(Keys.RETURN)
                    # driver.implicitly_wait(10)
                    
                    try :
                        WAIT.until(EC.staleness_of(old_element_batter))
                    except TimeoutException:
                        pass

                    # year_element = driver.find_element(by=By.ID, value='cphContents_cphContents_cphContents_lblLastDate')
                    year_element = WAIT.until(EC.presence_of_element_located((By.ID, 'cphContents_cphContents_cphContents_lblLastDate')))
                    year = year_element.text[:-1]
                    # data gathering

                    # td_data = driver.find_elements(by=By.TAG_NAME, value="td")
                    td_data = WAIT.until(EC.presence_of_all_elements_located((By.TAG_NAME,"td")))

                    # len(td_data_element) == 14
                    if(len(td_data) < 14): 
                        pass
                    else:
                        PA = td_data[1].text
                        AB = td_data[2].text
                        H = td_data[3].text
                        B_2 = td_data[4].text
                        B_3 = td_data[5].text
                        HR = td_data[6].text
                        RBI = td_data[7].text
                        BB = td_data[8].text
                        HBP = td_data[9].text
                        SO = td_data[10].text

                        pitcher_versus_data.append(
                            [year]+
                            [selected_pitcher]+
                            [selected_pitcher_id]+
                            [selected_batter]+
                            [selected_batter_id]+
                            [PA] +
                            [AB] +
                            [H]+
                            [B_2]+
                            [B_3]+
                            [HR] +
                            [RBI]+
                            [BB] +
                            [HBP]+
                            [SO] 
                        )


                    if (batter_idx >= (batter_len-1)):
                        try :
                            WAIT.until(EC.staleness_of(old_element_batter))
                        except TimeoutException:
                            None

            # saving df
            df = pd.DataFrame(pitcher_versus_data,columns=(
                'year','pitcher', 'pitcher_id', 'batter', 'batter_id','PA','AB','H','2B','3B','HR','RBI','BB','HBP','SO'
                ))

            df = df.replace("-",np.nan)
            df['year'] = df['year'].astype(int)
            df['pitcher_id'] = df['pitcher_id'].astype(int)
            df['batter_id'] = df['batter_id'].astype(int)
            df['PA'] = df['PA'].astype(int)
            df['AB'] = df['AB'].astype(int)
            df['H'] = df['H'].astype(int)
            df['2B'] = df['2B'].astype(int)
            df['3B'] = df['3B'].astype(int)
            df['HR'] = df['HR'].astype(int)
            df['RBI'] = df['RBI'].astype(int)
            df['BB'] = df['BB'].astype(int)
            df['HBP'] = df['HBP'].astype(int)
            df['SO'] = df['SO'].astype(int)

            save_df(
                df,
                os.path.join(DATASET_NAME,VERSUS_DATASET_NAME,f"Versus_pitcher_{year}_{selected_pitcher_id}.parquet"),
                os.path.join(VERSUS_DATASET_DIR,f"Versus_pitcher_{year}_{selected_pitcher_id}.parquet")
            )

    return 0

if __name__ == "__main__":
    try :
        st_time = time.time()
        NUMBER_OF_TEAM = 10
        coef = 10 // NUM_PROCESS
        process_list = []

        for i in range(0, NUM_PROCESS):
            if i == NUM_PROCESS-1 : 
                process_list.append(Process(target=pitcher_versus_batter_work, args=(1+i*coef,10,1)))
            else : 
                process_list.append(Process(target=pitcher_versus_batter_work, args=(1+i*coef,1+coef+i*coef,1)))
        for process in process_list:
            process.start()
        
        for process in process_list:
            process.join()

            
        end_time = time.time()
        print(f"{end_time-st_time} s")


    finally:
        driver.quit()
