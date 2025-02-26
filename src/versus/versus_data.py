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
from multiprocessing import Process, Queue

def wait_element_for_click(WAIT, BY, VALUE):
    return WAIT.until(EC.element_to_be_clickable((BY, VALUE)))

def select_dropdown_wait(WAIT, BY, VALUE, index):
    dropdown = Select(wait_element_for_click(WAIT, BY, VALUE))
    old_element = dropdown.first_selected_option  # Store reference to the old selection
    dropdown.select_by_index(index)
    wait_for_page_reload(WAIT, old_element)  # Wait for page reload

    return Select(wait_element_for_click(WAIT, BY, VALUE))  # Re-locate dropdown

def wait_for_page_reload(WAIT, old_element):
    try :
        WAIT.until(EC.staleness_of(old_element))
    except TimeoutException:
        return
 




def pitcher_versus_batter_work(team_start_idx : int, team_end_idx : int, attempt : int, driver, queue):
    try:
        save_whole_pitcher_versus_batter_data(team_start_idx, team_end_idx, driver)
        queue.put(0)
        exit(0)
    except Exception as e:
        if attempt < MAX_RETRIES:
            for item in traceback.format_exception(e):
                print(item)
            print("let's retry")
            time.sleep(SLEEP_TIME_BEFORE_RETRY)
            pitcher_versus_batter_work(team_start_idx, team_end_idx, attempt=attempt+1, driver=driver,queue=queue)
        else:
            print("exceed retry limit")
            queue.put(1)
            exit(1)



def save_whole_pitcher_versus_batter_data(team_start_idx : int, team_end_idx : int, driver):
    def set_initial_page_setting() :
        driver.get('https://www.koreabaseball.com/Record/Etc/HitVsPit.aspx')
        driver.implicitly_wait(3)

    #############################
    WAIT = WebDriverWait(driver, 10)
    #############################
    set_initial_page_setting()

    pitcher_team_selector = Select(wait_element_for_click(WAIT, By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlPitcherTeam'))
    for pitcher_team_idx in range(team_start_idx, team_end_idx):
        pitcher_team_selector = select_dropdown_wait(WAIT, By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlPitcherTeam', pitcher_team_idx)
        selected_pitcher_team = pitcher_team_selector.first_selected_option.text

        print(f"process with {team_start_idx} selected_pitcher_team :: {selected_pitcher_team}")

        pitcher_selector = Select(wait_element_for_click(WAIT, By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlPitcherPlayer'))
        for pitcher_idx in range(1, len(pitcher_selector.options)):
            pitcher_selector = Select(wait_element_for_click(WAIT, By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlPitcherPlayer'))
            pitcher_selector.select_by_index(pitcher_idx)
            selected_pitcher = pitcher_selector.first_selected_option.text
            selected_pitcher_id = pitcher_selector.first_selected_option.get_attribute("value")

            print(f"process with {team_start_idx} selected_pitcher :: {selected_pitcher}")


            pitcher_versus_data = []

            batter_team_selector = Select(wait_element_for_click(WAIT, By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlHitterTeam'))
            for batter_team_idx in range(1, len(batter_team_selector.options)):
                batter_team_selector = select_dropdown_wait(WAIT, By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlHitterTeam',batter_team_idx)

                selected_batter_team = batter_team_selector.first_selected_option.text

                if(selected_batter_team == selected_pitcher_team) : continue
                batter_selector = Select(wait_element_for_click(WAIT, By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlHitterPlayer'))
                old_element_batter = batter_selector.first_selected_option

                batter_len = len(batter_selector.options)
                for batter_idx in range(1, batter_len):
                    batter_selector = Select(wait_element_for_click(WAIT, By.NAME, 'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlHitterPlayer'))
                    batter_selector.select_by_index(batter_idx)
                    old_element_batter = batter_selector.first_selected_option
                    selected_batter = old_element_batter.text

                    selected_batter_id = batter_selector.first_selected_option.get_attribute("value")
                    
                    search_button = WAIT.until(EC.presence_of_element_located((By.NAME,'ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$btnSearch')))
                    search_button.send_keys(Keys.RETURN)
                    
                    try :
                        WAIT.until(EC.staleness_of(old_element_batter))
                    except TimeoutException:
                        pass

                    year_element = WAIT.until(EC.presence_of_element_located((By.ID, 'cphContents_cphContents_cphContents_lblLastDate')))
                    year = year_element.text[:-1]

                    td_data = WAIT.until(EC.presence_of_all_elements_located((By.TAG_NAME,"td")))

                    if(len(td_data) < 14): 
                        pass
                    else:
                        AVG = td_data[0].text
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
                        SLG = td_data[11].text
                        OBP = td_data[12].text
                        OPS = td_data[13].text

                        pitcher_versus_data.append(
                            [year]+
                            [selected_pitcher]+
                            [selected_pitcher_id]+
                            [selected_pitcher_team]+
                            [selected_batter]+
                            [selected_batter_id]+
                            [selected_batter_team]+
                            [AVG] +
                            [PA] +
                            [AB] +
                            [H]+
                            [B_2]+
                            [B_3]+
                            [HR] +
                            [RBI]+
                            [BB] +
                            [HBP]+
                            [SO] +
                            [SLG] +
                            [OBP] +
                            [OPS]
                        )


                    if (batter_idx >= (batter_len-1)):
                        try :
                            WAIT.until(EC.staleness_of(old_element_batter))
                        except TimeoutException:
                            None

            # saving df
            df = pd.DataFrame(pitcher_versus_data,columns=(
                'year','pitcher', 'pitcher_id', 'pitcher_team', 'batter', 'batter_id', 'batter_team', 'AVG', 
                'PA','AB','H','2B','3B','HR','RBI','BB','HBP','SO',
                'SLG','OBP','OPS'
                ))

            df = df.replace("-",np.nan)
            df['year'] = df['year'].astype('int32')
            df['pitcher_id'] = df['pitcher_id'].astype('int32')
            df['batter_id'] = df['batter_id'].astype('int32')
            df['PA'] = df['PA'].astype('int32')
            df['AB'] = df['AB'].astype('int32')
            df['H'] = df['H'].astype('int32')
            df['2B'] = df['2B'].astype('int32')
            df['3B'] = df['3B'].astype('int32')
            df['HR'] = df['HR'].astype('int32')
            df['RBI'] = df['RBI'].astype('int32')
            df['BB'] = df['BB'].astype('int32')
            df['HBP'] = df['HBP'].astype('int32')
            df['SO'] = df['SO'].astype('int32')
            df['AVG'] = df['AVG'].astype('float64')
            df['SLG'] = df['SLG'].astype('float64')
            df['OBP'] = df['OBP'].astype('float64')
            df['OPS'] = df['OPS'].astype('float64')


            save_df(
                df,
                os.path.join(DATASET_NAME,CURRENT_DATASET_NAME,CURRENT_YEAR,VERSUS_DATASET_NAME,f"Versus_pitcher_{year}_{selected_pitcher_id}.parquet"),
                os.path.join(VERSUS_DATASET_DIR,f"Versus_pitcher_{year}_{selected_pitcher_id}.parquet")
            )

    return 0

if __name__ == "__main__":
    drivers = [driver]
    NUM_PROCESS = 5
    try :
        if NUM_PROCESS > 1:
            if IS_BLOB:
                for i in range(NUM_PROCESS-1):
                    d = webdriver.Remote(
                        command_executor=command_executor_url,
                        options=options
                    )
                    drivers.append(d)
            else:
                for i in range(NUM_PROCESS-1):
                    d = webdriver.Chrome(options=options)
                    drivers.append(d)


        st_time = time.time()

        NUMBER_OF_TEAM = 10

        coef = 10 // NUM_PROCESS
        process_list = []
        queue = Queue()


        for i in range(0, NUM_PROCESS):
            if i == NUM_PROCESS-1 : 
                process_list.append(Process(target=pitcher_versus_batter_work, args=(1+i*coef,11,1,drivers[i],queue)))
            else : 
                process_list.append(Process(target=pitcher_versus_batter_work, args=(1+i*coef,1+coef+i*coef,1,drivers[i],queue)))
                
        for process in process_list:
            process.start()

        for _ in range(NUM_PROCESS):
            if queue.get() == 1:
                print("Error !! Terminating all process.")
                for process in process_list:
                    process.terminate()
                break

        
        for process in process_list:
            process.join()

            
        end_time = time.time()
        print(f"{end_time-st_time} s")


    finally:
        for d in drivers:
            d.quit()