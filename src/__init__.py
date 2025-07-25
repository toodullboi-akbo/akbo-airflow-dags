from selenium import webdriver
import os
import sys
import traceback
import numpy as np
import pandas as pd
from io import StringIO
import requests
from datetime import datetime
##################################################################

CURRENT_YEAR =  "2025" # str(datetime.now().year)
LEGACY_YEAR = "2001" # LEGACY start year ( from beginnig to 2001 )
MIN_YEAR = LEGACY_YEAR # MIN_YEAR + 1 까지 저장함
TEAM_MIN_YEAR = "2000"
CONST_SLEEP_TIME = 3
###############
NUM_PROCESS = 3
SLEEP_TIME_BEFORE_RETRY = 5
MAX_RETRIES = 5
###############


##################################################################
###############
IS_BLOB = True
###############
if IS_BLOB:
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
    wasb_hook = WasbHook(wasb_conn_id="my_wasb_storage_account")
    # from azure.storage.blob import BlobServiceClient
    container_name = "airflow-outputs"
    # blob_service_client = BlobServiceClient.from_connection_string(os.getenv("AIRFLOW_BLOB_SAS_KEY"))

###############
options = webdriver.ChromeOptions()
options.add_argument('headless')
if IS_BLOB:
    command_executor_url = 'http://selenium-grid-selenium-hub.airflow.svc:4444'
    connection_timeout = 500

    try:
        response = requests.get(command_executor_url, timeout=connection_timeout)
        response.raise_for_status()  # Check for successful status
        print("Selenium Grid is reachable.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to connect to Selenium Grid: {e}")
        # Optionally, exit or retry depending on your use case
        raise SystemExit("Unable to reach Selenium Grid within the timeout period.")

    driver = webdriver.Remote(
        command_executor=command_executor_url,
        options=options
    )
else:
    driver = webdriver.Chrome(options=options)
################
DATASET_NAME = "kbo-datasets"
BATTER_DATASET_NAME = "batter_datasets"
PITCHER_DATASET_NAME = "pitcher_datasets"
FIELDER_DATASET_NAME = "fielding_datasets"
RUNNER_DATASET_NAME = "runner_datasets"
TEAM_DATASET_NAME = "team_datasets"
LEGACY_DATASET_NAME = "legacy"
YEARLY_DATASET_NAME = "yearly"
VERSUS_DATASET_NAME = "versus"

CURRENT_DATASET_NAME = "current"

DATASET_DIR = ""
BATTER_DATASET_DIR = ""
PITCHER_DATASET_DIR = ""
FIELDING_DATASET_DIR = ""
RUNNER_DATASET_DIR = ""
BATTER_YEARLY_DATASET_DIR = ""
BATTER_LEGACY_DATASET_DIR = ""
PITCHER_YEARLY_DATASET_DIR = ""
PITCHER_LEGACY_DATASET_DIR = ""
TEAM_DATASET_DIR = ""
VERSUS_DATASET_DIR = ""
CURRENT_DATASET_DIR = ""
CURRENT_YEAR_DATASET_DIR = ""

if not IS_BLOB:
    DATASET_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), DATASET_NAME)
    if not os.path.exists(DATASET_DIR):
        os.mkdir(DATASET_DIR)
    CURRENT_DATASET_DIR = os.path.join(DATASET_DIR, CURRENT_DATASET_NAME)
    if not os.path.exists(CURRENT_DATASET_DIR):
        os.mkdir(CURRENT_DATASET_DIR)
    CURRENT_YEAR_DATASET_DIR = os.path.join(CURRENT_DATASET_DIR, CURRENT_YEAR)
    if not os.path.exists(CURRENT_YEAR_DATASET_DIR):
        os.mkdir(CURRENT_YEAR_DATASET_DIR)
    BATTER_DATASET_DIR = os.path.join(DATASET_DIR, BATTER_DATASET_NAME)
    if not os.path.exists(BATTER_DATASET_DIR):
        os.mkdir(BATTER_DATASET_DIR)
    PITCHER_DATASET_DIR = os.path.join(DATASET_DIR, PITCHER_DATASET_NAME)
    if not os.path.exists(PITCHER_DATASET_DIR):
        os.mkdir(PITCHER_DATASET_DIR)
    FIELDING_DATASET_DIR = os.path.join(DATASET_DIR, FIELDER_DATASET_NAME)
    if not os.path.exists(FIELDING_DATASET_DIR):
        os.mkdir(FIELDING_DATASET_DIR)
    RUNNER_DATASET_DIR = os.path.join(DATASET_DIR, RUNNER_DATASET_NAME)
    if not os.path.exists(RUNNER_DATASET_DIR):
        os.mkdir(RUNNER_DATASET_DIR)
    TEAM_DATASET_DIR = os.path.join(DATASET_DIR, TEAM_DATASET_NAME)
    if not os.path.exists(TEAM_DATASET_DIR):
        os.mkdir(TEAM_DATASET_DIR)
    VERSUS_DATASET_DIR = os.path.join(DATASET_DIR, VERSUS_DATASET_NAME)
    if not os.path.exists(VERSUS_DATASET_DIR):
        os.mkdir(VERSUS_DATASET_DIR)
    BATTER_YEARLY_DATASET_DIR = os.path.join(BATTER_DATASET_DIR, YEARLY_DATASET_NAME)
    if not os.path.exists(BATTER_YEARLY_DATASET_DIR):
        os.mkdir(BATTER_YEARLY_DATASET_DIR)
    BATTER_LEGACY_DATASET_DIR = os.path.join(BATTER_DATASET_DIR, LEGACY_DATASET_NAME)
    if not os.path.exists(BATTER_LEGACY_DATASET_DIR):
        os.mkdir(BATTER_LEGACY_DATASET_DIR)
    PITCHER_YEARLY_DATASET_DIR = os.path.join(PITCHER_DATASET_DIR, YEARLY_DATASET_NAME)
    if not os.path.exists(PITCHER_YEARLY_DATASET_DIR):
        os.mkdir(PITCHER_YEARLY_DATASET_DIR)
    PITCHER_LEGACY_DATASET_DIR = os.path.join(PITCHER_DATASET_DIR, LEGACY_DATASET_NAME)
    if not os.path.exists(PITCHER_LEGACY_DATASET_DIR):
        os.mkdir(PITCHER_LEGACY_DATASET_DIR)

        

    
###############
ENTIRE_BATTER_NUMBER_NAME_PATH = ""
ENTIRE_PITCHER_NUMBER_NAME_PATH = ""
CURRENT_BATTER_NUMBER_NAME_PATH = ""
CURRENT_PITCHER_NUMBER_NAME_PATH = ""
ENTIRE_BATTER_NUMBER_PATH = ""
ENTIRE_PITCHER_NUMBER_PATH = ""
CURRENT_BATTER_NUMBER_NAME_PATH = ""
CURRENT_PITCHER_NUMBER_NAME_PATH = ""
if IS_BLOB:
    ENTIRE_BATTER_NUMBER_NAME_PATH = os.path.join(DATASET_NAME,"Entire_Batter_Number.csv")
    ENTIRE_PITCHER_NUMBER_NAME_PATH = os.path.join(DATASET_NAME,"Entire_Pitcher_Number.csv")
    CURRENT_BATTER_NUMBER_NAME_PATH = os.path.join(DATASET_NAME,"Current_Batter_Number.csv")
    CURRENT_PITCHER_NUMBER_NAME_PATH = os.path.join(DATASET_NAME,"Current_Pitcher_Number.csv")
else: 
    ENTIRE_BATTER_NUMBER_PATH = os.path.join(DATASET_DIR,"Entire_Batter_Number.csv")
    ENTIRE_PITCHER_NUMBER_PATH = os.path.join(DATASET_DIR,"Entire_Pitcher_Number.csv")
    CURRENT_BATTER_NUMBER_NAME_PATH = os.path.join(DATASET_DIR,"Current_Batter_Number.csv")
    CURRENT_PITCHER_NUMBER_NAME_PATH = os.path.join(DATASET_DIR,"Current_Pitcher_Number.csv")

###############
### Functions ###
def save_df(df, blob_name_path,local_file_path):
    if IS_BLOB:
        parquet_data = df.to_parquet(engine="pyarrow", index=False)
        wasb_hook.load_string(
            string_data=parquet_data,
            container_name=container_name,
            blob_name=blob_name_path,
            overwrite=True
        )
    else:
        df.to_parquet(local_file_path, engine="pyarrow",index=False)


#################
