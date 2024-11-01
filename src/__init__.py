from selenium import webdriver
import os
import sys
import traceback
import numpy as np
import pandas as pd
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

###############
IS_BLOB = True

if IS_BLOB:
    wasb_hook = WasbHook(wasb_conn_id="my_wasb_storage_account")
    container_name = "airflow-outputs"

###############
options = webdriver.ChromeOptions()
options.add_argument('headless')
driver = webdriver.Remote(
    command_executor='http://seleniarm:4444/wd/hub',
    options=options
)
################
pd.set_option("future.no_silent_downcasting", True)
################
DATASET_NAME = "kbo-datasets"
BATTER_DATASET_NAME = "batter_datasets"
PITCHER_DATASET_NAME = "pitcher_datasets"
FIELDER_DATASET_NAME = "fielding_datasets"
RUNNER_DATASET_NAME = "runner_datasets"
################
DATASET_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), DATASET_NAME)
if not os.path.exists(DATASET_DIR):
    os.mkdir(DATASET_DIR)
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


###############

ENTIRE_BATTER_NUMBER_PATH = os.path.join(DATASET_DIR,"Entire_Batter_Number.csv")
ENTIRE_PITCHER_NUMBER_PATH = os.path.join(DATASET_DIR,"Entire_Pitcher_Number.csv")
ENTIRE_FIELDER_NUMBER_PATH = os.path.join(DATASET_DIR,"Entire_Fielder_Number.csv")

ENTIRE_BATTER_NUMBER_NAME_PATH = os.path.join(DATASET_NAME,"Entire_Batter_Number.csv")
ENTIRE_PITCHER_NUMBER_NAME_PATH = os.path.join(DATASET_NAME,"Entire_Pitcher_Number.csv")
ENTIRE_FIELDER_NUMBER_NAME_PATH = os.path.join(DATASET_NAME,"Entire_Fielder_Number.csv")

###############

MIN_YEAR = "2023" # MIN_YEAR + 1 까지 저장함
CONST_SLEEP_TIME = 1

###############

NUM_PROCESS = 3
SLEEP_TIME_BEFORE_RETRY = 5
MAX_RETRIES = 3
