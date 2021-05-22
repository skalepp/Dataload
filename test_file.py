from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
import psycopg2
import pandas as pd 
from sqlalchemy import create_engine
import os
import time
import shutil
from prefect import task

engine = create_engine('postgresql://postgres:baseball@localhost:5432/postgres')

PATH = "C:\Program Files (x86)\chromedriver.exe"
driver = webdriver.Chrome(PATH)

try:
    driver.get("https://statsplus.net/pbal/finance/current?year=2019")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/finances_2019.csv')
    df = pd.read_csv("C:/Users/skale/Downloads/finances_2019.csv")
    new_df = df["Team"].replace({"ABQ": "MIL", "OTT": "COL", "SAN":"WPG"}, inplace=True)
    new_df.to_csv('C:/Users/skale/Downloads/finances_2019.csv', index=False)

except:
    driver.quit()

finances_2019 = pd.read_csv('C:/Users/skale/Downloads/finances_2019.csv', engine='python', index_col=False)
finances_2019.to_sql('finances_2019', engine, if_exists='replace')

driver.quit()