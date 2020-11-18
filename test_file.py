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


driver.get("https://statsplus.net/pbal/war")
button = driver.find_element_by_xpath('//button[text()="CSV"]')
button.click()
time.sleep(2)
os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/team_record_current.csv')
df = pd.read_csv("C:/Users/skale/Downloads/team_record_current.csv")
left = df['Team'].str[:3]
left = left.str.upper()
final_df = [df, left]
final_df = pd.concat(final_df, axis=1)
final_df.to_csv('C:/Users/skale/Downloads/team_record_current.csv', index=False)
driver.quit()