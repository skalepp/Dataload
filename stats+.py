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

## To update driver (http://chromedriver.storage.googleapis.com/index.html)

engine = create_engine('postgresql://postgres:baseball@localhost:5432/postgres')

PATH = "C:\Program Files (x86)\chromedriver.exe"
driver = webdriver.Chrome(PATH)

download_file_2020 = os.path.expanduser("~/Downloads/draft_2020.csv")
download_file_2021 = os.path.expanduser("~/Downloads/draft_2021.csv")
download_file_2022 = os.path.expanduser("~/Downloads/draft_2022.csv")
download_file_2023 = os.path.expanduser("~/Downloads/draft_2023.csv")
download_file_2024 = os.path.expanduser("~/Downloads/draft_2024.csv")
download_file_2025 = os.path.expanduser("~/Downloads/draft_2025.csv")
download_file_2026 = os.path.expanduser("~/Downloads/draft_2026.csv")
download_file_2027 = os.path.expanduser("~/Downloads/draft_2027.csv")

batters_alltime_standard = os.path.expanduser("~/Downloads/batters_alltime_standard.csv")
batters_2019_standard = os.path.expanduser("~/Downloads/batters_2019_standard.csv")
batters_2020_standard = os.path.expanduser("~/Downloads/batters_2020_standard.csv")
batters_2021_standard = os.path.expanduser("~/Downloads/batters_2021_standard.csv")
batters_2022_standard = os.path.expanduser("~/Downloads/batters_2022_standard.csv")
batters_2023_standard = os.path.expanduser("~/Downloads/batters_2023_standard.csv")
batters_2024_standard = os.path.expanduser("~/Downloads/batters_2024_standard.csv")
batters_2025_standard = os.path.expanduser("~/Downloads/batters_2025_standard.csv")
batters_2026_standard = os.path.expanduser("~/Downloads/batters_2026_standard.csv")
batters_2027_standard = os.path.expanduser("~/Downloads/batters_2027_standard.csv")

batters_alltime_advanced = os.path.expanduser("~/Downloads/batters_alltime_advanced.csv")
pitchers_alltime_standard = os.path.expanduser("~/Downloads/pitchers_alltime_standard.csv")
pitchers_alltime_advanced = os.path.expanduser("~/Downloads/pitchers_alltime_advanced.csv")
fielding_alltime = os.path.expanduser("~/Downloads/fielding_alltime.csv")

team_2019 = os.path.expanduser("~/Downloads/team_2019.csv")
team_2020 = os.path.expanduser("~/Downloads/team_2020.csv")
team_2021 = os.path.expanduser("~/Downloads/team_2021.csv")
team_2022 = os.path.expanduser("~/Downloads/team_2022.csv")
team_2023 = os.path.expanduser("~/Downloads/team_2023.csv")
team_2024 = os.path.expanduser("~/Downloads/team_2024.csv")
team_2025 = os.path.expanduser("~/Downloads/team_2025.csv")
team_2026 = os.path.expanduser("~/Downloads/team_2026.csv")
team_2027 = os.path.expanduser("~/Downloads/team_2027.csv")

finances_2019 = os.path.expanduser("~/Downloads/finances_2019.csv")
finances_2020 = os.path.expanduser("~/Downloads/finances_2020.csv")
finances_2021 = os.path.expanduser("~/Downloads/finances_2021.csv")
finances_2022 = os.path.expanduser("~/Downloads/finances_2022.csv")
finances_2023 = os.path.expanduser("~/Downloads/finances_2023.csv")
finances_2024 = os.path.expanduser("~/Downloads/finances_2024.csv")
finances_2025 = os.path.expanduser("~/Downloads/finances_2025.csv")
finances_2026 = os.path.expanduser("~/Downloads/finances_2026.csv")
finances_2027 = os.path.expanduser("~/Downloads/finances_2027.csv")

team_record_current = os.path.expanduser("~/Downloads/team_record_current.csv")

# remove stale files
if os.path.isfile(download_file_2020):
    os.remove(download_file_2020)

if os.path.isfile(download_file_2021):
    os.remove(download_file_2021)

if os.path.isfile(download_file_2022):
    os.remove(download_file_2022)

if os.path.isfile(download_file_2023):
    os.remove(download_file_2023)

if os.path.isfile(download_file_2024):
    os.remove(download_file_2024)

if os.path.isfile(download_file_2025):
    os.remove(download_file_2025)

if os.path.isfile(download_file_2026):
    os.remove(download_file_2026)

if os.path.isfile(download_file_2027):
    os.remove(download_file_2027)

if os.path.isfile(batters_alltime_standard):
    os.remove(batters_alltime_standard)

if os.path.isfile(batters_2019_standard):
    os.remove(batters_2019_standard)

if os.path.isfile(batters_2020_standard):
    os.remove(batters_2020_standard)

if os.path.isfile(batters_2021_standard):
    os.remove(batters_2021_standard)

if os.path.isfile(batters_2022_standard):
    os.remove(batters_2022_standard)

if os.path.isfile(batters_2023_standard):
    os.remove(batters_2023_standard)

if os.path.isfile(batters_2024_standard):
    os.remove(batters_2024_standard)

if os.path.isfile(batters_2025_standard):
    os.remove(batters_2025_standard)

if os.path.isfile(batters_2026_standard):
    os.remove(batters_2026_standard)

if os.path.isfile(batters_2027_standard):
    os.remove(batters_2027_standard)

if os.path.isfile(batters_alltime_advanced):
    os.remove(batters_alltime_advanced)

if os.path.isfile(pitchers_alltime_standard):
    os.remove(pitchers_alltime_standard)

if os.path.isfile(pitchers_alltime_advanced):
    os.remove(pitchers_alltime_advanced)

if os.path.isfile(fielding_alltime):
    os.remove(fielding_alltime)

if os.path.isfile(team_2019):
    os.remove(team_2019)

if os.path.isfile(team_2020):
    os.remove(team_2020)

if os.path.isfile(team_2021):
    os.remove(team_2021)

if os.path.isfile(team_2022):
    os.remove(team_2022)

if os.path.isfile(team_2023):
    os.remove(team_2023)

if os.path.isfile(team_2024):
    os.remove(team_2024)

if os.path.isfile(team_2025):
    os.remove(team_2025)

if os.path.isfile(team_2026):
    os.remove(team_2026)

if os.path.isfile(team_2027):
    os.remove(team_2027)

if os.path.isfile(finances_2019):
    os.remove(finances_2019)

if os.path.isfile(finances_2020):
    os.remove(finances_2020)

if os.path.isfile(finances_2021):
    os.remove(finances_2021)

if os.path.isfile(finances_2022):
    os.remove(finances_2022)

if os.path.isfile(finances_2023):
    os.remove(finances_2023)

if os.path.isfile(finances_2024):
    os.remove(finances_2024)

if os.path.isfile(finances_2025):
    os.remove(finances_2025)

if os.path.isfile(finances_2026):
    os.remove(finances_2026)

if os.path.isfile(finances_2027):
    os.remove(finances_2027)

if os.path.isfile(team_record_current):
    os.remove(team_record_current)

try:
    driver.get("https://statsplus.net/pbal/draftyear?year=2020")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/draft_2020.csv')

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/draftyear?year=2021")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/draft_2021.csv')

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/draftyear?year=2022")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/draft_2022.csv')

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/draftyear?year=2023")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/draft_2023.csv')

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/draftyear?year=2024")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/draft_2024.csv')

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/draftyear?year=2025")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/draft_2025.csv')

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/draftyear?year=2026")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/draft_2026.csv')

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/draftyear?year=2027")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/draft_2027.csv')

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/playerstats?sort=pa,d&stat=bat&team=All&qual=Qual&pos=All&more=true&games=reg&startyear=2019&endyear=2027&rightleft=All&playerstatus=any&type=1")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/batters_alltime_standard.csv')

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/playerstats/?sort=pa,d&stat=bat&team=All&qual=Qual&pos=All&records=100&more=true&games=reg&startyear=2019&endyear=2019&rightleft=All&playerstatus=any&timespan=yrs&startdate=2028-04-03&enddate=2028-04-03&type=1")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/batters_2019_standard.csv')

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/playerstats/?sort=pa,d&stat=bat&team=All&qual=Qual&pos=All&records=100&more=true&games=reg&startyear=2020&endyear=2020&rightleft=All&playerstatus=any&timespan=yrs&startdate=2028-04-03&enddate=2028-04-03&type=1")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/batters_2020_standard.csv')

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/playerstats/?sort=pa,d&stat=bat&team=All&qual=Qual&pos=All&records=100&more=true&games=reg&startyear=2021&endyear=2021&rightleft=All&playerstatus=any&timespan=yrs&startdate=2028-04-03&enddate=2028-04-03&type=1")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/batters_2021_standard.csv')

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/playerstats/?sort=pa,d&stat=bat&team=All&qual=Qual&pos=All&records=100&more=true&games=reg&startyear=2022&endyear=2022&rightleft=All&playerstatus=any&timespan=yrs&startdate=2028-04-03&enddate=2028-04-03&type=1")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/batters_2022_standard.csv')

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/playerstats/?sort=pa,d&stat=bat&team=All&qual=Qual&pos=All&records=100&more=true&games=reg&startyear=2023&endyear=2023&rightleft=All&playerstatus=any&timespan=yrs&startdate=2028-04-03&enddate=2028-04-03&type=1")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/batters_2023_standard.csv')

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/playerstats/?sort=pa,d&stat=bat&team=All&qual=Qual&pos=All&records=100&more=true&games=reg&startyear=2024&endyear=2024&rightleft=All&playerstatus=any&timespan=yrs&startdate=2028-04-03&enddate=2028-04-03&type=1")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/batters_2024_standard.csv')

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/playerstats/?sort=pa,d&stat=bat&team=All&qual=Qual&pos=All&records=100&more=true&games=reg&startyear=2025&endyear=2025&rightleft=All&playerstatus=any&timespan=yrs&startdate=2028-04-03&enddate=2028-04-03&type=1")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/batters_2025_standard.csv')

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/playerstats/?sort=pa,d&stat=bat&team=All&qual=Qual&pos=All&records=100&more=true&games=reg&startyear=2026&endyear=2026&rightleft=All&playerstatus=any&timespan=yrs&startdate=2028-04-03&enddate=2028-04-03&type=1")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/batters_2026_standard.csv')

except:
    driver.quit()

try:
<<<<<<< HEAD
    driver.get("https://statsplus.net/pbal/playerstats/?sort=pa,d&stat=bat&team=All&qual=Qual&pos=All&records=100&more=true&games=reg&startyear=2027&endyear=2027&rightleft=All&playerstatus=any&timespan=yrs&startdate=2028-04-03&enddate=2028-04-03&type=1")
=======
    driver.get("https://statsplus.net/pbal/playerstats?sort=pa,d&stat=bat&team=All&qual=Qual&pos=All&more=true&games=reg&startyear=2027&endyear=2027&rightleft=All&playerstatus=any&type=1")
>>>>>>> 6b04f26646caf74b8bd5e55e906e42d2fcba279f
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/batters_2027_standard.csv')

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/playerstats?stat=bat&team=All&pos=All&playerstatus=any&more=true&games=reg&rightleft=All&startyear=2019&endyear=2027&qual=Qual&sort=war,d&type=2")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/batters_alltime_advanced.csv')
    df = pd.read_csv("C:/Users/skale/Downloads/batters_alltime_advanced.csv")
    new_df = df.rename(columns = {"BB%": "BB PCT", 
                                  "K%":"K PCT",
                                  "WAR   &nbsp;":"WAR"})
    new_df.to_csv('C:/Users/skale/Downloads/batters_alltime_advanced.csv', index=False)

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/playerstats?stat=pitch&team=All&pos=All&playerstatus=any&more=true&games=reg&rightleft=All&startyear=2019&endyear=2027&qual=Qual&sort=era,a&type=1")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/pitchers_alltime_standard.csv')

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/playerstats?stat=pitch&team=All&pos=All&playerstatus=any&more=true&games=reg&rightleft=All&startyear=2019&endyear=2027&qual=Qual&sort=war,d&type=2")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/pitchers_alltime_advanced.csv')
    df = pd.read_csv("C:/Users/skale/Downloads/pitchers_alltime_advanced.csv")
    new_df = df.rename(columns = {"BB%": "BB PCT", 
                                  "K%":"K PCT",
                                  "K-BB%":"K-BB PCT",
                                  "LOB%":"LOB PCT",
                                  "WAR   &nbsp;":"WAR"})
    new_df.to_csv('C:/Users/skale/Downloads/pitchers_alltime_advanced.csv', index=False)

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/playerstats?sort=zr,d&stat=field&team=All&qual=Qual&pos=0&more=true&startyear=2019&endyear=2027&playerstatus=any&type=0")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/fielding_alltime.csv')
    df = pd.read_csv("C:/Users/skale/Downloads/fielding_alltime.csv")
    new_df = df.rename(columns = {"F%": "FIELDING PCT", 
                                  "ZR   &nbsp;":"ZR"})
    new_df.to_csv('C:/Users/skale/Downloads/fielding_alltime.csv', index=False)

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/stats/batting?year=2019&split=1")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/team_2019.csv')
    df = pd.read_csv("C:/Users/skale/Downloads/team_2019.csv")
    new_df = df.rename(columns = {"BB%": "BB PCT", 
                                  "K%":"K PCT"})
    new_df.to_csv('C:/Users/skale/Downloads/team_2019.csv', index=False)

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/stats/batting?year=2020&split=1")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/team_2020.csv')
    df = pd.read_csv("C:/Users/skale/Downloads/team_2020.csv")
    new_df = df.rename(columns = {"BB%": "BB PCT", 
                                  "K%":"K PCT"})
    new_df.to_csv('C:/Users/skale/Downloads/team_2020.csv', index=False)

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/stats/batting?year=2021&split=1")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/team_2021.csv')
    df = pd.read_csv("C:/Users/skale/Downloads/team_2021.csv")
    new_df = df.rename(columns = {"BB%": "BB PCT", 
                                  "K%":"K PCT"})
    new_df.to_csv('C:/Users/skale/Downloads/team_2021.csv', index=False)

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/stats/batting?year=2022&split=1")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/team_2022.csv')
    df = pd.read_csv("C:/Users/skale/Downloads/team_2022.csv")
    new_df = df.rename(columns = {"BB%": "BB PCT", 
                                  "K%":"K PCT"})
    new_df.to_csv('C:/Users/skale/Downloads/team_2022.csv', index=False)

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/stats/batting?year=2023&split=1")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/team_2023.csv')
    df = pd.read_csv("C:/Users/skale/Downloads/team_2023.csv")
    new_df = df.rename(columns = {"BB%": "BB PCT", 
                                  "K%":"K PCT"})
    new_df.to_csv('C:/Users/skale/Downloads/team_2023.csv', index=False)

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/stats/batting?year=2024&split=1")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/team_2024.csv')
    df = pd.read_csv("C:/Users/skale/Downloads/team_2024.csv")
    new_df = df.rename(columns = {"BB%": "BB PCT", 
                                  "K%":"K PCT"})
    new_df.to_csv('C:/Users/skale/Downloads/team_2024.csv', index=False)

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/stats/batting?year=2025&split=1")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/team_2025.csv')
    df = pd.read_csv("C:/Users/skale/Downloads/team_2025.csv")
    new_df = df.rename(columns = {"BB%": "BB PCT", 
                                  "K%":"K PCT"})
    new_df.to_csv('C:/Users/skale/Downloads/team_2025.csv', index=False)

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/stats/batting?year=2026&split=1")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/team_2026.csv')
    df = pd.read_csv("C:/Users/skale/Downloads/team_2026.csv")
    new_df = df.rename(columns = {"BB%": "BB PCT", 
                                  "K%":"K PCT"})
    new_df.to_csv('C:/Users/skale/Downloads/team_2026.csv', index=False)

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/stats/batting?year=2027&split=1")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/team_2027.csv')
    df = pd.read_csv("C:/Users/skale/Downloads/team_2027.csv")
    new_df = df.rename(columns = {"BB%": "BB PCT", 
                                  "K%":"K PCT"})
    new_df.to_csv('C:/Users/skale/Downloads/team_2027.csv', index=False)

except:
    driver.quit()

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

try:
    driver.get("https://statsplus.net/pbal/finance/current?year=2020")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/finances_2020.csv')
    df = pd.read_csv("C:/Users/skale/Downloads/finances_2020.csv")
    new_df = df["Team"].replace({"KCH": "MIL", "OTT": "COL", "SAN":"WPG"}, inplace=True)
    new_df.to_csv('C:/Users/skale/Downloads/finances_2020.csv', index=False)

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/finance/current?year=2021")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/finances_2021.csv')
    df = pd.read_csv("C:/Users/skale/Downloads/finances_2021.csv")
    new_df = df["Team"].replace({"ANA": "MIL", "OTT": "COL", "SAN":"WPG"}, inplace=True)
    new_df.to_csv('C:/Users/skale/Downloads/finances_2021.csv', index=False)

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/finance/current?year=2022")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/finances_2022.csv')
    df = pd.read_csv("C:/Users/skale/Downloads/finances_2022.csv")
    #df = df["Team"].replace({"ABQ": "MIL", "OTT": "COL", "SAN":"WPG"}, inplace=True)
    df.to_csv('C:/Users/skale/Downloads/finances_2022.csv', index=False)

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/finance/current?year=2023")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/finances_2023.csv')
    df = pd.read_csv("C:/Users/skale/Downloads/finances_2023.csv")
    #df = df["Team"].replace({"ABQ": "MIL", "OTT": "COL", "SAN":"WPG"}, inplace=True)
    df.to_csv('C:/Users/skale/Downloads/finances_2023.csv', index=False)

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/finance/current?year=2024")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/finances_2024.csv')
    df = pd.read_csv("C:/Users/skale/Downloads/finances_2024.csv")
    #df = df["Team"].replace({"ABQ": "MIL", "OTT": "COL", "SAN":"WPG"}, inplace=True)
    df.to_csv('C:/Users/skale/Downloads/finances_2024.csv', index=False)

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/finance/current?year=2025")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/finances_2025.csv')
    df = pd.read_csv("C:/Users/skale/Downloads/finances_2025.csv")
    #df = df["Team"].replace({"ABQ": "MIL", "OTT": "COL", "SAN":"WPG"}, inplace=True)
    df.to_csv('C:/Users/skale/Downloads/finances_2025.csv', index=False)

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/finance/current?year=2026")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/finances_2026.csv')
    df = pd.read_csv("C:/Users/skale/Downloads/finances_2026.csv")
    #df = df["Team"].replace({"ABQ": "MIL", "OTT": "COL", "SAN":"WPG"}, inplace=True)
    df.to_csv('C:/Users/skale/Downloads/finances_2026.csv', index=False)

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/finance/current?year=2027")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/finances_2027.csv')
    df = pd.read_csv("C:/Users/skale/Downloads/finances_2027.csv")
    df = df["Team"].replace({"ABQ": "MIL", "OTT": "COL", "SAN":"WPG"}, inplace=True)
    df.to_csv('C:/Users/skale/Downloads/finances_2027.csv', index=False)

except:
    driver.quit()

try:
    driver.get("https://statsplus.net/pbal/war")
    button = driver.find_element_by_xpath('//button[text()="CSV"]')
    button.click()
    time.sleep(2)
    os.rename('C:/Users/skale/Downloads/statsplus.csv', 'C:/Users/skale/Downloads/team_record_current.csv')
    df = pd.read_csv("C:/Users/skale/Downloads/team_record_current.csv")
    left = df['Team'].str[-3:]
    left = left.str.upper()
    final_df = [df, left]
    final_df = pd.concat(final_df, axis=1)
    final_df.to_csv('C:/Users/skale/Downloads/team_record_current.csv', index=False)

except:
    driver.quit()

driver.quit()

draft_2020 = pd.read_csv('C:/Users/skale/Downloads/draft_2020.csv', engine='python', index_col=False)
draft_2020.to_sql('draft_2020', engine, if_exists='replace')

draft_2021 = pd.read_csv('C:/Users/skale/Downloads/draft_2021.csv', engine='python', index_col=False)
draft_2021.to_sql('draft_2021', engine, if_exists='replace')

draft_2022 = pd.read_csv('C:/Users/skale/Downloads/draft_2022.csv', engine='python', index_col=False)
draft_2022.to_sql('draft_2022', engine, if_exists='replace')

draft_2023 = pd.read_csv('C:/Users/skale/Downloads/draft_2023.csv', engine='python', index_col=False)
draft_2023.to_sql('draft_2023', engine, if_exists='replace')

draft_2024 = pd.read_csv('C:/Users/skale/Downloads/draft_2024.csv', engine='python', index_col=False)
draft_2024.to_sql('draft_2024', engine, if_exists='replace')

draft_2025 = pd.read_csv('C:/Users/skale/Downloads/draft_2025.csv', engine='python', index_col=False)
draft_2025.to_sql('draft_2025', engine, if_exists='replace')

draft_2026 = pd.read_csv('C:/Users/skale/Downloads/draft_2026.csv', engine='python', index_col=False)
draft_2026.to_sql('draft_2026', engine, if_exists='replace')

draft_2027 = pd.read_csv('C:/Users/skale/Downloads/draft_2027.csv', engine='python', index_col=False)
draft_2027.to_sql('draft_2027', engine, if_exists='replace')

missing_players = pd.read_csv('C:/Users/skale/Documents/PBA Stat Files/missing_players.csv', engine='python', index_col=False)
missing_players.to_sql('missing_players', engine, if_exists='replace')

batters_alltime_standard = pd.read_csv('C:/Users/skale/Downloads/batters_alltime_standard.csv', engine='python', index_col=False)
batters_alltime_standard.to_sql('batters_alltime_standard', engine, if_exists='replace')

batters_2019_standard = pd.read_csv('C:/Users/skale/Downloads/batters_2019_standard.csv', engine='python', index_col=False)
batters_2019_standard.to_sql('batters_2019_standard', engine, if_exists='replace')

batters_2020_standard = pd.read_csv('C:/Users/skale/Downloads/batters_2020_standard.csv', engine='python', index_col=False)
batters_2020_standard.to_sql('batters_2020_standard', engine, if_exists='replace')

batters_2021_standard = pd.read_csv('C:/Users/skale/Downloads/batters_2021_standard.csv', engine='python', index_col=False)
batters_2021_standard.to_sql('batters_2021_standard', engine, if_exists='replace')

batters_2022_standard = pd.read_csv('C:/Users/skale/Downloads/batters_2022_standard.csv', engine='python', index_col=False)
batters_2022_standard.to_sql('batters_2022_standard', engine, if_exists='replace')

batters_2023_standard = pd.read_csv('C:/Users/skale/Downloads/batters_2023_standard.csv', engine='python', index_col=False)
batters_2023_standard.to_sql('batters_2023_standard', engine, if_exists='replace')

batters_2024_standard = pd.read_csv('C:/Users/skale/Downloads/batters_2024_standard.csv', engine='python', index_col=False)
batters_2024_standard.to_sql('batters_2024_standard', engine, if_exists='replace')

batters_2025_standard = pd.read_csv('C:/Users/skale/Downloads/batters_2025_standard.csv', engine='python', index_col=False)
batters_2025_standard.to_sql('batters_2025_standard', engine, if_exists='replace')

batters_2026_standard = pd.read_csv('C:/Users/skale/Downloads/batters_2026_standard.csv', engine='python', index_col=False)
batters_2026_standard.to_sql('batters_2026_standard', engine, if_exists='replace')

batters_2027_standard = pd.read_csv('C:/Users/skale/Downloads/batters_2027_standard.csv', engine='python', index_col=False)
batters_2027_standard.to_sql('batters_2027_standard', engine, if_exists='replace')

batters_alltime_advanced = pd.read_csv('C:/Users/skale/Downloads/batters_alltime_advanced.csv', engine='python', index_col=False)
batters_alltime_advanced.to_sql('batters_alltime_advanced', engine, if_exists='replace')

pitchers_alltime_standard = pd.read_csv('C:/Users/skale/Downloads/pitchers_alltime_standard.csv', engine='python', index_col=False)
pitchers_alltime_standard.to_sql('pitchers_alltime_standard', engine, if_exists='replace')

pitchers_alltime_advanced = pd.read_csv('C:/Users/skale/Downloads/pitchers_alltime_advanced.csv', engine='python', index_col=False)
pitchers_alltime_advanced.to_sql('pitchers_alltime_advanced', engine, if_exists='replace')

fielding_alltime = pd.read_csv('C:/Users/skale/Downloads/fielding_alltime.csv', engine='python', index_col=False)
fielding_alltime.to_sql('fielding_alltime', engine, if_exists='replace')

team_2019 = pd.read_csv('C:/Users/skale/Downloads/team_2019.csv', engine='python', index_col=False)
team_2019.to_sql('team_2019', engine, if_exists='replace')

team_2020 = pd.read_csv('C:/Users/skale/Downloads/team_2020.csv', engine='python', index_col=False)
team_2020.to_sql('team_2020', engine, if_exists='replace')

team_2021 = pd.read_csv('C:/Users/skale/Downloads/team_2021.csv', engine='python', index_col=False)
team_2021.to_sql('team_2021', engine, if_exists='replace')

team_2022 = pd.read_csv('C:/Users/skale/Downloads/team_2022.csv', engine='python', index_col=False)
team_2022.to_sql('team_2022', engine, if_exists='replace')

team_2023 = pd.read_csv('C:/Users/skale/Downloads/team_2023.csv', engine='python', index_col=False)
team_2023.to_sql('team_2023', engine, if_exists='replace')

team_2024 = pd.read_csv('C:/Users/skale/Downloads/team_2024.csv', engine='python', index_col=False)
team_2024.to_sql('team_2024', engine, if_exists='replace')

team_2025 = pd.read_csv('C:/Users/skale/Downloads/team_2025.csv', engine='python', index_col=False)
team_2025.to_sql('team_2025', engine, if_exists='replace')

team_2026 = pd.read_csv('C:/Users/skale/Downloads/team_2026.csv', engine='python', index_col=False)
team_2026.to_sql('team_2026', engine, if_exists='replace')

team_2027 = pd.read_csv('C:/Users/skale/Downloads/team_2027.csv', engine='python', index_col=False)
team_2027.to_sql('team_2027', engine, if_exists='replace')

finances_2019 = pd.read_csv('C:/Users/skale/Downloads/finances_2019.csv', engine='python', index_col=False)
finances_2019.to_sql('finances_2019', engine, if_exists='replace')

finances_2020 = pd.read_csv('C:/Users/skale/Downloads/finances_2020.csv', engine='python', index_col=False)
finances_2020.to_sql('finances_2020', engine, if_exists='replace')

finances_2021 = pd.read_csv('C:/Users/skale/Downloads/finances_2021.csv', engine='python', index_col=False)
finances_2021.to_sql('finances_2021', engine, if_exists='replace')

finances_2022 = pd.read_csv('C:/Users/skale/Downloads/finances_2022.csv', engine='python', index_col=False)
finances_2022.to_sql('finances_2022', engine, if_exists='replace')

finances_2023 = pd.read_csv('C:/Users/skale/Downloads/finances_2023.csv', engine='python', index_col=False)
finances_2023.to_sql('finances_2023', engine, if_exists='replace')

finances_2024 = pd.read_csv('C:/Users/skale/Downloads/finances_2024.csv', engine='python', index_col=False)
finances_2024.to_sql('finances_2024', engine, if_exists='replace')

finances_2025 = pd.read_csv('C:/Users/skale/Downloads/finances_2025.csv', engine='python', index_col=False)
finances_2025.to_sql('finances_2025', engine, if_exists='replace')

finances_2026 = pd.read_csv('C:/Users/skale/Downloads/finances_2026.csv', engine='python', index_col=False)
finances_2026.to_sql('finances_2026', engine, if_exists='replace')

finances_2027 = pd.read_csv('C:/Users/skale/Downloads/finances_2027.csv', engine='python', index_col=False)
finances_2027.to_sql('finances_2027', engine, if_exists='replace')

team_record_current = pd.read_csv('C:/Users/skale/Downloads/team_record_current.csv', engine='python', index_col=False)
team_record_current.to_sql('team_record_current', engine, if_exists='replace')