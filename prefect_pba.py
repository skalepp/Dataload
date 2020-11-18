# from selenium import webdriver
# from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.support.ui import WebDriverWait
# import psycopg2
# import pandas as pd 
# from sqlalchemy import create_engine
# import os
# import time
# import shutil
# import prefect
# from prefect.schedules import IntervalSchedule

# Agent token - scpaQLadevRpYljp8VulMQ

from prefect import task, Flow, Parameter

@task
def extract():
    return [1, 2, 3]


@task
def transform(x):
    return [i * 10 for i in x]


@task
def load(y):
    print("Received y: {}".format(y))


with Flow("ETL") as flow:
    e = extract()
    t = transform(e)
    l = load(t)

flow.run()
