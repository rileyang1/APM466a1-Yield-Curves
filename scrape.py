from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import datetime as dt
import pandas as pd
import numpy as np
import time

from tqdm import tqdm

option = Options()
option.headless = False
driver = webdriver.Chrome(options=option)
bond_dict = {}
start_urls = ['https://markets.businessinsider.com/bonds/finder?borrower=71&maturity=shortterm&yield=&bondtype=2%2/c3%2/c4%2/c16&coupon=&currency=184&rating=&country=19',
               'https://markets.businessinsider.com/bonds/finder?p=2&borrower=71&maturity=shortterm&bondtype=2%/2c3%/2c4%2/c16&currency=184&country=19',
               'https://markets.businessinsider.com/bonds/finder?borrower=71&maturity=midterm&yield=&bondtype=2%2/c3%2/c4%2/c16&coupon=&currency=184&rating=&country=19']
count = 1

# collecting bond urls, coupons, yields and maturity date from starting urls
# individual bond urls are needed to collect historical time series data
for url in start_urls:
    driver.get(url)
    table = driver.find_element(By.XPATH, 
                                    '//*[@id="bond-searchresults-container"]/div/div/div[1]/div[1]/div[2]/div/div/div/table/tbody')
    rows = table.find_elements(By.TAG_NAME, 'tr')

    for row in rows:
        link = row.find_elements(By.TAG_NAME, 'a')[0].get_attribute('href')
        coupon = row.find_elements(By.TAG_NAME, 'td')[2].text
        yield_ = row.find_elements(By.TAG_NAME, 'td')[3].text
        maturityDate = row.find_elements(By.TAG_NAME, 'td')[5].text

        bond_dict[count] = {'link' : link,
                           'coupon' : coupon,
                           'yield' : yield_,
                           'maturityDate' : maturityDate}
        count += 1


# collecting historical time series data, ISIN, name, issue price, issue date 
c = 1
for key, items in tqdm(bond_dict.items()):
    url = bond_dict[key]['link']
    driver = webdriver.Chrome(options=option)
    driver.get(url)

    time.sleep(20)
    change_view_button = WebDriverWait(driver,10).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="DetailChart"]/app-root/div[2]/div/div[1]/div/div[1]/div[2]/div/div/div/div/div[3]')))
    change_view_button.click()
    time.sleep(10)

    chart = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="DetailChart"]/app-root/div[2]/app-detail-chart/div[2]/div[13]/div')))
    loc = chart.location
    size = chart.size

    bond_dict[key]['ISIN'] = driver.find_element(By.XPATH, 
                               '/html/body/main/div/div[4]/div[2]/div/div/div[1]/div[2]/div/div/div/table/tbody/tr[1]/td[2]').text
    bond_dict[key]['name'] = driver.find_element(By.XPATH, 
                               '/html/body/main/div/div[4]/div[2]/div/div/div[1]/div[2]/div/div/div/table/tbody/tr[2]/td[2]').text
    bond_dict[key]['issuePrice'] = driver.find_element(By.XPATH, 
                                     '/html/body/main/div/div[4]/div[2]/div/div/div[1]/div[2]/div/div/div/table/tbody/tr[8]/td[2]').text
    bond_dict[key]['issueDate'] = driver.find_element(By.XPATH, 
                                     '/html/body/main/div/div[4]/div[2]/div/div/div[1]/div[2]/div/div/div/table/tbody/tr[9]/td[2]').text
    
    action = webdriver.ActionChains(driver)
    action.move_to_element(chart).perform()
    action.move_by_offset(240,0).perform()

    close_ts = {}
    pace = -20
    while True:
        tooltipdate = driver.find_element(By.XPATH,'//*[@id="DetailChart"]/app-root/div[2]/app-detail-chart/div[2]/stx-hu-tooltip/stx-hu-tooltip-field[1]').text[5:9].strip()
        tooltipclose = driver.find_element(By.XPATH,'//*[@id="DetailChart"]/app-root/div[2]/app-detail-chart/div[2]/stx-hu-tooltip/stx-hu-tooltip-field[2]').text[6:]

        if tooltipdate:
            close_ts[tooltipdate] = tooltipclose
        else:
            if len(close_ts) < 10:
                print(f"{bond_dict[key]['name']} does not have sufficient ts data.")
            break

        try:
            action.move_by_offset(pace, 0).perform()
        except:
            print(f"{bond_dict[key]['name']}'s data has been scraped.")
            break
    
        time.sleep(2)
    
    bond_dict[key]['closeTS'] = close_ts
    driver.close()
    
    c += 1

df = pd.DataFrame.from_dict(bond_dict, orient='index')
df.drop(['link', 'name'], axis=1, inplace=True)
df = pd.concat([df.drop(['closeTS'], axis=1), df['closeTS'].apply(pd.Series)], axis=1)

for c in df.columns:
    if c[-1] == '/':
        df = df.rename({c : c[:-1]}, axis=1)

df.coupon = df.coupon.apply(str)
df['yield'] = df['yield'].astype(str)

for c in ['coupon', 'yield']:
    df[c] = df[c].apply(lambda x: x[:-1])

df.maturityDate = pd.to_datetime(df.maturityDate)
df.issueDate = pd.to_datetime(df.issueDate)

prefix = 'CAN'
newNames = []

for i in range(df.shape[0]):
    d = df.iloc[i]['maturityDate'].strftime('%b %y')
    c = df.iloc[i]['coupon'][:4]
    newName = str(prefix + ' ' + c + ' ' + d)
    newNames.append(newName)

df.index = newNames
    
for c in ['coupon', 'yield', 'issuePrice', '2/9', '2/8', '2/7', '2/6', '2/3', '2/2', '2/1', '1/31',
       '1/30', '1/27', '1/26', '1/25', '1/24', '1/23', '1/20', '1/19', '1/18',
       '1/17', '1/16', '1/13', '1/12', '1/11', '1/10']:
    df[c] = pd.to_numeric(df[c], errors='coerce')

df = df.round(2)  
df.to_csv('bond_ts_9Feb.csv')