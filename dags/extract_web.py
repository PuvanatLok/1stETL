## Library for Web Scraping
import selenium
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
## Library for data transformation
import time
from datetime import date
import pandas as pd
import numpy as np
## Library for NoSQL Connection
import pymongo
from pymongo import MongoClient

def extractWeb():
    ## Extract section
    baseUrl = 'https://www.imdb.com/chart/toptv/?ref_=nv_tvv_250'
    driver = webdriver.Chrome(ChromeDriverManager().install())
    driver.get(baseUrl) 
    links = driver.find_elements(By.XPATH, '//*[@id="sidebar"]/div[7]/span/ul/li/a')
    category = [cat.text for cat in driver.find_elements(By.XPATH, '//*[@id="sidebar"]/div[7]/span/ul/li')]
    Links = [link.get_attribute('href') for link in links]

    ## list to keep information
    title = []
    rating = []
    votes = []
    stars = []
    genres = []
    duration = []
    desc = []
    years = []

    ## Start fetching Information
    for i in range(len(Links)):
        driver.get(Links[i])

        ## get all amount of data
        try:
            ## If ',' exist in number
            if ',' in driver.find_element(By.XPATH, '//*[@id="main"]/div/div[1]/div[2]/span').text.split()[2]:
                all_movie = int(''.join(driver.find_element(By.XPATH, '//*[@id="main"]/div/div[1]/div[2]/span[1]').text.split()[2].split(',')))
            else:
                all_movie = int(driver.find_element(By.XPATH, '//*[@id="main"]/div/div[1]/div[2]/span').text.split()[2])
        except:
            all_movie = int(driver.find_element(By.XPATH, "//*[@id='main']/div/div[1]/div[2]/span").text.split(' ')[0])

        # use count to get through all the pages
        count = 0
        # Check page now
        page = 0

        ## Run each pages
        while count != all_movie:
                ## Get title of movies
                for name in driver.find_elements(By.XPATH, '//*[@id="main"]/div/div[3]/div/div/div[3]/h3/a'):
                    title.append(name.text)
                    ## Get Runtime and Genres of movies
                for i in driver.find_elements(By.XPATH, '//*[@id="main"]/div/div[3]/div/div/div[3]/p[1]'):
                    try:
                        if 'min' not in i.text:
                            duration.append(None)
                            genres.append(i.text.split('|')[1].strip())
                        elif 'min' in i.text.split('|')[0]:
                            duration.append(i.text.split('|')[0])
                            genres.append(i.text.split('|')[1].strip())
                        else:
                            text = i.text.split('|')[1:]
                            duration.append(text[0].strip())
                            genres.append(text[1].strip())
                    except:
                        genres.append(i.text)

                ## Get Rating of movies
                for rate in driver.find_elements(By.XPATH, '//*[@id="main"]/div/div[3]/div/div/div[3]/div/div[1]/strong'):
                    rating.append(rate.text)

                ## Get Vote of movies
                for vote in driver.find_elements(By.XPATH, '//*[@id="main"]/div/div[3]/div/div/div[3]/p[4]/span[2]'):
                    votes.append(vote.text)

                ## Get Star of movies
                for star in driver.find_elements(By.XPATH, '//*[@id="main"]/div/div[3]/div/div/div[3]/p[3]'):
                    stars.append(star.text.split(':')[-1].strip())

                ## Get Description of movies
                for des in driver.find_elements(By.XPATH, '//*[@id="main"]/div/div[3]/div/div/div[3]/p[2]'):
                    desc.append(des.text)

                ## Get Year of movies
                for year in driver.find_elements(By.XPATH, '//*[@id="main"]/div/div[3]/div/div/div[3]/h3/span[2]'):
                    years.append(year.text)

                ## Check if fetch all movie
                count = len(title)

                try:
                    ## check if previous exist
                    if page > 0:
                        ## Find Next button and click
                        element = driver.find_element(By.XPATH, '//*[@id="main"]/div/div[4]/a[2]')
                        driver.execute_script("arguments[0].click();", element)
                        page += 1
                    ## check if only next exist
                    else:
                        element = driver.find_element(By.XPATH, '//*[@id="main"]/div/div[4]/a')
                        driver.execute_script("arguments[0].click();", element)
                        page += 1
                except:
                    break
    df = pd.DataFrame(data={'Title':title,'Genre':genres,'Vote':votes,
                   'Stars':stars,'Runtime':duration,'Description':desc,'Releaseyear':years,'Ratings':rating})
    return df
    ## End Looping

def transform_data(df):
    ##  Create Date
    today = date.today().strftime("%d/%m/%Y")

    ## Add Modified Date Column
    df['InsertedDate'] = today

    ## Cut ',' in Vote then change type to int
    df['Vote'] = df['Vote'].apply(lambda x: ''.join(x.split(','))).astype('int64')

    ## Convert type of Ratings from Object to float
    df['Ratings'] = df['Ratings'].astype('float64')

    ## Fill None value with '0 min' value
    df = df.fillna(value={'Runtime': '0 min'})

    ## Drop duplicates name because one movie can categorize in multiple genre
    df = df.drop_duplicates(subset=['Title'])

    ## Convert Pandas to numpy for faster loop with 0.03 sec if pandas use 1.1 sec
    np_arr = np.array(df)
    lst = []
    for i in range(len(np_arr)):
        lst.append({'title': np_arr[i][0], 'genre': np_arr[i][1], 'vote': np_arr[i][2] ,
                       'stars': np_arr[i][3], 'runtime': np_arr[i][4], 'description': np_arr[i][5], 'releaseyear': np_arr[i][6], 
                        'Ratings': np_arr[i][7], 'InsertedDate': np_arr[i][8]})
    return lst

def loadData(lst):
    ## Load Section
    ## Connect to mongo
    client = MongoClient('localhost', 8081)
    ## Select DB = test1
    db = client['test1']
    ## Connect to collection to keep all movie information
    movies_collection = db['movies']
    movies_collection.insert_many(lst)

if __name__ == '__main__':
    try:
        df = extractWeb()
        transform_lst = transform_data(df)
        if len(transform_lst) != 0:
            print('Loading data....')
            loadData(transform_lst)
        else:
            print('No Data loaded')
        print('Data Loading Successfully')
    except Exception as e:
        print(e)