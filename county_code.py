# FALL 2021
# SI 206


from bs4 import BeautifulSoup
import requests
import re
import os
import json
import csv
import unittest
import sqlite3
import matplotlib
import matplotlib.pyplot as plt


def county_soup(filename):
    with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), filename), 'r') as f:
        r = f.read()
   
    soup = BeautifulSoup(r, 'html.parser')
    

    table = soup.find('table', class_="wikitable sortable") 
    illinois_counties = table.find_all('a')
    county_numbers = table.find_all('a', class_="external text")
    lst_counties = []
    lst_of_county_num = []
    for c in illinois_counties:
        if c.text[-6:] == "County":
            lst_counties.append(c.text[:-7])

    for num in county_numbers:
        lst_of_county_num.append(num.text)

    ret_list = []
    for i in range(len(lst_counties)):
        ret_list.append((lst_counties[i], lst_of_county_num[i]))



    return ret_list


def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn


def inputCountyData(data, curr, conn):
    curr.execute('CREATE TABLE IF NOT EXISTS County_ID (id INTEGER PRIMARY KEY, county TEXT, county_code INTEGER)')

    start = None
    curr.execute('SELECT id FROM County_ID WHERE id = (SELECT MAX(id) FROM County_ID)')
    start=curr.fetchone()
    if start != None:
        start = start[0] + 1
    else:
        start = 0

    count = start
    for county in data[start:start+24]:
        county_name = county[0]
        county_code = county[1]
        id_num = count
        count += 1

        curr.execute("INSERT INTO County_ID (id, county, county_code) VALUES(?, ?, ?)", (id_num, county_name, county_code))

    conn.commit()





def main():
    #run 4 times to get all 100 rows on data into database
    data = county_soup('list_of_counties_in_Illinois.html')
    curr, conn = setUpDatabase('Weather_Crash_Data_Illinois.db')

    for i in range(4):
        inputCountyData(data,curr,conn)
   


if __name__ == '__main__':
    main()


    

