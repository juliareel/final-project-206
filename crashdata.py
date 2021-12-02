
import json
import unittest
import os
import requests
import sqlite3

def create_request_url(year, county_code):
    base_url = "https://crashviewer.nhtsa.dot.gov/CrashAPI/crashes/GetCrashesByLocation?fromCaseYear={}&toCaseYear={}&state=26&county={}&format=json"
    return base_url.format(year, year, county_code)

def get_data(county_code, year):
    request_url = create_request_url(year, county_code)
    crashes_per_year = {}
    try:
        for i in range(2010, 2016):
            request_url = create_request_url(str(i), '003')
            r = requests.get(request_url)
            data = r.text
            data_list = json.loads(data)
            crashes_per_year[i] = data_list['Count']
    except:
        print("Exception")
        return None

    print(crashes_per_year)

def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def setUpCrashTable(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS Crashes (id INTEGER PRIMARY KEY, county_id INTEGER, year INTEGER, num_fatal_crashes INTEGER, num_fatalities INTEGER)")
    conn.commit()

def main():
    cur, conn = setUpDatabase('Weather_Crash_Data_Illinois.db')
    setUpCrashTable(cur, conn)



if __name__ == "__main__":
    main()