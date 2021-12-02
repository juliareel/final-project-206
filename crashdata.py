
import json
import unittest
import os
import requests
import sqlite3

def create_request_url(year, county_code):
    base_url = "https://crashviewer.nhtsa.dot.gov/CrashAPI/crashes/GetCrashesByLocation?fromCaseYear={}&toCaseYear={}&state=17&county={}&format=json"
    return base_url.format(year, year, county_code)

def get_crash_data(county_codes, year):
    # info_per_county_code = {}
    # for county in county_codes:
    #     county_info = {}
    #     info_per_county_code[county[0]] = county_info
    #     try:
    #         request_url = create_request_url(year, str(county[0]))
    #         r = requests.get(request_url)
    #         data = r.text
    #         data_list = json.loads(data)
    #     except:
    #         print("Exception")
    #         return None

    #     num_fatal_crashes = data_list['Count']
    #     total_fatalities = 0

    #     for crash in data_list['Results'][0]:
    #         total_fatalities += int(crash['FATALS'])

    #     county_info['num_fatal_crashes'] = num_fatal_crashes
    #     county_info['num_fatalities'] = total_fatalities
    #     county_info['year'] = int(year)
    #     county_info['id'] = county[1]
    
    # return info_per_county_code
    info_per_county_code = []
    for county in county_codes:
        try:
            request_url = create_request_url(year, str(county[0]))
            r = requests.get(request_url)
            data = r.text
            data_list = json.loads(data)
        except:
            print("Exception")
            return None

        num_fatal_crashes = data_list['Count']
        total_fatalities = 0
        for crash in data_list["Results"][0]:
            total_fatalities += int(crash["FATALS"])

        #id, county_id, year, num_fatal_crashes, num_fatalities

        info_per_county_code.append((county[1], year, num_fatal_crashes, total_fatalities))

    return info_per_county_code


def get_county_codes(curr, conn):
    curr.execute("SELECT county_code, id FROM Counties")
    county_codes_and_ids = curr.fetchall()
    return(county_codes_and_ids)

def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    curr = conn.cursor()
    return curr, conn

def setUpCrashTable(data, curr, conn):
    curr.execute("CREATE TABLE IF NOT EXISTS Crashes (county_id INTEGER PRIMARY KEY, year INTEGER, num_fatal_crashes INTEGER, num_fatalities INTEGER)")
    
    start = None
    curr.execute('SELECT county_id FROM Crashes WHERE county_id = (SELECT MAX(county_id) FROM Crashes)')
    start = curr.fetchone()
    if start != None:
        start = start[0] + 1
    else:
        start = 0 

    count = start
    for county in data[start:start+24]:
        county_id = county[0]
        year = county[1]
        num_fatal_crashes = county[2]
        num_fatalities = county[3]
        count += 1

        curr.execute("INSERT INTO Crashes (county_id, year, num_fatal_crashes, num_fatalities) VALUES(?, ?, ?, ?)", (county_id, year, num_fatal_crashes, num_fatalities))


    conn.commit()

def main():
    curr, conn = setUpDatabase('Weather_Crash_Data_Illinois.db')
    county_codes = get_county_codes(curr, conn)
    data = get_crash_data(county_codes, '2019')

    for i in range(5):
        setUpCrashTable(data, curr, conn)
   



if __name__ == "__main__":
    main()