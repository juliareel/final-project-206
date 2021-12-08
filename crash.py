# FALL 2021
# SI 206


from bs4 import BeautifulSoup
import requests
import re
import os
import json
import csv
import sqlite3
import matplotlib
import matplotlib.pyplot as plt
import plotly.graph_objects as go

def get_county_codes(curr, conn):
    """
    Takes in the database cursor and connection as inputs.
    Collects all of the county codes and county_ids from the Counties table in the database.
    Returns a list of tuples in the format (county_code, id)
    E.g. [(1,0), (3,1)...]
    """
    curr.execute("SELECT county_code, id FROM Counties")
    county_codes_and_ids = curr.fetchall()
    return(county_codes_and_ids)

def create_request_url(year, county_code):
    """
    Takes in a year as a string and a county_code as an integer.
    Creates and returns the url that will be processed NHTSA Crash data API.
    """
    base_url = "https://crashviewer.nhtsa.dot.gov/CrashAPI/crashes/GetCrashesByLocation?fromCaseYear={}&toCaseYear={}&state=17&county={}&format=json"
    return base_url.format(year, year, county_code)

    
def get_crash_data(county_codes, year):
    """
    Takes in the county_code/id list of tuples that was returned by get_county_codes().
    Takes in a year as an int. 
    Loops through the counties in the list of tuples and creates a request url.
    Processes the JSON data into a dictionary.
    Creates and returns a list of tuples each containing the number of fatal crashes, county_id, year, 
    and number of total fatalities for each county.
    """
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


def setUpDatabase(db_name):
    """
    Takes in the name of the database, a string, as the input. Returns the cursor and connection to the database.
    """
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn




def setUpCrashTable(data, curr, conn):
    """
    Takes in the list of tuples returned in get_crash_data() as input, along with the database cursor and connection. Inputs the data 
    into the table 25 rows at a time. Function does not return anything.
    """
    curr.execute("CREATE TABLE IF NOT EXISTS Crashes (county_id INTEGER PRIMARY KEY, num_fatal_crashes INTEGER, num_fatalities INTEGER)")
    
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
        num_fatal_crashes = county[2]
        num_fatalities = county[3]
        count += 1

        curr.execute("INSERT INTO Crashes (county_id, num_fatal_crashes, num_fatalities) VALUES(?, ?, ?)", (county_id, num_fatal_crashes, num_fatalities))


    conn.commit()




def main():
    """
    Takes no inputs and returns nothing. Selects data from database in order to create visualaztions (three bar charts).
    """
    curr, conn = setUpDatabase('Weather_Crash_Data_Illinois.db')
    county_codes = get_county_codes(curr, conn)
    data = get_crash_data(county_codes, '2019')
    setUpCrashTable(data, curr, conn)







if __name__ == '__main__':
    main()
