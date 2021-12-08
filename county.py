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



def county_soup(filename):
    """
    Takes in a filename (string) as an input. Opens the file (downloaded HTML file from Wikipedia) and creates a BeautifulSoup object after 
    retrieving content from the passed in file (Wikipedia page). Parses through the BeautifulSoup object and captures the county name and 
    county ID number. Adds these to a list of tuples and returns the list of tuples looking like (county name, county ID #). 
    """

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


def population_per_county(data):
    """ 
    Takes in no inputs. Creates a BeautifulSoup object after retrieving content from url. Parses through the BeautifulSoup object and captures
    the population for each county (listed in alphabetical order). Returns a list of population numbers that is organized by couny name alphabetically. 
    """
    url = "https://www.indexmundi.com/facts/united-states/quick-facts/illinois/population#table"
    resp = requests.get(url)

    soup = BeautifulSoup(resp.content, 'html.parser')
    pop = soup.find(id = "tableTab")
    pop_table = pop.find('tbody')
    pop_row = pop_table.find_all('tr')
    lst_populations = []

    for pop_county in pop_row:
        spaces_removed = pop_county.text
        spaces_removed = spaces_removed.strip("\n")
        count = 0
        for c in spaces_removed:
            if c.isnumeric():
                break
            else:
                count += 1
        pop_integer = spaces_removed.replace(",", "")
        lst_populations.append(pop_integer[count:])


    return lst_populations





def setUpDatabase(db_name):
    """
    Takes in the name of the database, a string, as the input. Returns the cursor and connection to the database.
    """
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn




def inputCountyData(data, pop_data, curr, conn):
    """
    Takes in a the list of tuples with county name and county id, a list of populattion per county, the database cursor and the database 
    connections as inputs. Creates a table that will hold an id number, county name, county code, and that county's population. Returns nothing.
    """
    curr.execute('CREATE TABLE IF NOT EXISTS Counties (id INTEGER PRIMARY KEY, county TEXT, county_code INTEGER, population INTEGER)')

    start = None
    curr.execute('SELECT id FROM Counties WHERE id = (SELECT MAX(id) FROM Counties)')
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
        population = pop_data[count]
        count += 1

        curr.execute("INSERT INTO Counties (id, county, county_code, population) VALUES(?, ?, ?, ?)", (id_num, county_name, county_code, population))

    conn.commit()



def main():
    """
    Takes no inputs and returns nothing. Selects data from database in order to create visualaztions (three bar charts).
    """
    data = county_soup('list_of_counties_in_Illinois.html')
    data_pop = population_per_county(data)
    curr, conn = setUpDatabase('Weather_Crash_Data_Illinois.db')

    inputCountyData(data,data_pop,curr,conn)







if __name__ == '__main__':
    main()


    

