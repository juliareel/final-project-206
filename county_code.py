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

def create_request_url(year, county_code):
    """
    Takes in a year as a string and a county_code as an integer.
    Creates and returns the url that will be processed NHTSA Crash data API.
    """
    base_url = "https://crashviewer.nhtsa.dot.gov/CrashAPI/crashes/GetCrashesByLocation?fromCaseYear={}&toCaseYear={}&state=17&county={}&format=json"
    return base_url.format(year, year, county_code)



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


def joinPopFatal(curr, conn):
    """
    Takes in the database cursor and the database connections as inputs. Joins the two tables based off of the county id numbers and selects the county name,
    population, and number of car crash fatalities for the given year. Returns a list of tuples that is sorted by highest to lowest population a table that 
    holds tuples with (county name, population, and number of fatal car crashes).
    
    """
    curr.execute("SELECT Counties.county, Counties.population, Crashes.num_fatalities FROM Counties JOIN Crashes ON Counties.id = Crashes.county_id")
    lst = []
    for row in curr:
        lst.append(row)

    ret_lst = sorted(lst, key = lambda x: x[1], reverse=True)

    return ret_lst[0:10]


def creatDictFatal(lst):
    """
    Takes in a list of tuples that hold (county name, population, and number of fatal car crashes). Returns a dictionary where the key is the county name and
    the value is the number of fatalities in that county.
    """
    dict_return = {}
    for l in lst:
        dict_return[l[0]] = l[2]

    return dict_return

def barchart_county_and_fatalities(county_dict):
    """
    Takes in a dictionary of where the key is the county name and the value is the number of fatalities in that county. Creates a bar chart where the key is x-axis
    and value is y-axis.
    """
    y = county_dict.values()
    x = county_dict.keys()
    csfont = {'fontname':'MS Serif'}
    hfont = {'fontname':'Helvetica'}

    plt.title('Fatalities From Car Crashes in the Top 10 Populated Illinois Counties in 2019',**csfont)
    plt.xlabel('Top Ten Populated Illinois Counties', **hfont)
    plt.ylabel('Number of Car Crash Fatalties', **hfont)
    plt.bar(x,y, color = 'coral')
    plt.xticks(rotation = 90)
    plt.gcf().subplots_adjust(bottom=0.40)   
    plt.show()



def creatDictPop(lst):
    """
    Takes in a list of tuples that hold (county name, population, and number of fatal car crashes). Returns a dictionary where the key is the county name and
    the value is the population in that county.
    """ 
    dict_return = {}
    for l in lst:
        dict_return[l[0]] = l[1]

    return dict_return

def barchart_county_and_pop(county_dict):
    """
    Takes in a dictionary of where the key is the county name and the value is the population in that county. Creates a bar chart where the key is x-axis
    and value is y-axis.
    """
    y = county_dict.values()
    x = county_dict.keys()
    csfont = {'fontname':'MS Serif'}
    hfont = {'fontname':'Helvetica'}

    plt.title('Population in the Top 10 Populated Illinois Counties in 2019',**csfont)
    plt.xlabel('Top Ten Populated Illinois Counties', **hfont)
    plt.ylabel('Population (by million)', **hfont)
    plt.bar(x,y, color = 'skyblue')
    plt.xticks(rotation = 90)
    plt.gcf().subplots_adjust(bottom=0.40)   
    plt.show()

def write_percentage_fatalities_per_county(filename, curr, conn):
    """
    Takes in a filename (string), the database cursor, and the database connections as inputs. Creates a file and writes the county name and that county's
    calculated ratio of fatalities per population. Returns a dictionary wehre the key is the county name and value is that calculated ratio/percentage.
    """
    path = os.path.dirname(os.path.abspath(__file__)) + os.sep
    #Writes the results of the average_followers_per_song() function to a file.
    outFile = open(path + filename, "w")
    outFile.write("Percentage of Fatal Car Crashes per Population in Illinois Counties in 2019\n")
    outFile.write("=======================================================================\n\n")
    outFile.write("County Name:  Percentage of Fatalities per Population  " + '\n' + '\n')
    curr.execute("SELECT Counties.county, Counties.population, Crashes.num_fatalities FROM Counties JOIN Crashes ON Counties.id = Crashes.county_id")
    lst = []
    for row in curr:
        lst.append(row)

    lst_of_county_fatalities = sorted(lst, key = lambda x: x[0])

    lst_of_percentages = []

    for data in lst_of_county_fatalities:

        perc = float(data[2]) / float(data[1])
        perc = perc * 100
        lst_of_percentages.append((data[0], perc, data[1]))
        outFile.write(str(data[0]) + ":  " + str(perc) + '\n' + '\n')
    outFile.close()

    ret_lst = sorted(lst_of_percentages, key = lambda x: x[2], reverse=True)
    ret_lst = ret_lst[0:10]

    perc_dict = {}
    for data in ret_lst:
        perc_dict[data[0]] = data[1]


    return perc_dict
    
def barchart_perc(percentages):
    """
    Takes in a dictionary of where the key is the county name and the value is the ratio of fatalities by population. Creates a bar chart where the key is x-axis
    and value is y-axis.
    """
    y = percentages.values()
    x = percentages.keys()
    csfont = {'fontname':'MS Serif'}
    hfont = {'fontname':'Helvetica'}

    plt.title('Ratio of Car Crash Fatalities by Population in 2019 in Illinois' ,**csfont)
    plt.xlabel('Top Ten Populated Illinois Counties', **hfont)
    plt.ylabel('Ratio of Car Crash Fatilites by Total Population', **hfont)
    plt.bar(x,y, color = 'limegreen')
    plt.xticks(rotation = 90)
    plt.gcf().subplots_adjust(bottom=0.40)   
    plt.show()



def main():
    """
    Takes no inputs and returns nothing. Selects data from database in order to create visualaztions (three bar charts).
    """
    data = county_soup('list_of_counties_in_Illinois.html')
    data_pop = population_per_county(data)
    curr, conn = setUpDatabase('Weather_Crash_Data_Illinois.db')

    for i in range(4):
        inputCountyData(data,data_pop,curr,conn)

    county_codes = get_county_codes(curr, conn)
    data = get_crash_data(county_codes, '2019')

    for i in range(5):
        setUpCrashTable(data, curr, conn)


    lst_of_county_fatalities = joinPopFatal(curr, conn)


    dict_for_visual = creatDictFatal(lst_of_county_fatalities)
    barchart_county_and_fatalities(dict_for_visual)


    dict_for_visual_pop = creatDictPop(lst_of_county_fatalities)
    barchart_county_and_pop(dict_for_visual_pop)


    #calculates percentage of fatalities per county and puts them into a text file
    dict_percent = write_percentage_fatalities_per_county("ratio_of_fatalities.txt", curr, conn)
    barchart_perc(dict_percent)





if __name__ == '__main__':
    main()


    

