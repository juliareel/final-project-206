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


def population_per_county(data):

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
    base_url = "https://crashviewer.nhtsa.dot.gov/CrashAPI/crashes/GetCrashesByLocation?fromCaseYear={}&toCaseYear={}&state=17&county={}&format=json"
    return base_url.format(year, year, county_code)

def get_crash_data(county_codes, year):
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
    cur = conn.cursor()
    return cur, conn


def inputCountyData(data, pop_data, curr, conn):
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
    curr.execute("SELECT Counties.county, Counties.population, Crashes.num_fatalities FROM Counties JOIN Crashes ON Counties.id = Crashes.county_id")
    lst = []
    for row in curr:
        lst.append(row)

    ret_lst = sorted(lst, key = lambda x: x[1], reverse=True)

    return ret_lst[0:10]


def creatDictFatal(lst):
    dict_return = {}
    for l in lst:
        dict_return[l[0]] = l[2]

    return dict_return

def barchart_county_and_fatalities(county_dict):
    
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
    dict_return = {}
    for l in lst:
        dict_return[l[0]] = l[1]

    return dict_return

def barchart_county_and_pop(county_dict):
    
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

    path = os.path.dirname(os.path.abspath(__file__)) + os.sep
    #Writes the results of the average_followers_per_song() function to a file.
    outFile = open(path + filename, "w")
    outFile.write("Percentage of Fatal Car Crashes per Population in Illinois Counties in 2019\n")
    outFile.write("=======================================================================\n\n")
    outFile.write("County Name:  Percentage of Fatalities per Population:  " + '\n' + '\n')
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
    #run 4 times to get all 100 rows on data into database
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

    #calculates percentage of fatalities per county and puts them into a text file
    dict_percent = write_percentage_fatalities_per_county("ratio_of_fatalities.txt", curr, conn)
    barchart_perc(dict_percent)


    dict_for_visual = creatDictFatal(lst_of_county_fatalities)
    barchart_county_and_fatalities(dict_for_visual)


    dict_for_visual_pop = creatDictPop(lst_of_county_fatalities)
    barchart_county_and_pop(dict_for_visual_pop)




if __name__ == '__main__':
    main()


    

