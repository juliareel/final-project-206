import json
import unittest
import os
import re
import requests
import datetime
import sqlite3

date_list = []
start_date = datetime.date(2019, 1, 1)
number_of_days = 365

for day in range(number_of_days):
    a_date = (start_date + datetime.timedelta(days = day)).isoformat()
    date_list.append(a_date)


def create_request_url(county, list_dates):
    """
    Takes in a list of dates and a county name (string) as an input. Generates a url to search within the weather api using a base url in tandem with 
    parameters which are formed using the input values. It then indexes the json data returned by the search, and appends the 'historical' values of each 
    dictionary to a list, 'list_data'. It then returns this list of data, which is a list of dictionaries where the keys are the dates for every day in the year
    2019 and the values contain the information we need for our analysis.
    
    """
    list_data = []
    for date in list_dates:
        base_url = 'https://api.weatherstack.com/historical?access_key=c1e596fa44c50dbf9597860a4e84a937'
        params = '&query='+ county +",Illinois" + '&historical_date=' + date + '&units=f'
        url = base_url + params
        r = requests.get(url)
        data = json.loads(r.text)
        try:
            list_data.append(data['historical'])
        except:
            continue
    return list_data


def snow_data(data):
    """
    Takes in a a dictionary and iterates through the keys which in our case are dates of each day in a year, and adds the total snowfall per day
    to a variable by indexing into the dictionary and accessing the total snowfall per day. It then returns that number, which in our case is the 
    total snow fall in a year for a specific county.

    """
    snow_dict = {}
    total_snow = 0
    list_dates = data.keys()
    for item in list_dates:
        try:
            snow_dict[item]= data[item]['totalsnow']
        except:
            continue
    for val in snow_dict.items():
        total_snow += int(val[1])
    return total_snow
    

def temp_data(data):
    """
    Takes in a dictionary and iterates through the keys which in our case are dates of each day in a year, and adds the avg temperature per day
    to a variable by indexing into the dictionary and accessing the total snowfall per day. It then divides that number by the amount of items in
    dictionary, which in our case is 365 (because there were 365 days int he year 2019) returns that number, which in our case is the 
    average temparature for the entire year of 2019 for a specific county.

    """
    avg_temp_dict= {}
    total_temp = 0
    list_dates = data.keys()
    for item in list_dates:
        try:
            avg_temp_dict[item]= data[item]['avgtemp']
        except:
            continue
    for val in avg_temp_dict.items():
        total_temp += int(val[1])
    total_avg_temp = total_temp/len(avg_temp_dict.items())
    return total_avg_temp



def snow_per_county(list_counties):
    """
    Takes in a list of counties (a list of strings) as input and, using create_request_url, tailors the api to search for data on each county by iterating 
    through the list. It then uses snow_data to get the total snowfall for each county. It saves each county name and total snowfall for the year 
    2019 in inches to a dictionary where the keys are county names and the values are the total snowfall in inches for the given county.
    
    """
    snow_county_dict = {}
    for county in list_counties:
        total_snow = 0
        list_data = create_request_url(county, date_list)
        for item in list_data:
            total_snow += snow_data(item)
        snow_county_dict[county] = total_snow
    return snow_county_dict

def temp_per_county(list_counties):
    """
    Takes in a list of counties (a list of strings) as input and, using create_request_url, tailors the api to search for data on each county by iterating 
    through the list. It then uses temp_data to get the avg yearly temp for each county. It saves each county name and total snowfall for the year 
    2019 in inches to a dictionary where the keys are county names and the values are the total snowfall in inches for the given county.
    
    """
    temp_county_dict = {}
    for county in list_counties:
        total_temp = 0
        list_data = create_request_url(county, date_list)
        for item in list_data:
            total_temp += temp_data(item)
            avg_temp = total_temp / len(list_data)
        temp_county_dict[county] = avg_temp
    return temp_county_dict



def create_county_list(cur, conn):
    """
    This function generates a list of counties by pulling each county name from the "Counties" table in the database using a select statement. It 
    then appends each name to a list by indexing the tuples returned with the fetchall statement. 
    
    """
    county_list = []
    cur.execute('SELECT county FROM Counties')
    conn.commit()
    list_counties = cur.fetchall()
    for item in list_counties:
        county_list.append(item[0])

    return county_list

def write_snow_cache(CACHE_FNAME, list_counties):
    """
    This function takes a string and a list as inputs. The string is what you want to name the file you are going to write the data to. It then
    writes the dictionary generated from snow_per_county to a json file with the name of your choice.
    
    """
    fw = open(CACHE_FNAME, "w")

    dicto = json.dumps(snow_per_county(list_counties))

    fw.write(dicto)

    fw.close()

def write_temp_cache(CACHE_FNAME, list_counties):
    """
    This function takes a string and a list as inputs. The string is what you want to name the file you are going to write the data to. It then
    writes the dictionary generated from plugging the list of counties into the function temp_per_county to a json file with the file name of your
    choice.
    
    """
    fw = open(CACHE_FNAME, "w")

    dicto = json.dumps(temp_per_county(list_counties))

    fw.write(dicto)

    fw.close()

def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn


def main():
    """
    This function allows us to pull data from the api 25 requests at time.
    
    """
   
    cur, conn = setUpDatabase('Weather_Crash_Data_Illinois.db')
    listy = create_county_list(cur, conn)
    first_set = listy[0:25]
    second_set = listy[25:50]
    third_set = listy[50:75]
    fourth_set = listy[75:100]
    fifth_set = listy[100:]



    write_snow_cache('Snow_Data.json',first_set)
    write_temp_cache('Temp_Data.json', first_set)
    write_snow_cache('Snow_Data_pt2.json', second_set)
    write_temp_cache('Temp_Data_pt2.json', second_set)
    write_snow_cache('Snow_Data_pt3.json', third_set)
    write_temp_cache('Temp_Data_pt3.json', third_set)
    write_snow_cache('Snow_Data_pt4.json', fourth_set)
    write_temp_cache('Temp_Data_pt4.json', fourth_set)
    write_snow_cache('Snow_Data_pt5.json', fifth_set)
    write_temp_cache('Temp_Data_pt5.json', fifth_set)



    
    

main()
