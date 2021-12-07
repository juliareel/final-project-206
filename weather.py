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
    snow_county_dict = {}
    for county in list_counties:
        total_snow = 0
        list_data = create_request_url(county, date_list)
        for item in list_data:
            total_snow += snow_data(item)
        snow_county_dict[county] = total_snow
    return snow_county_dict

def temp_per_county(list_counties):
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
    county_list = []
    cur.execute('SELECT county FROM Counties')
    conn.commit()
    list_counties = cur.fetchall()
    for item in list_counties:
        county_list.append(item[0])

    return county_list

def write_snow_cache(CACHE_FNAME, list_counties):
    fw = open(CACHE_FNAME, "w")

    dicto = json.dumps(snow_per_county(list_counties))

    fw.write(dicto)

    fw.close()

def write_temp_cache(CACHE_FNAME, list_counties):
    fw = open(CACHE_FNAME, "w")

    dicto = json.dumps(temp_per_county(list_counties))

    fw.write(dicto)

    fw.close()

def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn



# def setUpSnowTable(snow_dict, cur, conn):
#     value_list = snow_dict.items()
#     cur.execute("CREATE TABLE IF NOT EXISTS Total_Snowfall (county TEXT, snow_inches INTEGER)")
#     for item in value_list:
#         cur.execute("INSERT INTO Total_Snowfall (county,snow_inches) VALUES (?,?)",(item[0],item[1]))
#     conn.commit()

# def setUpTempTable(temp_dict, cur, conn):
#     value_list = temp_dict.items()
#     cur.execute("CREATE TABLE IF NOT EXISTS Avg_Temp (county TEXT, temp_f INTEGER)")
#     for item in value_list:
#         cur.execute("INSERT INTO Avg_Temp (county,temp_f) VALUES (?,?)",(item[0],item[1]))
#     conn.commit()

# #run four times to get 4 sets of 25 rows at a time
def main():
    
    cur, conn = setUpDatabase('Weather_Crash_Data_Illinois.db')
    listy = create_county_list(cur, conn)
    #first_set = listy[0:25]
    #second_set = listy[25:50]
    third_set = listy[50:75]
    # fourth_set = listy[75:100]
    # fifth_set = listy[100:]



    # write_snow_cache('Snow_Data.json',first_set)
    # write_temp_cache('Temp_Data.json', first_set)
    #write_snow_cache('Snow_Data_pt2.json', second_set)
    #write_temp_cache('Temp_Data_pt2.json', second_set)
    write_snow_cache('Snow_Data_pt3.json', third_set)
    write_temp_cache('Temp_Data_pt3.json', third_set)
    # write_snow_cache('Snow_Data_pt4.json', fourth_set)
    # write_temp_cache('Temp_Data_pt4.json', fourth_set)
    # write_snow_cache('Snow_Data_pt5.json', fifth_set)
    # write_temp_cache('Temp_Data_pt5.json', fifth_set)



    
    

main()
