import json
import unittest
import os
import re
import requests
import datetime

date_list = []
start_date = datetime.date(2010, 1, 1)
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
        list_data.append(data)
    return list_data


def snow_data(data):
    snow_dict = {}
    total_snow = 0
    target = data['historical']
    list_dates = target.keys()
    for item in list_dates:
        snow_dict[item]= target[item]['totalsnow']
    for val in snow_dict.items():
        total_snow += int(val[1])
    return total_snow
    

def temp_data(data):
    avg_temp_dict= {}
    total_temp = 0
    target = data['historical']
    list_dates = target.keys()
    for item in list_dates:
        avg_temp_dict[item]= target[item]['avgtemp']
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

listy = ['Boone']
print(temp_per_county(listy))

