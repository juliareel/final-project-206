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


def create_request_url(county, date):
    base_url = 'https://api.weatherstack.com/historical?access_key=c1e596fa44c50dbf9597860a4e84a937'
    params = '&query='+ county +",Illinois" + '&historical_date=' + date + '&units=f'
    url = base_url + params
    print(url)
    r = requests.get(url)
    data = json.loads(r.text)
    return data

list_data = []
for day in date_list:
    list_data.append(create_request_url('Boone', day))



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

yearly_snow = 0
yearly_temp = 0
for item in list_data:
    yearly_snow += snow_data(item)
    yearly_temp += temp_data(item)
avg_yearly_temp = int(yearly_temp)/365
print(yearly_snow)
print(avg_yearly_temp)




