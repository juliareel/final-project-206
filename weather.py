import json
import unittest
import os
import re
import requests

def create_request_url(latlong, date1, date2):
    base_url = 'https://api.weatherstack.com/historical?access_key=c1e596fa44c50dbf9597860a4e84a937'
    params = '&query='+ latlong + '&historical_date_start=' + date1 + '&historical_date_end=' + date2
    url = base_url + params

    r = requests.get(url)
    data = json.loads(r.text)
    return data


def snow_data(data):
    snow_dict = {}
    target = data['historical']
    i = 0
    for item in target.keys():
        snow_dict[item[i]]= target[item][i]['totalsnow']
        i += 1
    return snow_dict

def temp_data(data):
    avg_temp_dict= {}
    target = data['historical']
    i = 0
    for item in target.keys():
        avg_temp_dict[item[i]]= target[item][i]['avgtemp']
        i += 1
    return avg_temp_dict



create_request_url('42.279594,-83.732124', '2010-01-21', '2010-02-21')





