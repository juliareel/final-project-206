# FALL 2021
# SI 206


from bs4 import BeautifulSoup
import requests
import re
import os
import csv
import unittest
import json


def read_cache():
 
    try:
        cache_file = open("washtenaw_pop.json", 'r', encoding="utf-8") # Try to read the data from the file
        cache_contents = cache_file.read()  # If it's there, get it into a string
        CACHE_DICTION = json.loads(cache_contents) # And then load it into a dictionary
        cache_file.close() # Close the file, we're good, we got the data in a dictionary.
        return CACHE_DICTION
    except:
        CACHE_DICTION = {}
        return CACHE_DICTION






if __name__ == '__main__':
    pop_dict = add_total_population(2012, 'washtenaw', 'michigan')
    print(pop_dict)

