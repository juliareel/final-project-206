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




def setUpDatabase(db_name):
    """
    Takes in the name of the database, a string, as the input. Returns the cursor and connection to the database.
    """
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn




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

    curr, conn = setUpDatabase('Weather_Crash_Data_Illinois.db')



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
