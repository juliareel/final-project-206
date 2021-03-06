# FALL 2021
# SI 206

from bs4 import BeautifulSoup
import requests
import re
import os
import json
import csv
import sqlite3
import plotly.graph_objects as go

def setUpDatabase(db_name):
    """
    Takes in the name of the database, a string, as the input. Returns the cursor and connection to the database.
    """
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn


def setUpSnowTable(file_name, curr, conn):
    """
    Takes in the filename of the json file loaded from the API, the database cursor, and the database connections as inputs. Creates a table called
    Total_Snowfall and inserts the county_id and total snowfall for that county. Returns nothing. 
    """


    snow_data = open(file_name, 'r')
    snow_data_dict = json.loads(snow_data.read())
    value_list = snow_data_dict.items()
    curr.execute("CREATE TABLE IF NOT EXISTS Total_Snowfall (county_id INTEGER, snow_inches INTEGER)")
    for item in value_list:
        curr.execute("SELECT id FROM Counties WHERE county = ?", (item[0],))
        county_id = curr.fetchone()[0]
        curr.execute("INSERT INTO Total_Snowfall (county_id, snow_inches) VALUES (?,?)", (county_id, item[1]))
        conn.commit()
    snow_data.close()

def setUpTempTable(file_name, cur, conn):
    """
    Takes in the filename of the json file loaded from the API, the database cursor, and the database connections as inputs. Creates a table called
    Avg_Temp and inserts the county_id and average temperature of that county. Returns nothing. 
    """

    temp_data = open(file_name, 'r')
    temp_data_dict = json.loads(temp_data.read())
    value_list = temp_data_dict.items()
    cur.execute("CREATE TABLE IF NOT EXISTS Avg_Temp (county_id INTEGER, temp_f INTEGER)")
    for item in value_list:
        cur.execute("SELECT id FROM Counties WHERE county = ?", (item[0],))
        county_id = cur.fetchone()[0]
        cur.execute("INSERT INTO Avg_Temp (county_id, temp_f) VALUES (?,?)", (county_id, item[1]))
    conn.commit()
    temp_data.close()


def summary_for_scatterplot(cur, conn):
    """
    Takes in the database cursor and the database connections as inputs. Joins four tables based off of the county id numbers and selects the county name,
    number of fatal car crashes, total snowfall, and average temperature. Returns a list of tuples of these selected values. 
    """

    cur.execute(""" SELECT DISTINCT Counties.county, Crashes.num_fatal_crashes, Total_Snowfall.snow_inches, Avg_Temp.temp_f
                    FROM Counties INNER JOIN Crashes ON Counties.id = Crashes.county_id INNER JOIN Total_Snowfall ON 
                    Counties.id = Total_Snowfall.county_id INNER JOIN Avg_Temp on Counties.id = Avg_Temp.county_id""")
    results = cur.fetchall()
    return results


def visualization(lst_tups):
    """
    Takes in a list of tuples with corresponding snowfall inches, fatal car crashes, average temperature, and county name
    for each Illinois county in alphabetical order as inputs and returns nothing. Creates a scatterplot where the snowfall
    is x-axis and fatalities is y-axis. Also creates a bar chart where the temperature is x-axis and fatalities is y-axis
    """
    snowfall = []
    crashes = []
    temp = []
    name = []

    for t in lst_tups:     
        snowfall.append(t[2])
        crashes.append(t[1])
        temp.append(t[3])
        name.append(t[0])





    title_str = "Relationship between Total Snowfall and Car Crash Fatalities in Illinois in 2019"
    title_str2 = "Relationship between Average Temperature and Car Crash Fatalities in Illinois in 2019"

     #barchart

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=snowfall,
        y=crashes,
        hoverinfo = "text",
        hovertext=name,
        marker=dict(color="rgb(64, 60, 94)", size=10),
        mode="markers",
        name="Snowfall and Car Crash Fatalities",
    ))

    fig.update_layout(title = title_str,
        xaxis_title="Total Snowfall (inches)", yaxis_title="Fatal Car Crashes")
    
    fig.show()    



    #scatterplot

    fig2 = go.Figure([go.Bar(x=temp, y=crashes)])
    fig2.update_traces(marker_color="rgb(123, 164, 224)", marker_line_color="rgb(12, 62, 133)", marker_line_width=2, width=.05, hoverinfo = "text",
    hovertext=name)
    fig2.update_layout(title_text = title_str2, xaxis_title="Average Temperature (F)", yaxis_title="Fatal Car Crashes")
    fig2.show()


def write_calculations(filename, curr, conn):
    """
    Takes in a filename (string), the database cursor, and the database connections as inputs. Creates a file, selects from database,
    and writes the total population, total amount of fatal car crashes, total snowfall (in), and average temperature in Illinois in 
    2019 to the file. Returns nothing. 
    """
    path = os.path.dirname(os.path.abspath(__file__)) + os.sep
    #Writes the results of the average_followers_per_song() function to a file.
    outFile = open(path + filename, "w")
    outFile.write("Total population, total amount of fatal car crashes, total snowfall (in), and average temperature in Illinois in 2019\n")
    outFile.write("=======================================================================\n\n")


    curr.execute("SELECT population FROM Counties")
    population = curr.fetchall()

    total_pop = 0
    for p in population:
        total_pop = total_pop + p[0]

    outFile.write("The total population in Illinois in 2019: " + str(total_pop) + '\n' + '\n')


    curr.execute("SELECT num_fatal_crashes FROM Crashes")
    crashes = curr.fetchall()

    total_crashes = 0
    for c in crashes:
        total_crashes = total_crashes + c[0]

    outFile.write("The total number of fatal crashes in Illinois in 2019: " + str(total_crashes) + '\n' + '\n')


    curr.execute("SELECT snow_inches FROM Total_Snowfall")
    snow = curr.fetchall()

    total_snowfall = 0
    for s in snow:
        total_snowfall = total_snowfall + s[0]

    outFile.write("The total amount of snowfall (inches) in Illinois in 2019: " + str(total_snowfall) + '\n' + '\n')


    curr.execute("SELECT temp_f FROM Avg_Temp")
    temps = curr.fetchall()

    total_temp = 0
    for t in temps:
        total_temp = total_temp + t[0]

    outFile.write("The average temperature (Fahrenheit) in Illinois in 2019: " + str(total_temp/len(temps)) + '\n' + '\n')

def main():
    """
    Takes no inputs and returns nothing. Creates tables and selects data from database in order to create visualaztions (2 graphs).
    """


    curr, conn = setUpDatabase('Weather_Crash_Data_Illinois.db')
    setUpSnowTable("Snow_Data.json", curr, conn)
    setUpTempTable("Temp_Data.json", curr, conn)


    str_snow = "Snow_Data_pt"

    i = 2
    for count in range(4):
        count = 2 + count
        str_final = str_snow + str(count)+ ".json"
        setUpSnowTable(str_final, curr, conn)

    str_temp = "Temp_Data_pt"
    j = 2
    for cont in range(4):
        cont = j + cont
        str_fin = str_temp + str(cont) + ".json"
        setUpTempTable(str_fin, curr, conn)
    
   

    write_calculations("total-amounts.txt", curr, conn)


    lst_tups = summary_for_scatterplot(curr, conn)
    visualization(lst_tups)   


if __name__ == '__main__':
    main()