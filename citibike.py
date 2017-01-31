# -*- coding: utf-8 -*-
"""
Created on Fri Jan 27 13:23:10 2017

@author: amybrown
"""

import requests # this package will allow us to download data from any online resource
import json


# to access the url:
r = requests.get('https://feeds.citibikenyc.com/stations/stations.json')

# to get a basic view of the text type
r.text # returns a wall of text! not very good

# json function will make the text easier to read
r.json() #now, data is formatted into a javascript object notation(json). 

# will access json files similar to dictionary
r.json().keys()
# the keys are executionTime and stationBeanList

r.json()['executionTime'] # time file was created
r.json()['stationBeanList'] # list of stations

# obtain number of stations
len(r.json()['stationBeanList'])
# there are 665 stations 

# test that you have all the fields--not sure what this means
key_list = [] # unique list of keys for each station listing
for station in r.json()['stationBeanList']:
    for k in station.keys():
        if k not in key_list:
            key_list.append(k)
# provides the list values for each station
            
# remember that values for each station are a list and you can reference the elements like any other list using the index
r.json()['stationBeanList'][0] # will give data for each of the keys for station 1 (in zero position)

# getting data into a dataframe
from pandas.io.json import json_normalize
df = json_normalize(r.json()['stationBeanList'])

# checking the range of values
import matplotlib.pyplot as plt
import pandas as pd

df['availableBikes'].hist()
plt.show()

df['totalDocks'].hist()
plt.show()

df['availableDocks'].hist()
plt.show()

df['testStation'].hist()
plt.show()

##### CHALLENGE:
##### 1. Are there any test stations?
##### No, none of the 665 stations are test stations

import collections

freq_ts = collections.Counter(df['testStation']) # there are no test stations

##### 2. How many stations are in service?
##### There are a total of 655 stations, of which 642 are in service and 23 are not in service. 

freq_inserv = print(collections.Counter(df['statusValue']))

##### 3. What is the mean number of bikes in a station? What is the median?
##### The mean number of bikes is 9.5 and the median is 5. When only those stations that are
##### in service are considered, the mean number of bikes i 9.8 and the median is 6.0.
mean_bikes1 = print(round(df['availableBikes'].mean(), 1))
med_bikes1 = print(df['availableBikes'].median())

df_serv = df[df['statusValue']  == 'In Service']
df_serv['statusValue'].value_counts()

mean_bikes2 = print(round(df_serv['availableBikes'].mean(), 1))
med_bikes2 = print(df_serv['availableBikes'].median())

u'id',
u'totalDocks',
u'city',
u'altitude',
u'stAddress2',
u'longitude',
u'postalCode',
u'testStation',
u'stAddress1',
u'stationName',
u'latitude',
u'location'

u'availableDocks',
u'statusKey',
u'statusValue',
u'lastCommunicationTime',
u'landMark'

import sqlite3 as lite

con = lite.connect('citi_bike.db')
cur = con.cursor()

#with con:
#    cur.execute('delete from available_bikes')

with con:
    cur.execute('CREATE TABLE citibike_reference (id INT PRIMARY KEY, totalDocks INT, city TEXT, altitude INT, stAddress2 TEXT, longitude NUMERIC, postalCode TEXT, testStation TEXT, stAddress1 TEXT, stationName TEXT, landMark TEXT, latitude NUMERIC, location TEXT )')
    
#a prepared SQL statement we're going to execute over and over again
sql = "INSERT INTO citibike_reference (id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"

#for loop to populate values in the database
with con:
    for station in r.json()['stationBeanList']:
        #id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location)
        cur.execute(sql,(station['id'],station['totalDocks'],station['city'],station['altitude'],station['stAddress2'],station['longitude'],station['postalCode'],station['testStation'],station['stAddress1'],station['stationName'],station['landMark'],station['latitude'],station['location']))
        
#extract the column from the DataFrame and put them into a list
station_ids = df['id'].tolist() 

#add the '_' to the station name and also add the data type for SQLite
station_ids = ['_' + str(x) + ' INT' for x in station_ids]

#create the table
#in this case, we're concatenating the string and joining all the station ids 
#(now with '_' and 'INT' added)
with con:
    cur.execute("CREATE TABLE available_bikes ( execution_time INT, " +  ", ".join(station_ids) + ");")


# a package for parsing a string into a Python datetime object
from dateutil.parser import parse 

import collections


#take the string and parse it into a Python datetime object
exec_time = parse(r.json()['executionTime'])

with con:
    cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', (exec_time.strftime('%s'),))
    
id_bikes = collections.defaultdict(int) #defaultdict to store available bikes by station

#loop through the stations in the station list
for station in r.json()['stationBeanList']:
    id_bikes[station['id']] = station['availableBikes']

#iterate through the defaultdict to update the values in the database
with con:
    for k, v in id_bikes.items():
        cur.execute("UPDATE available_bikes SET _" + str(k) + " = " + str(v) + " WHERE execution_time = " + exec_time.strftime('%s') + ";")
        
##### Challenge
##### Create a script based on the code written so far that downloads the data, parses the result, and then uploads the data to the databse. 


##### The code needs to sleep for a minute and then perform the same task. Find a way to make this happen in your code. 


##### The code only needs to run for an hour. If it's sleeping every minute, the code only needs to loop 60 times. Find a way of doing this. 

import time
from dateutil.parser import parse
import collections
import sqlite3 as lite
import requests

con = lite.connect('citi_bike.db')
cur = con.cursor()

for i in range(60):
    r = requests.get('http://www.citibikenyc.com/stations/json')
    exec_time = parse(r.json()['executionTime']).strftime("%s")

    cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', (exec_time,))

    for station in r.json()['stationBeanList']:
        cur.execute("UPDATE available_bikes SET _%d = %d WHERE execution_time = %s" % (station['id'], station['availableBikes'], exec_time))
    con.commit()

    time.sleep(60)

con.close() #close the database connection when done

import pandas as pd
import sqlite3 as lite

con=lite.connect('citi_bike.db')
cur = con.cursor

# start with this line
df = pd.read_sql_query('select * from available_bikes order by execution_time', con, index_col='execution_time')

#df.to_pickle('/Users/amybrown/Thinkful/Unit_3/Lesson_1/citybike_df.csv') doesn't write file cleanly

hour_change = collections.defaultdict(int)
for col in df.columns:
    station_vals = df[col].tolist()
    station_id = col[1:] #trim the "_"
    station_change = 0
    for k,v in enumerate(station_vals):
        if k < len(station_vals) - 1:
            station_change += abs(station_vals[k] - station_vals[k+1])
    hour_change[int(station_id)] = station_change #convert the station id back to integer
    
def keywithmaxval(d):
    """Find the key with the greatest value"""
    return max(d, key=lambda k: d[k])

# assign the max key to max_station
max_station = keywithmaxval(hour_change)

#query sqlite for reference information

from datetime import date

con = lite.connect('citi_bike.db')
cur = con.cursor()
cur.execute("SELECT id, stationname, latitude, longitude FROM citibike_reference WHERE id = ?", (max_station,))
data = cur.fetchone()
print("The most active station is station id %s at %s latitude: %s longitude: %s " % data)
print("With %d bicycles coming and going in the hour between %s and %s" % (
    hour_change[max_station],
    date.fromtimestamp(int(df.index[0])).strftime('%Y-%m-%dT%H:%M:%S'),
    date.fromtimestamp(int(df.index[-1])).strftime('%Y-%m-%dT%H:%M:%S'),
))

import matplotlib.pyplot as plt
plt.bar(hour_change.keys(), hour_change.values())
plt.show()