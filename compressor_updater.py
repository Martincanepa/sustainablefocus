import pymysql
import pandas as pd
from datetime import datetime
from datetime import date
from datetime import timedelta
import pytz
import time
from time import sleep
from sqlalchemy import create_engine

tz = pytz.timezone('Australia/Adelaide')


site_names = ["Hectorville", "Prospect", "Findon", "Broadview", "Morphett Vale", "Littlehampton", "Underdale"]

feed_numbers = [[155,163,165,167,169,171,3,11,13,15],\
            [156, 164, 166,  168, 170, 172, 4, 12, 16, 18],\
            [566, 586, 591, 596, 601, 606, 186, 206, 216, 221],\
            [567, 587,592, 597, 602, 607, 187, 207, 217, 222],\
            [568, 588, 593, 598, 603, 608, 188, 208, 218, 223],\
            [569, 589, 594, 599, 604, 609, 189, 209, 219, 224],\
            [570, 590, 595, 600, 605, 610, 190, 210, 215, 225]]

feed_names = ["Temp1","Temp2","Temp3","Temp4","Temp5","Temp6", "CO21","CO22","CO23","CO24"]


# SQL connect function takes a list for an argument where all feed numbers must be indicated. The function then fetches \
# this information from the server and returns a pandas dataframe object

def sql_connect(feed_number):
    # lists to append values read from the database to then transfor to pandas dataframe
    data = pd.DataFrame()
    time_stamp = []
    values = []
    
    # date values to fetch most upto date values from server
    today = date.today()
    two_days_ago = today - timedelta(days=1)
    secs_two_days_ago = int(time.mktime(two_days_ago.timetuple()))


    # Connection Credentials
    database = "emoncms"
    username = 'ssl-analytics'
    password = 'SF_Emali-analytics_1120'


    # Open database connection
    db = pymysql.connect(host='198.199.98.60', user=username, passwd=password, db=database )

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    # Select first statement to create master table or second statement to add up to date values
#     sql = "select * from feed_"+str(feed_number)
    sql = "select * from feed_"+str(feed_number)+" where time> "+str(secs_two_days_ago)

    # execute SQL query
    cursor.execute(sql)
    results = cursor.fetchall()

    #  iterate through all the resutls and append values to list
    for i in range(len(results)):
        time_stamp.append(results[i][0])
        values.append(results[i][1])

    # add appended values to pandas dataframe
    data["Time Stamp"] = time_stamp
    data["Values"] = values



    db.close()
    
    return data


def feed_compressor(site_name, data, feed_name): # transforms timestamp into date in the following format YYYY/MM/DD HH:MM:SS and saves it into two columns
    
    # declare all list values to later add to pandas dataframe
    values = []
    hour = []
    date = []
    centre = []
    
    # transform numeric timestamp into date & time and split into two columns
    for i in range(len(data["Time Stamp"])):
        temp = datetime.fromtimestamp(data["Time Stamp"][i], tz)
        split = str(temp).split(" ")
        date.append(split[0])
        hour.append(int((split[1].split(":"))[0]))
    
    # add appended values to pandas dataframe 
    data["Date"] = date
    data["Hour"] = hour
   
    previous_date = 0
    current_date = 0
    previous_hour = 0
    current_hour = 0
    counter = 0
    values = 0
    hourly_averages = []
    hourly_dates = []
    hourly_hours = []

    # the compressor is essentially a funtion that iterates thoguh every value present in the previously created pandas \
    # the condition is to add up all values with the same "Hour" and count every time this is done. This condition
    # only satisfied when previous_hour = current_hour and when this statement stops being true, then the added values are 
    # divided by the times this was done, essentially an average, and it gets appended to a list called hourly_averages.
    # In parallel, the correspnding date for that hour is also stored on another list called hourly_dates which is in reality 
    # the first i of the iteration cycle for any given hour.
    
    for i in range(len(data)):
        current_date = data["Date"][i]
        current_hour = data["Hour"][i]
        if(previous_date == current_date and previous_hour == current_hour):
            counter += 1 
            values += data["Values"][i-1]
        else:
            try:
                # limit condition for the first value in the database, when pervious will  always be different to current
                # the try except functions are used to account for the singularity / unditermined case when 0 / 0.
                if((values/counter)!=0 and i !=0):
                    values = values + data["Values"][i-1] #this is needed since the last value won't be added in the previous if
                    counter = counter + 1
                    hourly_averages.append(float("{:.2f}".format(values/counter)))
                    hourly_dates.append(previous_date)
                    hourly_hours.append(previous_hour)
                    centre.append(site_name)
                counter = 0
                values = 0
                previous_date = data["Date"][i]
                previous_hour = data["Hour"][i]
                
            except:
                previous_date = data["Date"][i]
                previous_hour = data["Hour"][i]
                continue
                
    # pour over values from previously created list into pandas dataframe with compressed hourly values            
    hourly_db = pd.DataFrame()
    hourly_db["Centre"] = centre
    hourly_db["Date"] = hourly_dates
    hourly_db["Hour"] = hourly_hours
    hourly_db[feed_name] = hourly_averages
    hourly_db
    
    return hourly_db


# function to post pandas dataframe into database
def post(df, table_name):
    hostname = '45.55.108.62'
    database = "reporting"
    username = 'martin'
    password = 'democrat-Bath-plato'

    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db=database, user=username, pw=password))

    df.to_sql(table_name, engine, index=False, if_exists = 'replace')
    
    return


def read_dataframe():
    read_df = pd.DataFrame()

    
    database = "reporting"
    username = 'martin'
    password = 'democrat-Bath-plato'

    Centre = []
    Date = []
    Hour = []
    Temp1 = []
    Temp2 = []
    Temp3 = []
    Temp4 = []
    Temp5 = []
    Temp6 = []
    CO21 = []
    CO22 = []
    CO23 = []
    CO24 = []

    columns = [Centre, Date, Hour, Temp1, Temp2, Temp3, Temp4, Temp5, Temp6, CO21, CO22, CO23, CO24]

    db = pymysql.connect(host='45.55.108.62', user=username, passwd=password, db=database )

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    # sql = "select * from feed_"+str(feed_number)+" where time> "+str(secs_two_days_ago)
    sql = "select * from reporting.compressed"

    cursor.execute(sql)
    results = cursor.fetchall()

    for k in range(len(results)):
        for i in range(len(columns)):
            columns[i].append(results[k][i])


    read_df["Centre"] = Centre
    read_df["Date"] = Date
    read_df["Hour"] = Hour
    read_df["Temp1"] = Temp1
    read_df["Temp2"] = Temp2
    read_df["Temp3"] = Temp3
    read_df["Temp4"] = Temp4
    read_df["Temp5"] = Temp5
    read_df["Temp6"] = Temp6
    read_df["CO21"] = CO21
    read_df["CO22"] = CO22
    read_df["CO23"] = CO23
    read_df["CO24"] = CO24

    return read_df

# the table_compressor function builds on top of the previously created funtctions, and essentially concatenates the 
# tables created with the feed_compressor function. In other words, the feed_compressor compressed every feed and merges
# every result expanding the resulting table across whreas the table_compressor function contatenates the tables created by
# the feed_compressor and hence it expands the resulting table not horitzontally but vertically.

def table_compressor(site_names, feed_numbers, feed_names):
    frames = []
    for k in range(len(site_names)):
        print(k)
        for i in range(len(feed_names)):
            data = sql_connect(feed_numbers[k][i])
            compressed_feed = feed_compressor(site_names[k], data, feed_names[i])
            if i == 0:
                joined_feeds = compressed_feed
                continue
            joined_feeds = pd.merge(joined_feeds, compressed_feed, on=['Centre','Date','Hour'], how='inner')
        frames.append(joined_feeds)
    result = pd.concat(frames, sort = False)
    read_df = read_dataframe()
    up_to_date_table = pd.concat([read_df, result]).drop_duplicates()
    post(up_to_date_table, "compressed")
    print("done")
    return up_to_date_table

while(True):
    up_to_date_table = table_compressor(site_names, feed_numbers, feed_names)
    print("")
    print("Task executed successfully, now waiting 1 hour")
    sleep(3600)

