from urllib.request import Request, urlopen
import ssl
from bs4 import BeautifulSoup
import pandas as pd
import re
from time import sleep
import numpy as np

class BOM:
    
    def link_builder(link,year):
        link_split = link.split("=")
        ncc_code = link_split[1].split("&")[0]
        c = link_split[4].split("&")[0]
        station = link_split[-1]
        link = "http://www.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_nccObsCode={}&p_display_type=dailyDataFile&p_startYear={}&p_c={}&p_stn_num={}".format(ncc_code,year,c,station)
        print(link)
        return link
        
    
    def get_values(site):
        values = []
        hdr = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
        req = Request(site,headers=hdr)
        page = urlopen(req)
        soup = BeautifulSoup(page)
        table = soup.find('tbody')
        
        rows = table.findAll('tr')
        data = rows[0].findAll('td')
        for i in range(12):
            for k in range(1,32):
                data = rows[k].findAll('td')
                try:
                    if(data[i].attrs["class"][0]=="notDay"):
                        continue
                    else:
                        try:
                            values.append(float(data[i].text))
                        except:
                            values.append(None)

                except:
                    try:
                        values.append(float(data[i].text))
                    except:
                        values.append(None)
        return values
    
    def create_table(min_values,max_values,year):
        df = pd.DataFrame()
        df["Date"] = pd.date_range(start="1/1/{}".format(year), end="31/12/{}".format(year))
        df["Week"] = pd.to_datetime(df["Date"]).dt.week
        df["Date"] = df["Date"].dt.strftime('%d/%m/%Y')
        is_month = lambda date: date.split("/")[1]
        is_year = lambda date: date.split("/")[2]
        df["Month"] = df["Date"].apply(is_month)
        df["Year"] = df["Date"].apply(is_year)
        df["Min"] = min_values
        df["Max"] = max_values
        df["Mean"] = (df["Min"]+df["Max"])/2
        
        return df
      
min_url = "http://www.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_nccObsCode=123&p_display_type=dailyDataFile&p_startYear=2019&p_c=-1492452628&p_stn_num=086383"
max_url = "http://www.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_nccObsCode=122&p_display_type=dailyDataFile&p_startYear=2019&p_c=-1492452432&p_stn_num=086383"
# min_url = input("Enter min: ")
# max_url = input("Enter max: ")

    
baseline_period_start = "01/09/2017"
baseline_period_end = "31/08/2018"
reporting_period_start = "01/11/2019"
reporting_period_end = "31/10/2020"

def table_to_csv(period_start,period_end,name):
    year_start = period_start.split("/")[2]
    year_end = period_end.split("/")[2]

    if(year_start == year_end):
        year = year_start
        test = BOM    
        min_values = test.get_values(test.link_builder(min_url,year))
        max_values = test.get_values(test.link_builder(min_url,year))
        final_df = test.create_table(min_values,max_values, year)
        export_csv = final_df.to_csv(name+'_table.csv',index = None, header=True)

    else:
        test = BOM
        min_values1 = test.get_values(test.link_builder(min_url,year_start))
        max_values1 = test.get_values(test.link_builder(max_url,year_start))
        df1 = test.create_table(min_values1,max_values1, year_start)
        min_values2 = test.get_values(test.link_builder(min_url,year_end))
        max_values2 = test.get_values(test.link_builder(max_url,year_end))
        df2 = test.create_table(min_values2, max_values2, year_end)
        df3 = pd.concat([df1,df2]).reset_index(drop=True)
        index_start = df3[df3["Date"] == period_start].index.item()
        index_end = df3[df3["Date"]==period_end].index.item()
        final_df = df3.loc[range(index_start,index_end+1)]
        export_csv = final_df.to_csv(name+'_table.csv',index = None, header=True)
        return final_df
    

baseline_table = table_to_csv(baseline_period_start,baseline_period_end,"baseline")
reporting_table = table_to_csv(reporting_period_start,reporting_period_end,"reporting")
print(baseline_table)
print(reporting_table) 
