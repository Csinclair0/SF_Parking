#!/usr/bin/python3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import itertools
import datetime as dt
import time
from scipy import stats
import geopandas as gpd
import sqlite3
import folium
from folium import plugins



conn = sqlite3.connect('SF_Parking.db')
c = conn.cursor()
raw_folder = '/home/colin/Desktop/Parking_Project/data/raw/'
proc_folder = '/home/colin/Desktop/Parking_Project/data/processed/'


def load_data():
    """Function to load and associate all tickets with an address for further analysis."""
    ticket_data = pd.read_sql_query("Select  * from ticket_data " ,con = conn, parse_dates = ['TickIssueDate'])
    address_data = result_query('Select * from address_data')
    agencies = pd.read_csv(raw_folder +'SF_Agency_List.csv')
    c.execute('Select Max(TickIssueDate), Min(TickIssueDate) from ticket_data')
    totaldays = c.fetchone()
    maxdate = time.strptime( totaldays[0], '%Y-%m-%d %H:%M:%S')
    mindate = time.strptime( totaldays[1], '%Y-%m-%d %H:%M:%S')
    totaldays = (time.mktime(maxdate) - time.mktime(mindate)) / (60*60*24)
    totalyears = totaldays /365


def generate_plots():
    """Create all plots from exploratory analysis"""

    #Most common ticket types
    ax = ticket_data['ViolationDesc'].value_counts().nlargest(10).plot(kind = 'bar', figsize = (15,5))
    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    ax.set_title('Total Tickets by Violation Type')

    #Most common vehicle types
    ax = ticket_data['VehMake'].value_counts().nlargest(30).plot(kind = 'bar', figsize = (15,5))
    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    ax.set_title('Total Tickets by Vehicle Make')


    ticket_data = ticket_data.merge(address_data, left_on = 'address', right_on = 'address')

    #Total tickets by neighborhood
    ax = ticket_data['nhood'].value_counts().nlargest(20).plot(kind = 'bar', figsize = (15,5))
    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    ax.set_title('Total Tickets by Neighborhood')



    #Types of Tickets by hour
    ticket_data['Hour'] = ticket_data['TickIssueDate'].apply(lambda x: pd.to_datetime(x).hour)
    type_by_hour = ticket_data[ticket_data['ViolationDesc'].isin(ticket_data['ViolationDesc'].value_counts()[:10].index.tolist())]
    pivot_df = type_by_hour.groupby(['Hour', 'ViolationDesc'])['Hour'].count().unstack('ViolationDesc').fillna(0)
    ax = pivot_df.plot(kind = 'bar', figsize = (15, 10), stacked = True)
    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    ax.set_title('Total Tickets by Hour of Day, by Ticket Type')


    #Street cleaning by hour only
    type_by_hour = ticket_data[ticket_data['ViolationDesc'] == 'STR CLEAN']
    pivot_df = type_by_hour.groupby(['Hour', 'ViolationDesc'])['Hour'].count().unstack('ViolationDesc').fillna(0)
    ax = pivot_df.plot(kind = 'bar', figsize = (10, 6), stacked = True)
    ax.set_title('Total Street Cleaning Tickets by Hour of Day, by District')
    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))


    #Tickets by Nhood by hour
    ticket_data = ticket_data[ticket_data['nhood'] !='Unkown']
    type_by_hood = ticket_data[ticket_data['nhood'].isin(ticket_data['nhood'].value_counts()[:10].index.tolist())]
    pivot_df = type_by_hood.groupby(['Hour', 'nhood'])['Hour'].count().unstack('nhood').fillna(0)
    pivot_df.plot(kind = 'bar', figsize = (15, 10), stacked = True)
    plt.legend(loc = 1)
    plt.title('Total Tickets by Hour of Day, Split by Neighborhood')


    #Types of Tickets by day of week
    ticket_data['Weekday'] = ticket_data['TickIssueDate'].apply(lambda x: x.weekday())
    type_by_dow = ticket_data[ticket_data['ViolationDesc'].isin(ticket_data['ViolationDesc'].value_counts()[:10].index.tolist())]
    pivot_df = type_by_dow.groupby(['Weekday', 'ViolationDesc'])['Weekday'].count().unstack('ViolationDesc').fillna(0).reset_index()
    days = {6:'Sunday', 0:'Monday', 1:'Tuesday', 2:'Wednesday', 3:'Thursday', 4:'Friday', 5:'Saturday'}
    pivot_df['Weekday'] = pivot_df['Weekday'].map(days)
    pivot_df.plot( x= 'Weekday', kind = 'bar', figsize = (10, 6), stacked = True)
    plt.title('Total Tickets by Day, By Ticket Type')



    #only look at top 15 neighgborhoood, top 10 ticket types
    df = ticket_data[ticket_data['nhood'].isin(ticket_data['nhood'].value_counts()[:15].index)]
    df = df[df['ViolationDesc'].isin(df['ViolationDesc'].value_counts()[:10].index)]
    total_tickets_by_hood = df.groupby(['nhood', 'ViolationDesc'])['index_x'].count()
    by_nhood = df.groupby(['nhood', 'ViolationDesc'])['index_x'].count()
    nhood_pct = by_nhood.groupby(level=0).apply(lambda x: 100 * x / float(x.sum())).unstack('ViolationDesc').fillna(0)
    nhood_pct.plot(kind = 'bar', stacked = True, figsize = (15, 10))
    plt.legend(bbox_to_anchor = (0, 1))
    plt.title('Percent Share of Ticket Type by Neighborhood')



    #Look at neighborhoods top car types, sort by least toyota
    df = ticket_data[ticket_data['VehMake'].isin(ticket_data['VehMake'].value_counts()[:10].index)]
    by_car = df.groupby(['nhood', 'VehMake'])['index_x'].count()
    by_car_pct = by_car.groupby(level=0).apply(lambda x: 100 * x / float(x.sum())).unstack('VehMake').fillna(0)
    cols = ['TOYT', 'HOND', 'FORD', 'CHEV', 'VOLK', 'NISS', 'SUBA', 'BMW', 'MERZ', 'MISC']
    by_car_pct = by_car_pct[cols]
    by_car_pct.sort_values(by = 'TOYT', ascending = False, inplace = True)
    by_car_pct.plot(kind = 'bar', stacked = True, figsize = (15, 10))
    plt.legend(bbox_to_anchor = (0, 1))
    plt.title('Percent Share of Tickets by Vehicle Make, by Neighborhood')


def create_my_ticket_map():
    """Function to find and map all of my tickets within the data""""

    my_tickets = result_query("Select * from ticket_data where TickRPPlate = '7XCS244'")
    my_tickets = my_tickets.merge(address_data, left_on = 'address', right_on = 'address')
    m = folium.Map([37.7749, -122.4194], zoom_start = 12)

    for i in np.arange(len(my_tickets)):
        folium.Marker(location = [my_tickets['lat'][i],my_tickets['lon'][i]], \
                      popup = my_tickets['TickIssueDate'][i] + ' received '+ my_tickets['ViolationDesc'][i]).add_to(m)



def create_otherguys_tickets():
    """Function to fin the most ticketed man in San Francisco's tickets and then map them out"""
    some_car = result_query("Select * from ticket_data where TickRpPlate = '7MJW700'")
    some_car = some_car.merge(address_data, left_on = 'address', right_on = 'address')
    m = folium.Map([37.7749, -122.4194], zoom_start = 12)

    for i in np.arange(len(some_car)):
        folium.Marker(location = [some_car['lat'][i],some_car['lon'][i]], \
                      popup = some_car['TickIssueDate'][i] + ' received '+ some_car['ViolationDesc'][i]).add_to(m)
    m

def create_heatmaps():




def volume_maps():
    """Function to plot volume maps, while coloring streets using colormap""".
    streetvolume = gpd.read_file(proc_folder + './final_streets/SF_Street_Data.shp')

    streetvolume = streetvolume.to_crs(epsg = 4326)
    times = ['am', 'pm', 'ev', 'ea']
    for time in times:
        streetvolume['totalinv_' + time]  = streetvolume['total_'+time].apply(lambda x: np.log(1/(x+.5)))

    streetvolume.plot(column = 'totalinv_ea', cmap = 'RdYlGn',  figsize = (15,15), alpha = .75)
