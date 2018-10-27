#!/usr/bin/python3
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import datetime as dt
import time
import re
import geopandas as gpd
import sqlite3
import folium
from shapely.geometry import Point
import webbrowser, os
import mplleaflet



raw_loc = '/home/colin/Desktop/SF_Parking/data/raw/'
proc_loc = '/home/colin/Desktop/SF_Parking/data/processed/'
image_loc = '/home/colin/Desktop/SF_Parking/reports/figures/explore/'
map_loc = '/home/colin/Desktop/SF_Parking/reports/maps/'
#plt.style.use('fivethirtyeight')
mpl.rcParams['savefig.bbox'] = 'tight'
mpl.rcParams['figure.autolayout'] = True
mpl.rc('xtick', labelsize = 8 )


global conn
conn = sqlite3.connect(proc_loc + 'SF_Parking.db')




def data_by_meter(storefigs, address_data):
    """Function to plot out average tickets per meter for each neighborhood

    Returns
    -------
    plot.
    """
    meter_address = pd.read_sql_query('Select TickMeter, address, count(*) num from ticket_data group by TickMeter, address', conn)
    meter_address = meter_address[meter_address.address.isin(address_data['address'])]
    meter_address.sort_values(by = 'num', ascending = False)
    meter_address.drop_duplicates(subset = 'TickMeter')
    meters = meters.merge(meter_address, left_on = 'TickMeter', right_on = 'TickMeter')

    plt.figure(figsize = (10,6))
    ax = meters.merge(address_data, left_on = 'address', right_on = 'address').groupby(by = 'nhood')['total_tickets'].mean().plot(kind = 'bar')
    ax.set_title('Average Tickets per Parking Meter by Neighborhood')
    plt.show()
    title = 'MetbyNhood.png'
    if storefigs == 'Y':
        plt.savefig(image_loc + title)

    return


def load_data():
    """
    Function to load and associate all tickets with an address for further analysis.

    Returns
    -------
    type
        Description of returned object.

    """
    ticket_data = pd.read_sql_query("Select  * from ticket_data " ,con = conn, parse_dates = ['TickIssueDate'])
    address_data = pd.read_sql_query('Select * from address_data', conn)
    ticket_data = ticket_data.merge(address_data, left_on = 'address', right_on = 'address')
    return ticket_data


def generate_plots(ticket_data, storefigs):
    """function to go through and create exploratory plots.

    Parameters
    ----------
    ticket_data : dataframe
        merged ticket and address data

    Returns
    -------
    none

    """
    agencies = pd.read_csv(raw_loc +'SF_Agency_List.csv')
    c = conn.cursor()
    c.execute('Select Max(TickIssueDate), Min(TickIssueDate) from ticket_data')
    totaldays = c.fetchone()
    maxdate = time.strptime( totaldays[0], '%Y-%m-%d %H:%M:%S')
    mindate = time.strptime( totaldays[1], '%Y-%m-%d %H:%M:%S')
    totaldays = (time.mktime(maxdate) - time.mktime(mindate)) / (60*60*24)
    totalyears = totaldays /365

    ticket_data['nhood'] = ticket_data['nhood'].apply(lambda x: 'Fidi' if x == 'Financial District/South Beach' else x)


    #Most common ticket types
    choice = int(input("How many of the most common tickets would you like to see? (def 10)"))
    if isinstance(choice, int) == False:
        print("I did not like that answer cause it was invalid. I'm gonna show 10.")
        choice = 10


    ax = ticket_data['ViolationDesc'].value_counts().nlargest(choice).plot(kind = 'bar', figsize = (15,5), zorder = 1)
    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    plt.xticks(rotation = 45)
    ax.set_axisbelow(True)
    ax.yaxis.grid(color='gray', linestyle='dashed', alpha = .5)
    ax.set_title('Total Tickets by Violation Type')
    plt.show()
    title = 'TickByViolation.png'
    if storefigs == 'Y':
        plt.savefig(image_loc + title)


    #Most common vehicle types
    choice = int(input("How many of the most common vehicles would you like to see? def (30)"))
    if isinstance(choice, int) == False:
        print("I did not like that answer cause it was invalid. I'm gonna show 30.")
        choice = 30
    ax = ticket_data['VehMake'].value_counts().nlargest(choice).plot(kind = 'bar', figsize = (15,5), zorder = 1)
    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    ax.set_title('Total Tickets by Vehicle Make')
    ax.set_axisbelow(True)
    ax.yaxis.grid(color='gray', linestyle='dashed', alpha = .5)
    plt.xticks(rotation = 45)
    title = 'TickByVehMake.png'
    if storefigs == 'Y':
        plt.savefig(image_loc + title)
    plt.show()



    #Total tickets by neighborhood
    choice = int(input("How many of the most common neighborhoods would you like to see? (def 20)"))
    if isinstance(choice, int) == False:
        print("I did not like that answer cause it was invalid. I'm gonna show 20.")
        choice = 20
    ax = ticket_data['nhood'].value_counts().nlargest(choice).plot(kind = 'bar', figsize = (15,5))
    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    ax.set_title('Total Tickets by Neighborhood')
    ax.set_axisbelow(True)
    ax.yaxis.grid(color='gray', linestyle='dashed', alpha = .5)
    plt.xticks(rotation = 45)
    title = 'TickByNHood.png'
    if storefigs == 'Y':
        plt.savefig(image_loc + title)
    plt.show()


    #Types of Tickets by hour
    choice = int(input("How many of the most common violations would you like to see, by hour? (def 10)"))
    if isinstance(choice, int) == False:
        print("I did not like that answer cause it was invalid. I'm gonna show 10.")
        choice = 10
    ticket_data['Hour'] = ticket_data['TickIssueDate'].apply(lambda x: pd.to_datetime(x).hour)
    type_by_hour = ticket_data[ticket_data['ViolationDesc'].isin(ticket_data['ViolationDesc'].value_counts()[:choice].index.tolist())]
    pivot_df = type_by_hour.groupby(['Hour', 'ViolationDesc'])['Hour'].count().unstack('ViolationDesc').fillna(0)
    ax = pivot_df.plot(kind = 'bar', figsize = (15, 10), stacked = True)
    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    ax.set_title('Total Tickets by Hour of Day, by Ticket Type')
    ax.set_axisbelow(True)
    ax.yaxis.grid(color='gray', linestyle='dashed', alpha = .5)
    plt.xticks(rotation = 45)
    title = 'TickByHour.png'
    if storefigs == 'Y':
        plt.savefig(image_loc + title)
    plt.show()



    #Street cleaning by hour only
    print("Heres street cleaning by hour, you get no decision in this.")
    type_by_hour = ticket_data[ticket_data['ViolationDesc'] == 'STR CLEAN']
    pivot_df = type_by_hour.groupby(['Hour', 'ViolationDesc'])['Hour'].count().unstack('ViolationDesc').fillna(0)
    ax = pivot_df.plot(kind = 'bar', figsize = (10, 6), stacked = True)
    ax.set_title('Total Street Cleaning Tickets by Hour of Day, by District')
    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    ax.set_axisbelow(True)
    ax.yaxis.grid(color='gray', linestyle='dashed', alpha = .5)
    plt.xticks(rotation = 0)
    ax.legend_.remove()
    title = 'StreetCleanByHour.png'
    if storefigs == 'Y':
        plt.savefig(image_loc + title)
    plt.show()




    #Tickets by Nhood by hour
    choice = int(input("How many of the most common neighborhoods would you like to see, by violation?(def10)"))
    if isinstance(choice, int) == False:
        print("I did not like that answer cause it was invalid. I'm gonna show 10.")
        choice = 10
    ticket_data_val = ticket_data[ticket_data['nhood'] !='Unkown']
    type_by_hood = ticket_data_val[ticket_data_val['nhood'].isin(ticket_data['nhood'].value_counts()[:choice].index.tolist())]
    pivot_df = type_by_hood.groupby(['Hour', 'nhood'])['Hour'].count().unstack('nhood').fillna(0)
    ax = pivot_df.plot(kind = 'bar', figsize = (15, 10), stacked = True)
    plt.legend(loc = 1)
    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    ax.set_title('Total Tickets by Hour of Day, Split by Neighborhood')
    ax.set_axisbelow(True)
    ax.yaxis.grid(color='gray', linestyle='dashed', alpha = .5)
    plt.xticks(rotation = 45)
    title = 'ByHourByHood.png'
    if storefigs == 'Y':
        plt.savefig(image_loc + title)
    plt.show()



    #Types of Tickets by day of week
    choice = int(input("How many of the most common violations would you like to see, by day of week? (def 10)"))
    if isinstance(choice, int) == False:
        print("I did not like that answer cause it was invalid. I'm gonna show 10.")
        choice = 10
    ticket_data['Weekday'] = ticket_data['TickIssueDate'].apply(lambda x: x.weekday())
    type_by_dow = ticket_data[ticket_data['ViolationDesc'].isin(ticket_data['ViolationDesc'].value_counts()[:choice].index.tolist())]
    pivot_df = type_by_dow.groupby(['Weekday', 'ViolationDesc'])['Weekday'].count().unstack('ViolationDesc').fillna(0).reset_index()
    days = {6:'Sunday', 0:'Monday', 1:'Tuesday', 2:'Wednesday', 3:'Thursday', 4:'Friday', 5:'Saturday'}
    pivot_df['Weekday'] = pivot_df['Weekday'].map(days)
    ax = pivot_df.plot( x= 'Weekday', kind = 'bar', figsize = (10, 6), stacked = True)
    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    plt.title('Total Tickets by Day of week, By Ticket Type')
    ax.set_axisbelow(True)
    ax.yaxis.grid(color='gray', linestyle='dashed', alpha = .5)
    plt.xticks(rotation = 45)
    title = 'ByDayByType.png'
    if storefigs == 'Y':
        plt.savefig(image_loc + title)
    plt.show()


    #only look at top 15 neighgborhoood, top 10 ticket types
    choice1 = int(input("How many of the most common Neighborhoods would you like to see, by violation? (def 15)"))
    if isinstance(choice1, int) == False:
        print("I did not like that answer cause it was invalid. I'm gonna show 10.")
        choice1 = 15
    choice2 = int(input("and number of violation types? (def 10)"))
    if isinstance(choice2, int) == False:
        print("I did not like that answer cause it was invalid. I'm gonna show 10.")
        choice2 = 10


    df = ticket_data[ticket_data['nhood'].isin(ticket_data['nhood'].value_counts()[:choice1].index)]
    df = df[df['ViolationDesc'].isin(df['ViolationDesc'].value_counts()[:choice2].index)]
    total_tickets_by_hood = df.groupby(['nhood', 'ViolationDesc'])['index_x'].count()
    by_nhood = df.groupby(['nhood', 'ViolationDesc'])['index_x'].count()
    nhood_pct = by_nhood.groupby(level=0).apply(lambda x: 100 * x / float(x.sum())).unstack('ViolationDesc').fillna(0)
    nhood_pct.plot(kind = 'bar', stacked = True, figsize = (15, 10))
    plt.legend(bbox_to_anchor = (0, 1))
    plt.title('Percent Share of Ticket Type by Neighborhood')
    title = 'ShareByHood.png'
    if storefigs == 'Y':
        plt.savefig(image_loc + title)
    plt.show()




    #Look at neighborhoods top car types, sort by least toyota
    choice = int(input("How many of the most common Vehicle makes would you like to see, by neighborhood? (def 10)"))
    if isinstance(choice, int) == False:
        print("I did not like that answer cause it was invalid. I'm gonna show 10.")
        choice = 10

    df = ticket_data[ticket_data['VehMake'].isin(ticket_data['VehMake'].value_counts()[:choice].index)]
    by_car = df.groupby(['nhood', 'VehMake'])['index_x'].count()
    by_car_pct = by_car.groupby(level=0).apply(lambda x: 100 * x / float(x.sum())).unstack('VehMake').fillna(0)
    cols = ['TOYT', 'HOND', 'FORD', 'CHEV', 'VOLK', 'NISS', 'SUBA', 'BMW', 'MERZ', 'MISC']
    by_car_pct = by_car_pct[cols]
    by_car_pct.sort_values(by = 'TOYT', ascending = False, inplace = True)
    by_car_pct.plot(kind = 'bar', stacked = True, figsize = (15, 10))
    plt.legend(bbox_to_anchor = (0, 1))
    plt.title('Percent Share of Tickets by Vehicle Make, by Neighborhood')
    title = 'VehByNhood.png'
    if storefigs == 'Y':
        plt.savefig(image_loc + title)
    plt.show()



    choice = input('Would you like to generate a chart by neighborhood, plotting meter success?')
    if choice == 'Y':
        data_by_meter(storefigs, address_data)

    return


def create_ticket_map(lic_plate, ticket_data):
    """Function to find and map all of the tickets for any specific license plate
    Parameters
    ----------
    lic_plate : string
        licence plate string

    Returns
    -------
    folium map with markers.

    """
    tickets = ticket_data[ticket_data.TickRPPlate == lic_plate]

    if (tickets.shape[0] > 0):
        m = folium.Map([37.7749, -122.4194], zoom_start = 12)
        for i in np.arange(len(tickets)):
            folium.Marker(location = [tickets['lat'].iloc[i],tickets['lon'].iloc[i]], \
                    popup =str( tickets['TickIssueDate'].iloc[i]) + ' received '+ tickets['ViolationDesc'].iloc[i]).add_to(m)

        filename = map_loc + lic_plate + ".html"
        m.save(filename)
        webbrowser.open('file://' + os.path.realpath(filename))
        time.sleep(20)
    return "No Tickets Found with that License Plate. Try Mine! '7XCS244'"



def create_heatmap_query(sql_add):
    """Function to return a heatmap of tickets that meet the criteria of the search addding.

    Parameters
    ----------
    sql_add : string
        string argument of query. Will follow 'WHERE' in SQL statement.

    Returns
    -------
    folium heatmap


    """
    df = pd.read_sql_query('Select lat, lon from ticket_data t1 join address_data t2 on t1.address = t2.address '
                     "where " + sql_add, conn)
    if df.shape[0] > 50000:
        df = df.sample(n = 50000)
    ticketarr = df[['lat', 'lon']].as_matrix()
    m = folium.Map([37.7749, -122.4194], zoom_start = 12)
    m.add_children(folium.plugins.HeatMap(ticketarr, radius = 8))
    filename = map_loc + re.sub('[\W]', '', sql_add)+ ".html"
    m.save(filename)
    webbrowser.open('file://' + os.path.realpath(filename))
    time.sleep(20)
    return



def volume_maps(ticket_data):
    """Function to plot volume maps, while coloring streets using colormap. We'll also add tickets if you user requests.

    Parameters
    ----------
    ticket_data : dataframe
        merged ticket data


    Returns
    -------
    matplotlib chart
        chart of volume and tickets

    """
    choice = int(input('How many tickets would you like to add?'))
    streetvolume = gpd.read_file(proc_loc + 'final_streets/SF_Street_Data.shp')
    streetvolume = streetvolume.to_crs(epsg = 4326)
    times = ['am', 'pm', 'ev', 'ea']
    for time in times:
        streetvolume['totalinv_' + time]  = streetvolume['total_'+time].apply(lambda x: np.log(1/(x+.5)))
    nhoods = gpd.read_file(raw_loc + 'AnalysisNeighborhoods.geojson')

    ax = streetvolume.plot(column = 'totalinv_ea', cmap = 'RdYlGn', alpha = 1)
    if choice > 0:
        df = ticket_data.sample(n = choice)
        geometry = [Point(xy) for xy in zip(df.lon, df.lat)]
        df = df.drop(['lon', 'lat'], axis=1)
        crs = {'init': 'epsg:4326'}
        gdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)
        gdf.plot(ax = ax, marker = "*", color='black', markersize=2);
    filename = (map_loc + 'VolumeMap.html')
    try:
        os.remove(filename)
    except:
        pass

    mplleaflet.show(fig=ax.figure, crs=streetvolume.crs, tiles='cartodb_positron', path = filename)


    return





def colored_ticket_map(ticket_data):
    """Function to plot volume maps, while coloring streets using colormap. We'll also add tickets if you user requests.

    Parameters
    ----------
    ticket_data : dataframe
        merged ticket data


    Returns
    -------
    matplotlib chart
        chart of volume and tickets

    """
    colordict = {'STR CLEAN': 'cyan', 'RES/OT': 'green', 'MTR OUT DT': 'red', 'DRIVEWAY': 'orange', 'DBL PARK':'blue'}
    print(colordict)
    choice = int(input('How many tickets would you like to add?'))
    if choice > 0:
        df = ticket_data.sample(n = choice)
        df['color'] = df['ViolationDesc'].apply(lambda x: colordict.get(x, 'magenta'))
        colors = df['color']
        geometry = [Point(xy) for xy in zip(df.lon, df.lat)]
        df = df.drop(['lon', 'lat'], axis=1)
        crs = {'init': 'epsg:4326'}
        gdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)
        ax = gdf.plot( marker = "*", color=colors, markersize=2);
    filename = (map_loc + 'TicketMap.html')
    try:
        os.remove(filename)
    except:
        pass

    mplleaflet.show(fig=ax.figure, crs=gdf.crs, tiles='cartodb_positron', path = filename)

    return




def main():
    print('Loading Data in usable form for analysis')
    ticket_data = load_data()
    storefigs = input('Would you like to save the figures in the project folder?(Y or N)')
    choice = input('Welcome to the Exploratory Section. You wanna See some charts? Cause I got charts.(Y or N)')
    if choice == 'Y':
        generate_plots(ticket_data, storefigs)

    choice = input('Would you like to look up some license plates? (Y or N)')
    if choice == 'Y':
        while choice == 'Y':
            querystring = input('What license plate?')
            create_ticket_map(querystring, ticket_data)
            choice = input('Would you like another?')



    choice = input('Would you like to create some heatmaps? (Y or N)')
    if choice == 'Y':
        while choice == 'Y':
            querystring = input('What would you like to filter on? (Please refer to readme for instructions)')
            create_heatmap_query(querystring)
            choice = input('Would you like another?')


    choice = input('Would you like to see a volume plot? (Y or N)')
    if choice == 'Y':
        volume_maps(ticket_data)


    choice = input('Would you like to plot some tickets colored by type? (Y or N)')
    if choice == 'Y':
        colored_ticket_map(ticket_data)

    print('You have made it through the Exploratory section!')



if __name__== '__main__':
    main()
