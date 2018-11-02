#!/usr/bin/python3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import datetime as dt
import time
from scipy import stats
import matplotlib as mpl
import geopandas as gpd
import sqlite3
import pickle
import math
from shapely.geometry import Point
from geopandas import GeoSeries, GeoDataFrame
import mplleaflet
from matplotlib.animation import FuncAnimation
import matplotlib.animation as animation

global conn

raw_loc= '/home/colin/Desktop/SF_Parking/data/raw/'
proc_loc = '/home/colin/Desktop/SF_Parking/data/processed/'
map_loc = '/home/colin/Desktop/SF_Parking/reports/maps/'
conn = sqlite3.connect(proc_loc + 'SF_Parking.db')
colordict = {'STR CLEAN': 'cyan', 'RES/OT': 'green', 'MTR OUT DT': 'red', 'DRIVEWAY': 'orange', 'DBL PARK':'blue'}


def project_to_line(lineid, streetvolume, point):

    df = streetvolume[streetvolume.lineid == lineid]
    line = df['geometry'].iloc[0]
    x = np.array(point.coords[0])

    u = np.array(line.coords[0])
    v = np.array(line.coords[len(line.coords)-1])

    n = v - u
    n /= np.linalg.norm(n, 2)

    npoint = u + n*np.dot(x - u, n)
    npoint = Point(npoint[0], npoint[1])
    return npoint


def load_data():
    """Loads all required dataframes for use.
    Returns
    -------
    dataframes
        address data, streetvolume(geo), nhoods(geo), streetsweeping(geo)

    """
    address_data = pd.read_sql_query('Select * from address_data', conn)
    streetvolume = gpd.read_file(proc_loc + '/final_streets/SF_Street_Data.shp')
    nhoods = gpd.read_file(raw_loc + 'AnalysisNeighborhoods.geojson')
    streetsweeping = gpd.read_file(proc_loc + 'final_sweeping/final_sweeping.shp')
    return address_data, streetvolume, nhoods, streetsweeping

def create_routes():
    """Queries sql for our last ticket of every day for each street link, returns into dataframe.

    Returns
    -------
    dataframe
        dataframe detailing route and ticket times.

    """
    by_route = pd.read_sql_query("Select  strftime('%Y-%m-%d', TickIssueDate) as sweepdate, lineid, "
                        " max(strftime('%H:%M',TickIssueDate)) as last_ticket from ticket_data t1 "
                       " join address_data t2 on t1.address = t2.address WHERE ViolationDesc = 'STR CLEAN' "
                    " group by strftime('%Y-%m-%d', TickIssueDate) ,  lineid", conn)
    by_route['weekday'] = by_route['sweepdate'].apply(lambda x: pd.to_datetime(x).weekday())
    by_route['mins'] = by_route['last_ticket'].apply(lambda x: int(x.split(':')[0]) * 60 + int(x.split(':')[1]))
    return by_route


def live_day_graph(datestring, address_data, streetvolume):
    """This function will take the date as an argument, and then it will create a live graph of a day, plotting tickets as they were given out that day.

    Parameters
    ----------
    datestring : string
        date, in form of string. that is the date you would like to query. Must be in format %d-%m-%Y.


    Returns
    -------
    type
        Description of returned object.

    """

    fig = plt.figure(figsize = (20,20))
    plt.rcParams["animation.html"] = "jshtml"
    plt.rcParams["animation.embed_limit"] = 100
    ax = plt.axes()
    sqlstring = "Select * from ticket_data where strftime('%d-%m-%Y', TickIssueDate) = '" + datestring + "'"
    df = pd.read_sql_query(sqlstring, conn)
    df = df.merge(address_data, left_on = 'address', right_on = 'address')
    df['color'] = df['ViolationDesc'].apply(lambda x: colordict.get(x, 'magenta'))
    ax.set_title('Parking tickets on ' + datestring)
    geometry = [Point(xy) for xy in zip(df.lon, df.lat)]
    crs = {'init': 'epsg:4326'}
    gdf = GeoDataFrame(df, crs=crs, geometry=geometry)
    nhoods = gpd.read_file(raw_loc + 'AnalysisNeighborhoods.geojson')



    nhoods.plot( ax = ax, alpha = .15, color = 'gray')
    # First set up the figure, the axis, and the plot element we want to animate
    streetvolume.plot(ax =ax, color = 'black', figsize = (20, 20), alpha =.25, linewidth = 1)
    gdf.sort_values(by = 'TickIssueDate', inplace = True)
    gdf['TickIssueDate'] = gdf['TickIssueDate'].apply(lambda x: dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    gdf['TickIssueTime'] = gdf['TickIssueDate'].apply(lambda x: x.time().hour*60 + int(x.time().minute))
    ttl = ax.text(.5, 1.05, '', transform = ax.transAxes, va='center')
    numframes = gdf.shape[0]
    i = 0

    def animate(i):
        if i%240 == 0:
            print('hour ' + str(i/240) + ' finished')
        df = gdf[gdf.TickIssueTime == i]
        timestr = (str(math.floor(i/4)) + ':' + str((i %4) * 15))
        colors = df['color']
        iterar = df.plot(ax = ax, marker = '*', c = colors, markersize = 10 )
        ttl.set_text(timestr)
        i += 1
        return iterar

    ani = FuncAnimation(fig, animate, repeat=False, frames = 1440, interval=100)
    #plt.show()
    # Set up formatting for the movie files
    Writer = animation.writers['ffmpeg']
    writer = Writer(fps=15, metadata=dict(artist='Me'), bitrate=1800)
    ani.save( map_loc + datestring + '.mp4', writer = writer)

    return


def getweekofmon(dt):
    """function to take datetime and return what day of week it is.

    Parameters
    ----------
    dt : datetime
        current datetime

    Returns
    -------
    integer
        day of week 0-mon to 7-sun

    """
    first_day = dt.replace(day=1)

    dom = dt.day
    adjusted_dom = dom + first_day.weekday()

    return int(math.ceil(adjusted_dom/7.0))





def find_recent_street_cleaning(streetsweeping, streetnumber, streetname, address_data, ResOT, invalid_ids):
    """Function to find recently cleaned streets. Process will be to first look up closest address, then filter on streets that were cleaned that day.

    Parameters
    ----------
    streetnumber : type
        Description of parameter `streetnumber`.
    streetname : type
        Description of parameter `streetname`.
    ResOT : type
        Description of parameter `ResOT`.

    Returns
    -------
    matplotlib axis


    """
    ad = address_data[address_data.street == streetname]
    if ad.shape[0] == 0:
        return print('Could not find streetname')
    ad['delta'] = np.abs(ad['number'] - streetnumber)
    ad.sort_values(by = 'delta', inplace = True)
    ad = ad.iloc[0]
    df = streetsweeping
    point = Point(ad.lon, ad.lat)

    df = df[df.lineid.isin(invalid_ids['lineid']) == False]

    weekdaydict = {0: 'Mon', 1:'Tues', 2:'Wed', 3:'Thu', 4:'Fri', 5:'Sat', 6:'Sun'}
    time =  dt.datetime.now()

    colname = 'week' + str(getweekofmon(time)) + 'ofmon'
    cleaned_today = df[(df.weekday == weekdaydict[time.weekday()]) & (df[colname] == 1)]
    not_today = df[(df.weekday != weekdaydict[time.weekday()]) | (df[colname] == 0)]
    nhoods = gpd.read_file(raw_loc + '/AnalysisNeighborhoods.geojson')
    cleaned_today['distance'] = cleaned_today['geometry'].apply(lambda x: point.distance(x))
    cleaned_today.sort_values(by = 'distance', inplace = True)
    cleaned_today_closest = cleaned_today[:25]



    ax = not_today.plot(color = 'red', alpha = .15)
    ax = cleaned_today.plot( color = 'yellow', alpha = .75)
    cleaned_today[:500].plot(ax = ax, color = 'green', alpha = 1)
    circleaddress = mpl.patches.Circle((ad['lon'], ad['lat']), radius = 5)
    mplleaflet.show(fig=ax.figure, crs=cleaned_today.crs, tiles='cartodb_positron',)
    return



def mean_confidence_interval(data, confidence=0.95):
    """Generates a confidence interval on when the street cleaner usually arrives. Creates map of last ticket given, and creates a confidence interval

    Parameters
    ----------
    data : numpy array
        array of times, minute form
    confidence : float
        confidence interval

    Returns
    -------
    average, lower and uppper confidence interval on what time that street get's cleaned.

    """
    a = 1.0 * np.array(data)
    n = len(a)
    m, se = np.mean(a), stats.sem(a)
    h = se * stats.t.ppf((1 + confidence) / 2., n-1)
    return m, m-h, m+h


def min_to_time(mins):
    """turns minutes into a  %H:%M format

    Parameters
    ----------
    mins : float
        minutes, total number

    Returns
    -------
    string representation of time

    """
    return str(math.ceil(mins / 60)) + ":" + str(int(mins%60))




def return_conf_interval(number, street, by_route, address_data):
    """function looks up the closest address to the input, create a dataframe that includes the last ticket given at a street on each day a ticket was given, and then create a confidence interval on that.

    Parameters
    ----------
    number : type
        Description of parameter `number`.
    street : type
        Description of parameter `street`.
    by_route : type
        Description of parameter `by_route`.

    Returns
    -------
    none
        prints output

    """
    ad = address_data[address_data.street == street]
    if ad.shape[0] == 0:
        return print('Could not find streetname')
    ad['delta'] = np.abs(ad['number'] - number)
    ad.sort_values(by = 'delta', inplace = True)
    streeline = ad['lineid'].iloc[0]
    street_data = by_route[by_route.lineid == streeline]
    if street_data.shape[0] == 0:
        return print('No street sweeping ticket data found for closest address. ')
    mean, low, high = mean_confidence_interval(street_data['mins'])
    print("low: " + min_to_time(low))
    print("mean: " + min_to_time(mean))
    print("High: " + min_to_time(high))

    return



def map_route_video(weekday, df, streetvolume):
    fig = plt.figure(figsize = (20,20))
    ax = plt.axes()
    plt.rcParams["animation.html"] = "jshtml"
    plt.rcParams["animation.embed_limit"] = 100
    nhoods = gpd.read_file(raw_loc + 'AnalysisNeighborhoods.geojson')



    nhoods.plot(ax = ax , alpha = .15, color = 'gray')
    # First set up the figure, the axis, and the plot element we want to animate
    streetvolume.plot(ax =ax, color = 'black', figsize = (20, 20), alpha =.25, linewidth = 1)
    df.sort_values(by = 'mins', inplace = True)
    ttl = ax.text(.5, 1.05, '', transform = ax.transAxes, va='center')
    i = 0
    df['mins'] = df['mins'].apply(lambda x: math.ceil(x))

    def animate(i):
        if i%240 == 0:
            print('hour ' + str(i/240) + ' finished')
        gdf = df[df.mins == i]
        timestr = (str(math.floor(i/60)) + ':' + str((i %60)))
        iterar = gdf.plot(ax = ax, color = 'red', alpha = 1, linewidth = 1 )
        ttl.set_text(timestr)
        i += 1
        return iterar

    ani = FuncAnimation(fig, animate, repeat=False, interval=100, frames = 1440)
    #plt.show()
    # Set up formatting for the movie files
    Writer = animation.writers['ffmpeg']
    writer = Writer(fps=15, metadata=dict(artist='Me'), bitrate=1800)

    ani.save( map_loc + weekday + 'cleaning.mp4', writer = writer)

    return


def map_the_route(weekday, by_route, streetvolume):
    """Function to take a weekday and map out from earliest to last where the street sweepers travel. Will plot on mplleaflet.

    Parameters
    ----------
    weekday : string
        string of weekday

    Returns
    -------
    matplotlib chart.

    """

    by_street = by_route[by_route.weekday ==weekday].groupby(by = 'lineid', as_index = False)['mins'].mean()
    df = streetvolume.merge(by_street, left_on = 'lineid', right_on = 'lineid')
    ax = df.plot(cmap = 'jet', column = 'mins', figsize = (20,20))
    weekdaynum = {1:'Mon',2:'Tues', 3:'Wed', 4:'Thurs',  5:'Fri', 6:'Sat',  7:'Sun'}
    weekday = weekdaynum[weekday]
    filename = (map_loc + weekday + 'RouteMap.html')
    try:
        os.remove(filename)
    except:
        pass
    print('Plotting HTML Map')
    mplleaflet.show(fig=ax.figure, crs=streetvolume.crs, tiles='cartodb_positron', path = filename)
    print('Creating Video File')
    map_route_video(weekday, df, streetvolume)

    return




def plot_model(numticks):
    """Function to plot a map, coloring the street based on the model fitted value.

    Parameters
    ----------
    numticks : int
        number of tickets to plot on the map

    Returns
    -------
    html map

    """
    print('Loading Data')
    df = pd.read_sql_query("Select t1.address, lat, lon, lineid from ticket_data t1 join address_data t2 on "
            " t1.address = t2.address where ViolationDESC = 'RES/OT' and lineid > 0  Limit + " + str(numticks), conn)


    print('Loading final model')
    with open(proc_loc + 'FinalModel.pkl', 'rb') as handle:
        model = pickle.load(handle)
    streetvolume = gpd.read_file(proc_loc + 'final_streets/SF_Street_Data.shp')
    model = model[['lineid', 'fitted']]
    streetdf = streetvolume.merge(model, left_on = 'lineid', right_on = 'lineid')
    streetdf['fitted'] = -1 *  streetdf['fitted']
    print('Creating Map of model')
    choice2 = input('Would you like to project the address data onto the street?(Y or N)')
    streetdf= streetdf.to_crs(epsg = 4326)
    streetdf.sort_values(by = 'fitted', ascending = False, inplace = True)
    streetdf.reset_index(inplace = True)
    ax = streetdf.plot(column = 'index', cmap = 'RdYlGn', alpha = 1)
    geometry = [Point(xy) for xy in zip(df.lon, df.lat)]
    df = df.drop(['lon', 'lat'], axis=1)
    crs = {'init': 'epsg:4326'}
    gdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)
    if choice2 == 'Y':
        print('Projecting Addresses to Street')
        gdf['geometry'] = gdf.apply(lambda x: project_to_line(x['lineid'], streetvolume, x['geometry']), axis = 1)
    gdf.plot(ax = ax, marker = "*", color='black', markersize=3);
    filename = (map_loc + 'ModelMap.html')
    try:
        os.remove(filename)
    except:
        pass
    mplleaflet.show(fig=ax.figure, crs=streetvolume.crs, tiles='cartodb_positron', path = filename)
    return




def main():
    """Main function to choose which extra you would like to do.

    Returns
    -------
    none

    """
    weekdaydict = {'Mon':1,'Tues':2, 'Wed':3, 'Thurs': 4, 'Fri': 5, 'Sat':6, 'Sun': 7}
    print("Preparing all neccesary datasets")
    address_data, streetvolume, nhoods, streetsweeping = load_data()
    by_route = create_routes()
    invalid_ids = False


    runagain = 'Y'
    while runagain == 'Y':
        choice = int(input('Which extra would you like to do? 1.Day animation 2.Recent Street Cleaning 3.Estimated Sweeping Time 4. Map the Route 5. Plot the generated model'))


        if choice ==  1:
            datestring = input('Which date would you like to make?(format %d-%m-%Y)')
            live_day_graph(datestring, address_data, streetvolume)

        elif choice == 2:
            number = int(input('Whats the number of the address?'))
            street = input('What is the full street (name + suffix)')
            resOT = input('Would you like to avoid residential overtime?(Y or N)')
            if resOT == 'Y':
                resOT = True
                invalid_ids =pd.read_sql_query('Select distinct lineid from address_data t1 join ticket_data t2 on '
                                              " t1.address = t2.address where ViolationDesc = 'RES/OT' ", conn)
            else:
                resOT = False
            find_recent_street_cleaning(streetsweeping, number, street, address_data, resOT, invalid_ids)



        elif choice == 3:
            number = int(input('Whats the number of the address?'))
            street = input('What is the full street (name + suffix)')
            return_conf_interval(number, street, by_route, address_data)


        elif choice == 4:
            weekday = input('What day of week would you like to look at?(Mon,Tues, Wed, Thurs, Fri, Sat, Sun)')
            if weekday in weekdaydict.keys():
                weekday = weekdaydict[weekday]
                map_the_route(weekday, by_route, streetvolume)
            else:
                print('invalid input')


        elif choice ==5:
            numticks = int(input('How many tickets would you like to plot?'))
            plot_model(numticks)

        else:
            runagain = input('Your entry was invalid, would you like to try again ?(Y or N)')


        runagain = input('Would you like to do another?')





if __name__== '__main__':
    main()
