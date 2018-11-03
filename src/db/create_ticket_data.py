#!/usr/bin/python3
import os
import sqlite3
import pandas as pd
import datetime as dt
import glob
import time
import pickle
import re
from geopy.geocoders import Nominatim
from tqdm import tqdm
import numpy as np
from sqlalchemy import create_engine
import geopandas as gpd
from shapely.geometry import Point
import math
import warnings
warnings.filterwarnings('ignore')

raw_loc = '/home/colin/Desktop/SF_Parking/data/raw/'
proc_loc= '/home/colin/Desktop/SF_Parking/data/processed/'


def create_db():
    """This function will take no arguments and create a SQlite Database in the raw processed folder location.

    Returns
    ------- None
    type

    """

    try:
        os.remove(proc_loc + 'SF_Parking.db')
        print("Legacy DB deleted")
    except:
        pass
    disk_engine = create_engine('sqlite:///'+ proc_loc +'SF_Parking.db')
    return sqlite3.connect(proc_loc + 'SF_Parking.db')



def create_raw_data():
    """This function loops through the raw ticket files, and inserts them into the raw ticket table in the created SQL DB.

    Returns
    ------- None
    type


    """
    for csv_file in glob.glob(raw_loc + 'ticket_data/PRR_*'):
        filestring =os.path.basename(csv_file)
        index_start = 1
        j = 0
        start = dt.datetime.now()
        print('{} file started at {}'.format(filestring, start.strftime("%H:%M")))
        df = pd.read_csv(csv_file, encoding = 'utf-8', parse_dates = ['Tick Issue Date'])
        df = df.rename(columns = {c: c.replace(' ', '') for c in df.columns})
        try:
            df.to_sql('raw_ticket_data', con = conn, if_exists='append')
        except:
            print('File read error')


        print ('{} file finished in {:03.2f} minutes '.format(filestring, (dt.datetime.now()-start).seconds / 60))


def create_block_limits():
    """This function loops through the Block Limits table and creates two objects. These will become global variables as they will not be altered after this. Streetintersections is a dataframe that contains each intersection, and the beginning number of that block. Streetnamedict is a dictionary that contains a streetname, its suffix, and the minimum and maximum block limits of that combination.

    Returns
    ------- StreetIntersections , Streetnamedict
    type
        dataframe and dictionary

    """
    columns_first = ['BlockStart', 'StreetName', 'Suffix']
    columns_second = ['BlockEnd', 'Cross1', 'Cross2', 'numbers']
    valid_suffix = ['ST', 'WY', 'DR', 'AV', 'LN', 'WAY', 'TER', 'PL', 'AVE']

    streetnamedict = {}
    with open(raw_loc + 'SF_Block_Limits_Table.txt') as f:
        lines = [line.rstrip('\n') for line in f]

    streetintersections = pd.DataFrame(columns = ['Street', 'Suffix', 'Isection', 'Number'])

    streetnamedict = {}
    suffixnums = pd.DataFrame(columns = ['Suffix', 'Min', 'Max', 'Count'])
    suffixnumsdefault = pd.DataFrame(columns = ['Suffix', 'Min', 'Max', 'Count'])
    streetnamedict['NA'] = suffixnumsdefault
    for idx, line in enumerate(lines):
        rowsplit = line.split()
        if len(rowsplit) == 5 and rowsplit[3] in valid_suffix:
            suffix = rowsplit[3]
            streetname = rowsplit[2]
            minnum = int(rowsplit[1])
            maxnum = int(lines[idx+1].split()[1])
            suffixnums = streetnamedict.get(streetname, suffixnumsdefault)
            totalsuffix = pd.DataFrame(columns = ['Suffix', 'Min', 'Max', 'Count'])
            isections = str(lines[idx+1]).replace( '/', ' ').split()[3:5]
            dfrec1 = [streetname, streetname]
            dfrec2 = [suffix, suffix]
            dfrec3 = [isections[0], isections[1]]
            dfrec4 = [minnum, maxnum]
            newrecords = pd.DataFrame({'Street': dfrec1, 'Suffix': dfrec2, 'Isection': dfrec3, 'Number': dfrec4 })
            streetintersections = streetintersections.append(newrecords)

            if suffixnums.shape[0] > 0:
                suffixnumsame = suffixnums[suffixnums['Suffix'] == suffix]
                suffixnumother = suffixnums[suffixnums['Suffix'] != suffix]
                if suffixnumsame.shape[0] == 0:
                    suffixlist = [suffix, minnum, maxnum, 0]
                    suffixnumsame.loc[0] = [value for value in suffixlist]

                elif suffixnumsame.shape[0] == 1:
                    if suffixnumsame['Max'][0] < maxnum:
                        suffixnumsame['Max'][0] = maxnum

                    if suffixnumsame['Min'][0] > minnum:
                        suffixnumsame['Min'][0] = minnum

                    suffixnumsame['Count'][0] += 1

                totalsuffix = suffixnumother.append(suffixnumsame)

            else:

                suffixlist = [suffix, minnum, maxnum, 0]
                totalsuffix.loc[0] = [value for value in suffixlist]

            totalsuffix.reset_index()
            streetnamedict[streetname] = totalsuffix
    streetintersections.drop_duplicates(subset = ['Street', 'Isection'], inplace = True)
    return streetintersections, streetnamedict



replacements = ['[^0-9a-zA-Z\s]', '^0+']
streetnums = {'1':'ST', '2': 'ND', '3': 'RD', '4': 'TH', '5': 'TH', '6': 'TH', '7': 'TH', '8': 'TH', '9': 'TH', '0': 'TH'}
def replace_street(street):
    """ This function will
    1. remove any non alphanumeric characters
    2. return a new mapped street name if the street name is a number. It also padds a zero for numbers less than 10. IE turn 3 into 03RD.

    Parameters
    ----------
    street : string
        the string of the streetname to pass through

    Returns
    -------street
    type string


    """
    if isinstance(street, str):
        for rep in replacements:
            street = re.sub(rep, "", street)

    streetint = re.findall(r'\d+', str(street))
    if len(streetint) > 0 and int(streetint[0]) < 100:
        street = int(streetint[0])

        if street < 10:
            street = '0' + str(street) + str(streetnums[str(street)])
        elif street < 14:
            street = str(street) + 'TH'
        else:
            street = str(street) + str(streetnums[str(street)[-1]])


    return street


def return_num(strnum):
    """This function will take the street number, remove any non numeric characters, and replace it with a -1 if it is null. This will be so that we do not join it on any streets.

    Parameters
    ----------
    strnum : string
        Description of parameter `strnum`.

    Returns
    ------- strnum : int
    """
    if strnum != strnum or strnum == ' ':
        return -1
    else:
        strnum = re.sub('[^1-9]', '', str(strnum))
        return int(strnum)


valid_suffix = ['ST', 'WY', 'DR', 'AV', 'LN', 'WAY', 'TER', 'PL', 'BLVD', 'AVE']
def return_street(streetname):
    """ remove suffix from street and return only the 'street name'

    Parameters
    ----------
    streetname : string
        Full street, 'JONES ST'

    Returns
    -------
    string
        'JONES'

    """
    if streetname == None:
        return streetname
    if streetname.split(" ")[-1] in valid_suffix:
        return " ".join(str(streetname).split(" ")[:-1])

    return streetname



def return_intersections(streetname):
    """This function will take the streetname, and see if it is an intersection by checking if it contains the word ' AND'. It will then check our street intersections dataframe, and try to return a valid address.

    Parameters
    ----------
    streetname : string
        string value of ticket data record.

    Returns
    ------- streetname
    type
        new, valid, address.

    """
    if streetname != None and isinstance(streetname, str) and ' AND ' in streetname:
        streetnames = streetname.split(' AND ')
        df = streetintersections[(streetintersections.Street == streetnames[0]) \
                                        & (streetintersections.Isection == streetnames[1])]
        if df.shape[0] > 0:
            return str(int(df['Number'].iloc[0])) + ' ' + df['Street'].iloc[0] + ' ' + df['Suffix'].iloc[0]
    return None



def check_location(location):
    """Checks if location coordinates are within San francisco, this is to validate any addresses that we look up on openstreetmap.

    Parameters
    ----------
    location : tuple of coordinates
        These are the latitude and longitude returned from Open Street Map,

    Returns
    ------- Boolean which will dictate whether it can be entered.



    """
    if location.latitude > 35 and location.latitude < 39 and location.longitude > -123 and location.longitude < -120:
        return True
    else:
        return False



def create_locs(address):
    """Reverse Geocoding using Open Street Map.

    Parameters
    ----------
    address : string
        The combined street number, full street name, and ' SAN FRANCISCO CA'
        IE '980 BUSH STREET SAN FRANCISCO CA'

    Returns
    -------location
    tuple of coordinates, or nonetype


    """
    geolocator = Nominatim(user_agent = 'SF_Parking_EDA')
    try:
        location = geolocator.geocode(address, timeout = 10)
    except:
        location = None
    time.sleep(1)

    if location != None and check_location(location):
        return (location.latitude, location.longitude )
    else:
        return None




def return_streetname_unknown( streetnum, streetname):
    """This function uses the streetnamedict dictionary to try and find a valid suffix to append to the street name. It will return the first value it finds as valid. If it can;t find anything perfectly matching, it will take the closest block limit.

    Parameters
    ----------
    streetnum : integer
        Ticket street number value
    streetname : string
        Ticket street name value

    Returns
    ------- streetname
    type
        streetname plus a suffix at the end.

    """
    global availablesuffix
    global streetnamedict

    if streetnum != streetnum:
        strnum = ''
    else:
        strnum = str(int(streetnum) )

    streetname = replace_street(streetname)
    availablesuffix = streetnamedict.get(streetname, ' ')
    suffix = ''
    if isinstance(availablesuffix , pd.DataFrame):

        if availablesuffix.shape[0] == 1:
            suffix = str(availablesuffix['Suffix'][0])
        elif availablesuffix.shape[0] > 1:
            validsuffix = availablesuffix[(availablesuffix['Min'] < streetnum) & (availablesuffix['Max'] > streetnum)]
            if validsuffix.shape[0] > 0:
                validsuffix = validsuffix.sort_values(by = 'Count', ascending = False)
            else:
                availablesuffix['delta'] = availablesuffix.apply(lambda x: min(np.abs(x['Min'] - streetnum), np.abs(x['Max'] - streetnum)), axis = 1)
                validsuffix = availablesuffix.sort_values(by = 'delta', ascending = True)

            validsuffix = validsuffix.reset_index()

            suffix = str(validsuffix['Suffix'][0])


    if streetname == None:
        streetname = ''
    streetname = str(streetname)
    streetname += ' ' + suffix
    return streetname





def create_address_data():
    """This is the main function that creates the address data table. The process is as follows.

    1. Read Raw File, strip out apartment numbers, combine number and street to create an address, add column of 'streetname' which has suffix removed. Insert into raw address table.
    2. Find similar addresses by using known ones, searching for those that are on the same blockself.
    3. Anything left over will be searched through the intersections function, and then 500 of the top most occuring addresses will be rever geocoded using openstreetmap.


    Returns
    ------- single_address : DataFrame
    ------- double_address : DataFrame
    ------- addresses : DataFrane
    type
        dataframes separating addresses that could have one location or two, based on the street number and street name combination. also all addresses are returned

    """
    print("Reading address data file")
    addresses = pd.read_csv(raw_loc + 'san_francisco_addresses.csv')
    addresses.columns = map(str.lower, addresses.columns)

    keepcolumns = ['lon', 'lat', 'number', 'street']
    addresses = addresses[keepcolumns]
    addresses['number'] = addresses['number'].apply(lambda x: re.findall( '\d+', x)[0]).astype(int)
    addresses['address'] = addresses.apply(lambda x: str(x['number']) + " " + str(x['street']), axis = 1)
    addresses['streetname'] = addresses['street'].apply(return_street)
    addresses.drop_duplicates(subset = 'address', inplace = True)
    addresses['type'] = 'known'
    addresses.to_sql('raw_address_data', if_exists = 'replace', con = conn)


    print("Finding similar addresses")
    df = pd.read_sql_query('Select distinct tickstreetno , tickstreetname , count(*) total_tickets from raw_ticket_data t1'
                      ' left join raw_address_data t2 on t1.TickStreetNo = t2.number and t1.TickStreetName = t2.streetname '
                      " where t2.address is null group by tickstreetno, tickstreetname ", conn)

    df['TickStreetNo'] = df['TickStreetNo'].apply(return_num)
    df['TickStreetName'] = df['TickStreetName'].apply(replace_street)
    df['TickStreetName'] = df['TickStreetName'].apply(return_street)
    df['blocknum'] = df['TickStreetNo'].apply(lambda x: math.ceil(x/100))
    df.drop_duplicates(inplace = True)

    df2 = addresses
    df2['blocknum'] = df2['number'].apply(lambda x: math.ceil(x/100))
    newdf = df.merge(df2, how = 'left', left_on = ['TickStreetName', 'blocknum'], \
                 right_on = ['streetname', 'blocknum'])


    unfound = newdf[pd.isnull(newdf.number)]
    unfound['type'] == "unknown"
    newdf = newdf[pd.isnull(newdf.number) == False]
    newdf['delta'] = np.abs(newdf['number'] - newdf['TickStreetNo'])
    newdf.sort_values(by = 'delta', inplace = True)
    newdf.drop_duplicates(subset = ['TickStreetName', 'TickStreetNo'], keep = 'first', inplace = True)

    newdf = newdf[[ 'lon', 'lat', 'TickStreetNo', 'street', 'address','streetname' ]]
    newdf.columns = ['lon', 'lat', 'number', 'street', 'address','streetname' ]
    newdf['address'] = newdf['number'].map(str) + ' ' + newdf['street']
    newdf.drop_duplicates(inplace = True)
    newdf['type'] = 'similar'
    newdf.to_sql('raw_address_data', conn, if_exists = 'append')
    unfound = unfound[unfound.TickStreetNo < 10000]


    print("Searching for Intersection Addresses")
    #unfound = unfound[(unfound.TickStreetNo < 10000) & (unfound.TickStreetNo > 0)]
    isection = unfound[['TickStreetNo','TickStreetName', 'total_tickets']]
    isection['address'] = isection['TickStreetName'].apply(return_intersections)
    unfound = isection[pd.isnull(isection.address) == True]
    isection = isection[pd.isnull(isection.address) == False]
    isection = isection.merge(addresses, left_on = 'address', right_on = 'address')
    isection = isection[['number', 'streetname', 'street', 'address', 'lat', 'lon']]
    isection.to_sql('raw_address_data', if_exists = 'append', con = conn)



    print("Searching for Unknown Addresses")
    unfound.drop_duplicates(inplace = True)
    tqdm.pandas()
    unfound['street'] = unfound.apply(lambda x: return_streetname_unknown(x['TickStreetNo'], x['TickStreetName']), axis = 1)
    unfound['address'] = unfound.apply(lambda x: str(x['TickStreetNo']) + " " + str(x['street']), axis = 1)
    lookup = unfound.sort_values(by = 'total_tickets', ascending = False)[:500]                                             #CHANGE  TO 500
    lookup['coordinates'] = lookup['address'].progress_apply(lambda x: create_locs(x + ' SAN FRANCISCO CA'))
    lookup.dropna(subset = ['coordinates'], inplace = True)
    lookup['lat'] = lookup['coordinates'].apply(lambda x: x[0])
    lookup['lon'] = lookup['coordinates'].apply(lambda x: x[1])
    lookup.rename(columns = {'TickStreetNo':'number', 'TickStreetName':'streetname'}, inplace = True)
    lookup = lookup[['lat', 'lon', 'street', 'number', 'streetname', 'address']]
    unfound = unfound[unfound['address'].isin(lookup['address']) == False]
    unfound['type'] = 'unfound'
    lookup['type'] = 'searched'
    lookup.to_sql('raw_address_data', if_exists = 'append', con = conn)


    print("associating neighborhoods")
    addresses = pd.read_sql_query('Select * from raw_address_data', conn)
    addresses['geometry'] = addresses.apply(lambda x: Point(x['lon'], x['lat']), axis = 1)
    point = gpd.GeoDataFrame(addresses['geometry'])
    point.crs = {'init': 'epsg:4326'}
    poly  = gpd.GeoDataFrame.from_file(raw_loc+ 'AnalysisNeighborhoods.geojson')
    pointInPolys = gpd.tools.sjoin(point, poly, how='left')
    addresses['geometry'] = addresses['geometry'].astype(str)
    pointInPolys['geometry'] = pointInPolys['geometry'].astype(str)
    addresses = addresses.merge(pointInPolys, left_on = 'geometry', right_on = 'geometry')
    addresses.drop(columns = ['geometry', 'index', 'index_right'], inplace = True)
    addresses.drop_duplicates(subset = 'address', inplace = True)
    addresses['number'] = addresses['number'].astype(int)
    addresses.to_sql('address_data', conn, if_exists = 'replace')


    unfound.rename(columns = {'TickStreetNo':'number', 'TickStreetName': 'streetname'}, inplace = True)
    unfound.drop(columns = 'total_tickets', inplace = True)
    unfound['number'] = unfound['number'].astype(int)
    unfound.to_sql('address_data', if_exists = 'append', con = conn)



    """Function is to separate addresses into those that may have have more than one address associated with a ticket and street name combo. """
    grouped = addresses.groupby(by = ['number', 'streetname'], as_index = False)['address'].agg('count')
    grouped.sort_values(by = 'address', ascending = False)
    grouped.columns = ['number', 'streetname', 'count_ad']
    single_address = grouped[grouped.count_ad ==1]
    single_address = single_address.merge(addresses, left_on = ['number', 'streetname'], right_on = ['number', 'streetname'])
    double_address = addresses[addresses.address.isin(single_address['address']) == False]
    single_address.to_sql('single_address', conn, if_exists = 'replace')

    return single_address, double_address, addresses



def bernoulli(p):
    """bernoulli random number generator. takes probability and if less returns a 0.

    Parameters
    ----------
    p : float between 0 and 1


    Returns
    -------
     0 or 1

    """
    if np.random.random() < p:
        return 0
    else:
        return 1


def return_address(row):
    """Function is used for a 'double address' when we can't make a decision based on previous neighborhoods. First we'll use a couple rules we know. Then we'll use the neighborhood and ticket type to generate a probability of which address it was at. We'll pass that probability through the bernoulli probability.

    Parameters
    ----------
    row : dataframe row
        row of ticket dataframe that is being processed
    Returns
    -------
    address : string
        final address chosen

    """
    streetnum = row['TickStreetNo']
    streetname = row['TickStreetName']
    ticket_type = row['ViolationDesc']
    df = double_address[(double_address.number == streetnum) & (double_address.streetname == streetname)]
    if df.shape[0] > 1:
        if len(re.findall('\d+', streetname)) > 0:

            if ticket_type == 'RES/OT' and int(re.findall('\d+', streetname)[0]) > 15 and (streetnum < 2200 or streetnum > 2600):
                df_st = df[df.street.str.contains("ST")]
                if df_st.shape[0] == 1:
                    return str(int(streetnum)) + " " + df_st['street'].iloc[0]

            if ticket_type == 'RES/OT' and int(re.findall('\d+', streetname)[0]) > 21:
                df_st = df[df.street.str.contains("ST")]
                if df_st.shape[0] == 1:
                    return str(int(streetnum)) + " " + df_st['street'].iloc[0]

        df['ViolationDesc'] = ticket_type

        df_2 = df.merge(nhoodtype, left_on = ['nhood', 'ViolationDesc'], right_on = ['nhood', 'ViolationDesc'])

        if df_2.shape[0] > 0:
            totalcounts = df_2['tickets'].sum()
            topcount = df_2['tickets'].iloc[0]
            topchoice = bernoulli(float(topcount / totalcounts))
            return str(int(streetnum)) + " " + df_2['street'].iloc[topchoice]

        totalcounts = addresses[addresses.streetname == streetname].shape[0]
        topcount = addresses[addresses.streetname == streetname]['street'].value_counts().iloc[0]
        topchoice = bernoulli(float(topcount / totalcounts))
        return str(int(streetnum)) + " " + df['street'].iloc[topchoice]


def Time(row):
    """Combines date and time into one.

    Parameters
    ----------
    row : dataframe row
        row in ticket dataframe being processed

    Returns
    -------
    newtime : datetime
        time and date combined

    """
    try:
        timeadd = dt.datetime.strptime(row['TickIssueTime'], '%H:%M').time()
    except:
        timeadd = dt.datetime.strptime('00:00', '%H:%M').time()

    newtime = dt.datetime.combine(dt.datetime.strptime(row['TickIssueDate'], '%Y-%m-%d %H:%M:%S') , timeadd)
    return newtime


def return_time_delta(time):
    """Returns a timedelta object from the given time in H:M

    Parameters
    ----------
    time : string
        string of time (%H:%M)

    Returns
    -------
    timedelta
        timedelta object contianing hours and minutes

    """
    if time == None:
        time = [0,0]
    else:
        time = time.split(":")
    if len(time) < 2:
        time = [0,0]
    return dt.timedelta(hours = int(time[0]), minutes = int(time[1]))



def return_cost(coststring):
    """Strips out currency from amount, returns 0 if nothing there.

    Parameters
    ----------
    coststring : string
        string of value for either amoutn owed or paid '$98'

    Returns
    -------
    integer
        integer of cost

    """
    coststring = re.sub('[^1-9]', '', str(coststring))
    try:
        intreturn = int(coststring)
    except:
        intreturn = 0

    return intreturn


def process_ticket_data():
    """The final function that puts it all together. Process each column to give us clean data. Split out all problem records that could be associated with two different addresses. For those quesitonable records, we'll merge against the valid locations on badge and neighborhood, and then sort by time. Anything we can't merge on, we'll pass to our function. Well then replace the columns and insert into our processes ticket data table. This function is designed to process the total data set in chunks so it does not create large merges or long functions.

    Parameters
    ----------
    addresses : DataFrame
        dataframe of all addresses

    Returns
    -------
    none, finished database is created.

    """
    c = conn.cursor()
    c.execute('Select Count(*) from raw_ticket_data')
    totalleft = c.fetchone()[0]
    print('{} total rows required'.format(totalleft))
    np.random.seed(1)
    df_total = pd.read_sql_query('Select Ticketnumber, TickIssueDate, TickIssueTime, ViolationDesc, '
                  ' VehMake, TickRPPlate, TickStreetNo, TickMeter, Agency, TickBadgeIssued, '
                   'TickStreetName , TotalPaid, TotalAmtDue from raw_ticket_data ', conn)
    columnlist = df_total.columns.tolist()
    df_total.sort_values(by = 'TickIssueDate', inplace = True)
    n = 500000
    totalsize = df_total.shape[0]
    indexes = [i for i in range(0,totalsize, n)]
    columnlist = df_total.columns.tolist()
    columnlist.append('address')
    tqdm.pandas()
    j = 1
    for i in indexes:
        df = df_total[i:i+n]
        print('Iteration {} started at {}. {} records left'.format(j, dt.datetime.now().strftime("%H:%M"), totalsize))
        df['TickStreetNo'] = df['TickStreetNo'].apply(return_num)
        df['ViolationDesc'] = df['ViolationDesc'].apply(lambda x: x.replace('METER DTN','MTR OUT DT'))
        df['TickStreetName'] = df['TickStreetName'].apply(replace_street)
        df['TickStreetName'] = df['TickStreetName'].apply(return_street)
        df['TotalPaid'] = df['TotalPaid'].apply(return_cost)
        df['TotalAmtDue'] = df['TotalAmtDue'].apply(lambda x: re.sub('[^1-9]', '', str(x)))
        df['TickRPPlate'] = df['TickRPPlate'].apply(lambda x: 'None' if len(re.findall('[\w+]', str(x))) == 0 else str(x).replace('[^\w+]', ''))
        df['Tdelt'] = df['TickIssueTime'].apply(return_time_delta)

        df_1 = df.merge(single_address, left_on = ['TickStreetNo', 'TickStreetName'], right_on = ['number', 'streetname'])
        df_2 = df.merge(double_address, left_on = ['TickStreetNo', 'TickStreetName'], right_on = ['number', 'streetname'])

        df_2 = df_2.merge(df_1, how = 'left', left_on = ['TickIssueDate', 'TickBadgeIssued', 'nhood'], right_on = ['TickIssueDate', 'TickBadgeIssued', 'nhood'])
        df_3 = df_2[pd.isnull(df_2['Tdelt_y'])]
        df_2.dropna(subset = ['Tdelt_y'], inplace = True)
        df_2['timedelta'] = df_2.apply(lambda x: np.abs(x['Tdelt_y'] - x['Tdelt_x']), axis = 1)
        df_2.sort_values(by = 'timedelta', inplace = True)

        df_2.columns = [col.replace('_x', '') for col in df_2.columns]
        df_3.columns = [col.replace('_x', '') for col in df_3.columns]
        df_2.drop_duplicates(subset = 'TicketNumber', inplace = True)
        print("Searching for unmatchable addresses")
        df_3['address'] = df_3.progress_apply(return_address, axis = 1)

        df = df_1.append(df_2)
        df = df.append(df_3)
        df['TickIssueDate'] = df.apply(Time, axis = 1)
        df = df[columnlist]

        if i == 0:
            df.to_sql('ticket_data', if_exists = 'replace',con = conn)
        else:
            df.to_sql('ticket_data', if_exists = 'append',con = conn)

        totalsize -= n
        j+=1

    del c

    return


def find_closest_segment(LineString, street, streetvolume):
    """Function to look for closest linestring
    Filter on same streetname, if none found then search all streets
    cnn is effectively the street link if for street cleaning, so we only need to look up that and then join it with all others.

    Parameters
    ----------
    LineString : Shapely LineString
        Street Sweeping line segment
    street : string
        name of street without suffix 'JONES'
    streetvolume : type
        streetvolume dataframe

    Returns
    -------
    integer
        lineid of streetvolume dataframe

    """
    streetdf = streetvolume[streetvolume['streetname'] == street]
    if streetdf.shape[0] == 0:
        streetdf = streetvolume
    streetdf['distanceTo'] = streetdf['geometry'].apply(lambda x: LineString.distance(x))
    streetdf.sort_values(by = 'distanceTo', ascending = True, inplace = True)
    return streetdf['lineid'].iloc[0]



def find_closest_point(point, street, streetvolume):
    """This will use shapely's functions to identify which lineid an address should be assigned to, when we couldnt find it by merging the street sweeping.

    Parameters
    ----------
    point : shapely point
        coordinates of address
    street : shapely street
        streetname of address
    streetvolume : geopandas dataframe

    Returns
    -------
    int
        lineid of returned segment

    """
    streetdf = streetvolume[streetvolume['streetname'] == street]
    if streetdf.shape[0] == 0:
        streetdf = streetvolume
    streetdf['pdistance'] = streetdf['geometry'].apply(lambda x: point.distance(x))
    streetdf.sort_values(by = 'pdistance', ascending = True, inplace = True)
    return streetdf['lineid'].iloc[0]


def process_volume():
    """This function will load the street volume shapefile, put it into the correct coordinate system, remove duplicates, create a new column to be used as the line id, and then insert it into our SQL database as well as save it as a new shapefile. We'll then load the street sweeping file, use a shapely join and filtering to pair it with a street volume id, and search for any lines that didnt't find a match, by using the 'find closest point' function.

    Returns
    -------
    geopandas dataframes
        streetsweeping and street volume dataframes

    """
    print('processing street volume file')
    streetvolume = gpd.read_file(raw_loc + '/street_volume/LOADALL_FINAL.shp')
    streetvolume.crs = {'init' : 'epsg:2227'}
    streetvolume = streetvolume.to_crs(epsg = 4326)
    streetvolume = streetvolume[streetvolume['MTYPE'] == 'SF']
    streetvolume.columns = streetvolume.columns.str.lower()
    streetvolume = streetvolume[(streetvolume['distance'] <= 7) & (pd.isnull(streetvolume.streetname) == False)]
    times = ['am', 'pm', 'ev', 'ea']
    columnlist = ['v_over_cea', 'distance', 'streetname', 'geometry', 'vvol_carea', 'vvol_trkea', 'vvol_busea', 'speed_ea', 'oneway', 'speed', 'bike_class', 'time_ea']
    for i in np.arange(4):
        streetvolume['total_' + times[i]] = streetvolume['vvol_car' +times[i]] + streetvolume['vvol_trk' + times[i]] + streetvolume['vvol_bus' + times[i]]
        columnlist.append('total_' + times[i])

    streetvolume = streetvolume[columnlist]
    streetvolume.reset_index(inplace = True)
    streetvolume.rename(columns = {'index' : 'lineid'}, inplace = True)
    streetvolume.to_file(proc_loc + 'final_streets/SF_Street_Data.shp')


    print('joining street sweeping file')
    streetsweeping = gpd.read_file(raw_loc + '/street_sweeping/Street_Sweeping.shp')
    streetsweeping['streetname'] = streetsweeping['streetname'].apply(return_street)
    streetsweeping['totalpermonth'] = 0
    streetsweeping = streetsweeping[streetsweeping.weekday != 'Holiday']
    streetsweeping.drop_duplicates(subset = ['cnn', 'blockside', 'weekday'], inplace = True)
    for i in np.arange(1,6):
        colname = 'week' + str(i) + 'ofmon'
        streetsweeping[colname] = streetsweeping[colname].apply(lambda x: 1 if x == 'Y' else 0 )
        streetsweeping['totalpermonth'] += streetsweeping[colname]




    df = streetsweeping.groupby(by = ['cnn', 'blockside', 'weekday'])['week1ofmon', 'week2ofmon', 'week3ofmon', 'week4ofmon', 'week5ofmon', 'totalpermonth'].sum()
    for i in np.arange(1,6):
        colname = 'week' + str(i) + 'ofmon'
        df[colname] = df[colname].apply(lambda x: 1 if x >=1 else 0 )
    streetsweeping.drop(columns = ['totalpermonth', 'week1ofmon', 'week2ofmon', 'week3ofmon', 'week4ofmon', 'week5ofmon'] , inplace = True)
    streetsweeping = streetsweeping.merge(df, left_on = ['cnn', 'blockside', 'weekday'], right_on = ['cnn', 'blockside', 'weekday'])
    streetvolume_j = streetvolume[['lineid', 'geometry', 'streetname', 'total_ea']]
    streetsweeping = gpd.tools.sjoin(streetsweeping, streetvolume_j, how='left')



    streetsweeping.drop(columns = 'index_right', inplace = True)
    unfound = streetsweeping[pd.isnull(streetsweeping.lineid) | (streetsweeping.streetname_left != streetsweeping.streetname_right)]
    streetsweeping= streetsweeping[streetsweeping.streetname_left == streetsweeping.streetname_right]
    streetsweeping.drop(columns = ['streetname_right'], inplace = True)
    streetsweeping.rename(columns = {'streetname_left':'streetname'}, inplace = True)
    unfound.rename(columns = {'streetname_left':'streetname'}, inplace = True)
    unfound.drop(columns = ['streetname_right'], inplace = True)
    subset = [column for column in streetsweeping.columns if column not in ['geometry', 'lineid']]
    streetsweeping.drop_duplicates(subset = subset, inplace = True)


    print('matching unfound street sweeping links')
    unfound_cnn = unfound[['cnn', 'geometry', 'streetname']]
    unfound_cnn.drop_duplicates(subset =['cnn'], inplace = True)
    tqdm.pandas()
    unfound.drop(columns = 'lineid', inplace = True)

    dfstreets = streetvolume.copy()
    unfound_cnn['lineid'] = unfound_cnn.progress_apply(lambda x: find_closest_segment(x['geometry'], x['streetname'], dfstreets), axis = 1)
    unfound_cnn = unfound_cnn[['cnn', 'lineid']]
    df = unfound.merge(unfound_cnn, left_on = 'cnn', right_on = 'cnn')

    print("Storing Data to SQL")
    streetsweeping = streetsweeping.append(df)
    streetsweeping.to_file(proc_loc + 'final_sweeping/final_sweeping.shp')
    sqldf_sweep = streetsweeping.drop(columns = ['geometry', 'geometry'])
    sqldf_sweep.to_sql('street_sweep_data', con = conn, if_exists = 'replace')
    return streetsweeping, streetvolume



def pair_address(streetsweeping, streetvolume):
    """Merge all addresses with a street cleaning link, since we can filter on their numbers. Then we can assign it to a street volume link. For any that we can't directly find, we'll us our function to locate the one closest, using coordinates and shortest distance.

    Parameters
    ----------
    streetsweeping : geopandas dataframe
        the streetsweeping dataframe

    Returns
    -------
    none

    """
    addresses = pd.read_sql_query('Select * from address_data', conn)
    streetsweeping['corridor'] = streetsweeping['corridor'].apply(lambda x: x.upper())
    addresses['blocknum'] = addresses['number'].apply(lambda x: math.floor(int(x) / 100))
    streetsweeping['blocknum'] = streetsweeping['lf_fadd'].apply(lambda x: math.floor(int(x) / 100))
    streetsweeping = streetsweeping[['corridor', 'blocknum', 'lineid']]
    addresses = addresses.merge(streetsweeping,  how = 'left', left_on = ['street', 'blocknum'], right_on = ['corridor', 'blocknum'])

    addresses.drop_duplicates(subset = ['address'], inplace = True)
    unfound = addresses[pd.isnull(addresses.lineid)]
    unfound.dropna(subset = ['lat', 'lon' ], inplace = True)
    addresses.dropna(subset = ['lineid'], inplace = True)
    addresses.drop(columns = ['blocknum', 'index', 'corridor'], inplace = True)

    geometry = [Point(xy) for xy in zip(unfound.lon, unfound.lat)]
    crs = {'init': 'epsg:4326'}
    gdf = gpd.GeoDataFrame(unfound, crs=crs, geometry=geometry)
    addresses = addresses[['lon', 'lat', 'number', 'street', 'address', 'streetname', 'nhood', 'lineid']]
    tqdm.pandas()
    dfstreets = streetvolume.copy()
    gdf['lineid'] = gdf.progress_apply(lambda x: find_closest_point(x['geometry'], x['street'], dfstreets), axis = 1)
    addresses = addresses.append(gdf)
    addresses = addresses[['address', 'lat', 'lon', 'lineid', 'nhood', 'number', 'street', 'streetname']]
    addresses.to_sql('address_data', conn, if_exists = 'replace')
    return


def pair_parking(streetvolume):
    """Similar to pairing street cleaning with volume, but pairing the street parking availability. Except this time, we will simply append the parking supply data to the street volume and re-insert it into our db.

    Parameters
    ----------
    streetvolume : geopandas dataframe
        street volume dataframe created up to this point

    Returns
    -------
    none
        stored in SQL
    """
    spaces = gpd.read_file(raw_loc + '/onstreet_parking/Sfpark_OnStreetParkingCensus_201404.shp')
    spaces.crs = {'init': 'epsg:2227'}
    spaces = spaces.to_crs(epsg =4326)
    spaces = spaces[spaces.PRKNG_SPLY < 1000]
    spaces.sort_values(by = 'PRKNG_SPLY', ascending = False, inplace = True)
    spaces = spaces[['geometry', 'PRKNG_SPLY', 'ST_NAME']]
    spaces.rename(columns = {'PRKNG_SPLY':'park_supply'}, inplace = True)
    total_join = gpd.tools.sjoin(streetvolume, spaces, how= 'left')
    total_join ['park_supply']= total_join.apply(lambda x: 0 if x['streetname'] != x['ST_NAME'] else x['park_supply'], axis = 1)
    total_join.sort_values(by = 'park_supply', ascending = False, inplace = True)
    total_join.drop_duplicates(subset = ['lineid'], inplace = True)
    total_join.to_file(proc_loc+ '/final_streets/SF_Street_Data.shp')
    total_join.drop(columns = ['index_right', 'geometry', 'ST_NAME'], inplace = True)
    total_join.to_sql('street_volume_data', conn, if_exists = 'replace')

    return




def main():
    """main function to accomplish all procedures in order.
    1. Create database
    2. Insert raw ticket database
    3. Create block Limits
    4. create address data
    5. Process all ticket data

    Returns
    -------
    none
        finished data creation.

    """
    print("Initializing Database")
    global conn
    conn = create_db()

    print("Inserting Raw Ticket Data")
    create_raw_data()
    print("Creating Block Limits DataFrame")

    global streetnamedict
    global streetintersections
    streetintersections, streetnamedict = create_block_limits()

    print("Starting Address Creation")
    global single_address
    global double_address
    global addresses
    single_address, double_address, addresses = create_address_data()
    print("Finished Creating addresses")

    global nhoodtype
    nhoodtype = pd.read_sql_query('Select nhood, violationdesc, count(*) tickets from raw_ticket_data t1 join single_address t2 '
                            ' on t1.TickStreetNo = t2.number and t1.TickStreetName = t2.streetname group by nhood, violationdesc', conn)

    print("Processing Ticket Data")
    process_ticket_data()
    print('Finished processing Ticket Data!')
    print('Starting Street Data Creation')
    streetsweeping, streetvolume = process_volume()
    print(' Finished Street Data Creation')

    print('Pairing Addresses with Street Data')
    pair_address(streetsweeping, streetvolume)
    print('Finished Pairing Addresses')

    print('Pairing Parking')
    pair_parking(streetvolume)
    print('Finished Pairing Parking')

    conn.close()

    print('Finished Creating Entire Database and updated Shapefiles at {}'.format( dt.datetime.now().strftime("%H:%M")))

if __name__== '__main__':
    main()
