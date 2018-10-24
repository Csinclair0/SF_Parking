#!/usr/bin/python3
import sqlite3
import pandas as pd
import datetime as dt
import glob
import time
import geopandas as gpd
import re
from geopy.geocoders import Nominatim
from tqdm import tqdm
import numpy as np
from sqlalchemy import create_engine
import math
from geopandas.tools import sjoin
import warnings

raw_folder = '/home/colin/Desktop/Parking_Project/data/raw/'
proc_folder = '/home/colin/Desktop/Parking_Project/data/processed/'


def create_db():
    """Function solely to create sqlite database"""
    disk_engine = create_engine('sqlite:///SF_Parking.db')
    conn = sqlite3.connect('SF_Parking.db')
    c = conn.cursor()

def create_raw_data():
    """
    Function to loop through all raw ticket text files and insert into sqlite database, unprocessed. The only alteration will be to rename to remove all columns to remove spaces.

    """
    start = dt.datetime.now()
    for csv_file in glob.glob(raw_folder + '/ticket_data/PRR_*'):
        index_start = 1
        j = 0
        print('{} file started at {}'.format(csv_file, dt.datetime.now()))
        df = pd.read_csv(csv_file, encoding = 'utf-8', parse_dates = ['Tick Issue Date'])
        df = df.rename(columns = {c: c.replace(' ', '') for c in df.columns})
        try:
            df.to_sql('raw_ticket_data', conn, if_exists='append')
        except:
            print('File read error')


        print ('{} file finished in {:03.2f} minutes '.format(csv_file, (dt.datetime.now()-start).seconds / 60))


def create_block_limits():
    """
    Use the SF Block Limit table to create a dictionary of dataframes for each street name. We can use this to bounce against when we have a combination that doesnt match the raw address records. We'll also create a dataframe of street intersections
    """
    columns_first = ['BlockStart', 'StreetName', 'Suffix']
    columns_second = ['BlockEnd', 'Cross1', 'Cross2', 'numbers']
    valid_suffix = ['ST', 'WY', 'DR', 'AV', 'LN', 'WAY', 'TER', 'PL', 'AVE']
    streetnamedict = {}

    with open(raw_folder + 'SF_Block_Limits_Table.txt') as f:
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
            #print(lines[idx+1])

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
    sqldf = streetintersections[['Number', 'Street']]



replacements = ['[^0-9a-zA-Z\s]', '^0+']
streetnums = {'1':'ST', '2': 'ND', '3': 'RD', '4': 'TH', '5': 'TH', '6': 'TH', '7': 'TH', '8': 'TH', '9': 'TH', '0': 'TH'}

def replace_street(street):
    """ This function is used to map numbered street names with their correct full name IE 3 into 03RD
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
    """ function to remove non number characters """
    if strnum != strnum or strnum == ' ':
        return -1
    else:
        strnum = re.sub('[^1-9]', '', str(strnum))
        return int(strnum)


valid_suffix = ['ST', 'WY', 'DR', 'AV', 'LN', 'WAY', 'TER', 'PL', 'BLVD', 'AVE']
def return_street(streetname):
    """ Some streets have names like 'AVENUE A' we dont want to strip out, only remove the last suffix if in the list. """
    if streetname == None:
        return streetname
    if streetname.split(" ")[-1] in valid_suffix:
        return " ".join(str(streetname).split(" ")[:-1])

    return streetname



def create_address_data():
    """
    Function to read address data, strip out apartment numbers, combine into a full address, and strip out the street name into its own column.

    """
    addresses = pd.read_csv('san_francisco_addresses.csv')
    addresses.columns = map(str.lower, addresses.columns)

    keepcolumns = ['lon', 'lat', 'number', 'street']
    addresses = addresses[keepcolumns]
    addresses['number'] = addresses['number'].apply(lambda x: re.findall( '\d+', x)[0]).astype(int)
    addresses['address'] = addresses.apply(lambda x: str(x['number']) + " " + str(x['street']), axis = 1)
    addresses['streetname'] = addresses['street'].apply(return_street)
    addresses.drop_duplicates(subset = 'address', inplace = True)
    addresses['type'] = 'known'
    addresses.to_sql('raw_address_data', if_exists = 'replace', con = conn)


def find_similar_address():
    """For any address that won't yield a match, must search for 'similar' addresses that are known that are on same block and streetname. Filter out all those it could not identify and save them into an unfound dataframe. Only include those we did, take the closest records coordinates, note we only drop on 'street' (street + suffix)
    and not streetname , so we'll keep both suffixes if valid"""

    df = result_query('Select distinct tickstreetno , tickstreetname , count(*) total_tickets from raw_ticket_data t1'
                      ' left join raw_address_data t2 on t1.TickStreetNo = t2.number and t1.TickStreetName = t2.streetname '
                      " where t2.address is null group by tickstreetno, tickstreetname ")

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


def return_intersections(streetname):
        """Sometimes they put an intersection instead of any coordinates, lets try to solve that
        function to return address of record that only put intersection"""
    if streetname != None and isinstance(streetname, str):
        if ' AND ' in streetname:
            streetnames = streetname.split(' AND ')
            df = streetintersections[(streetintersections.Street == streetnames[0]) \
                                            & (streetintersections.Isection == streetnames[1])]
            if df.shape[0] > 0:
                return str(int(df['Number'].iloc[0])) + ' ' + df['Street'].iloc[0] + ' ' + df['Suffix'].iloc[0]
    else:
        return None


def find_intersection_address():
    ""'Scrap out those we know are invalid, check if they are intersections'
    unfound = unfound[unfound.TickStreetNo < 10000]
    isection = unfound[['TickStreetNo','TickStreetName', 'total_tickets']]
    isection['address'] = isection['TickStreetName'].apply(return_intersections)
    unfound = isection[pd.isnull(isection.address) == True]
    isection = isection[pd.isnull(isection.address) == False]
    isection = isection.merge(addresses, left_on = 'address', right_on = 'address')
    isection = isection[['number', 'streetname', 'street', 'address', 'lat', 'lon']]
    isection.to_sql('raw_address_data', if_exists = 'append', con = conn)



def return_streetname_unknown( streetnum, streetname):
    """ use street dictionary of dataframes to look for suffix of any combination that is not found
    use most popular if more than one. """
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
                availablesuffix['delta'] = availablesuffix.apply(lambda x: return_closest(x['Min'], x['Max'], streetnum), axis = 1)
                validsuffix = availablesuffix.sort_values(by = 'delta', ascending = True)

            validsuffix = validsuffix.reset_index()

            suffix = str(validsuffix['Suffix'][0])


    if streetname == None:
        streetname = ''
    streetname = str(streetname)
    streetname += ' ' + suffix
    return streetname

def check_location(location):
    """Function to make sure reverse geocoded address is valid"""
    if location.latitude > 35 and location.latitude < 39 and location.longitude > -123 and location.longitude < -120:
        return True
    else:
        return False


geolocator = Nominatim(user_agent = 'SF_Parking_EDA')
def create_locs(address):
    """Function to look up address that we coudn't generate"""
    try:
        location = geolocator.geocode(address, timeout = 10)
    except:
        location = None
    time.sleep(1)

    if location != None and check_location(location):
        return (location.latitude, location.longitude )
    else:
        return None




def return_unknown_addresses():
    unfound.drop_duplicates(inplace = True)
    tqdm.pandas()
    unfound['street'] = unfound.apply(lambda x: return_streetname_unknown(x['TickStreetNo'], x['TickStreetName']), axis = 1)
    unfound['address'] = unfound.apply(lambda x: str(x['TickStreetNo']) + " " + str(x['street']), axis = 1)
    lookup = unfound.sort_values(by = 'total_tickets', ascending = False)[:500]
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


def add_nhoods():
    """Function to look up all addresses that have coordinates and associate a neighborhood with them. Use a shapely join to associate them. For any that don't have coordinates, we'll include them anyway without the neighborhoood identifier."""
    addresses = result_query('Select * from raw_address_data')
    addresses['geometry'] = addresses.apply(lambda x: Point(x['lon'], x['lat']), axis = 1)
    point = gpd.GeoDataFrame(addresses['geometry'])
    point.crs = {'init': 'epsg:4326'}
    poly  = gpd.GeoDataFrame.from_file(raw_folder + 'AnalysisNeighborhoods.geojson')
    pointInPolys = sjoin(point, poly, how='left')
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


def create_singles():
    """Function is to separate addresses into those that may have have more than one address associated with a ticket and street name combo. """
    grouped = addresses.groupby(by = ['number', 'streetname'], as_index = False)['address'].agg('count')
    grouped.sort_values(by = 'address', ascending = False)
    grouped.columns = ['number', 'streetname', 'count_ad']
    single_address = grouped[grouped.count_ad ==1]
    single_address = single_address.merge(addresses, left_on = ['number', 'streetname'], right_on = ['number', 'streetname'])
    double_address = addresses[addresses.address.isin(single_address['address']) == False]


def bernoulli(p):
    """Bernoulli random number generator"""
    if np.random.random() < p:
        return 0
    else:
        return 1



nhoodtype = result_query('Select nhood, violationdesc, count(*) tickets from raw_ticket_data t1 join single_address t2 '
                         ' on t1.TickStreetNo = t2.number and t1.TickStreetName = t2.streetname group by nhood, violationdesc')

def return_address(row):
    """ For anything we can't find using our merges, we use a ratio of total tickets for that violation description and neighborhood"""
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
    #add time to date
    try:
        timeadd = dt.datetime.strptime(row['TickIssueTime'], '%H:%M').time()
    except:
        timeadd = dt.datetime.strptime('00:00', '%H:%M').time()

    newtime = dt.datetime.combine(dt.datetime.strptime(row['TickIssueDate'], '%Y-%m-%d %H:%M:%S') , timeadd)
    return newtime


def return_time_delta(time):
    """return timedelta for comparison"""
    if time == None:
        time = [0,0]
    else:
        time = time.split(":")
    if len(time) < 2:
        time = [0,0]
    return dt.timedelta(hours = int(time[0]), minutes = int(time[1]))

def return_cost(coststring):
    """Strip out currency signs, ignore nulls, return number"""
    coststring = re.sub('[^1-9]', '', str(coststring))
    try:
        intreturn = int(coststring)
    except:
        intreturn = 0

    return intreturn


def process_ticket_data():
    """The final function that puts it all together. Process each column to give us clean data. Split out all problem records that could be associated with two different addresses. For those quesitonable records, we'll merge against the valid locations on badge and neighborhood, and then sort by time. Anything we can't merge on, we'll pass to our function. Well then replace the columns and insert into our processes ticket data table. This function is designed to process the total data set in chunks so it does not create large merges or long functions."""
    c = conn.cursor()
    c.execute('Select Count(*) from raw_ticket_data')
    totalleft = c.fetchone()[0]
    print('{} total rows required'.format(totalleft))
    np.random.seed(1)
    df_total = result_query('Select Ticketnumber, TickIssueDate, TickIssueTime, ViolationDesc, '
                  ' VehMake, TickRPPlate, TickStreetNo, TickMeter, Agency, TickBadgeIssued, '
                   'TickStreetName , TotalPaid, TotalAmtDue from raw_ticket_data ')
    columnlist = df_total.columns.tolist()
    df_total.sort_values(by = 'TickIssueDate', inplace = True)
    warnings.filterwarnings('ignore')
    n = 500000  #chunk row size
    totalsize = df_total.shape[0]
    indexes = [i for i in range(0,totalsize, n)]
    columnlist = df_total.columns.tolist()
    columnlist.append('address')
    tqdm.pandas()
    j = 1
    for i in indexes:
        df = df_total[i:i+n]
        print('Iteration {} started at {}. {} records left'.format(j, dt.datetime.now(), totalsize))
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

    print('Finished!')



def main():
    create_db()
    create_raw_data()
    create_block_limits()
    creat_address_data()
    find_similar_address()
    find_intersection_address()
    return_unknown_addresses()
    add_nhoods()
    create_singles()
    process_ticket_data()


if __name__== '__main__':
    print("NO")
    main()
