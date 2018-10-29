#!/usr/bin/python3
from shapely.geometry import Point
import geopandas as gpd
import sqlite3
import pandas as pd
import re
from tqdm import tqdm
import numpy as np
import matplotlib.pyplot as plt
import math
import warnings
warnings.filterwarnings('ignore')

raw_loc = '/home/colin/Desktop/SF_Parking/data/raw/'
proc_loc = '/home/colin/Desktop/SF_Parking/data/processed/'

global conn
conn = sqlite3.connect(proc_loc + 'SF_Parking.db')

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
        else:
            street = str(street) + str(streetnums[str(street)[-1]])


    return street



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
    if streetname.split(" ")[-1] in valid_suffix:
        return" ".join(str(streetname).split(" ")[:-1])

    return streetname


def find_closest_segment(LineString, street, streetvolume):
    """Function to look for closest linestring
    Filter on same streetname, if none found then search all streets
    cnn is effectively the street link if for street cleaning, so we only need to look up that and then join it with all others.

    Parameters
    ----------def find_closest_segment(LineString, street):
    streetdf = streetvolume[streetvolume['streetname'] == street]
    if streetdf.shape[0] == 0:
        streetdf = streetvolume
    streetdf['distance'] = streetdf['geometry'].apply(lambda x: LineString.distance(x))
    streetdf.sort_values(by = 'distance', ascending = True, inplace = True)
    return streetdf['lineid'].iloc[0]
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
    streetdf['distance'] = streetdf['geometry'].apply(lambda x: LineString.distance(x))
    streetdf.sort_values(by = 'distance', ascending = True, inplace = True)
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
    streetvolume.to_file(proc_loc + '/final_streets/SF_Street_Data.shp')


    print('joining street sweeping file')
    streetsweeping = gpd.read_file(raw_loc + '/street_sweeping/Street_Sweeping.shp')
    streetsweeping['streetname'] = streetsweeping['streetname'].apply(return_street)
    streetsweeping['totalpermonth'] = 0
    for i in np.arange(1,6):
        colname = 'week' + str(i) + 'ofmon'
        streetsweeping[colname] = streetsweeping[colname].apply(lambda x: 1 if x == 'Y' else 0 )
        streetsweeping['totalpermonth'] += streetsweeping[colname]




    df = streetsweeping.groupby(by = ['cnn', 'blockside', 'weekday'])['week1ofmon', 'week2ofmon', 'week3ofmon', 'week4ofmon', 'week5ofmon', 'totalpermonth'].sum()
    streetsweeping.drop_duplicates(subset = ['cnn', 'blockside', 'weekday'])
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

    unfound_cnn['lineid'] = unfound_cnn.progress_apply(lambda x: find_closest_segment(x['geometry'], x['streetname'], streetvolume), axis = 1)
    unfound_cnn = unfound_cnn[['cnn', 'lineid']]
    df = unfound.merge(unfound_cnn, left_on = 'cnn', right_on = 'cnn')

    print("Storing Data to SQL")
    streetsweeping = streetsweeping.append(df)
    streetsweeping.to_file(proc_loc + '/final_sweeping/final_sweeping.shp')
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
    gdf['lineid'] = gdf.progress_apply(lambda x: find_closest_point(x['geometry'], x['street'], streetvolume), axis = 1)
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
    total_join.sort_values(by = 'park_supply', inplace = True)
    total_join.drop_duplicates(subset = ['lineid'], inplace = True)
    total_join.to_file(proc_loc+ '/final_streets/SF_Street_Data.shp')
    total_join.drop(columns = ['index_right', 'geometry'], inplace = True)
    total_join.to_sql('street_volume_data', conn, if_exists = 'replace')

    return


def main():
    """Main function to run through process.
    1. create streetsweeping and streetvolume dataframes
    2. pair addresses
    3. pair parking
    Returns
    -------
    none
            finished script

    """
    print('Starting Street Data Creation')
    streetsweeping, streetvolume = process_volume()
    print(' Finished Street Data Creation')

    print('Pairing Addresses with Street Data')
    pair_address(streetsweeping, streetvolume)
    print('Finished Pairing Addresses')

    print('Parking Parking')
    pair_parking(streetvolume)
    print('Finished Pairing Parking')
    conn.close()
    print('Finished Creating Entire Database and Shapefiles at {}'.format( start.strftime("%H:%M")))


if __name__== '__main__':
    main()
