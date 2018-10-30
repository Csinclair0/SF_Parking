#!/usr/bin/python3
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3

raw_loc = '/home/colin/Desktop/SF_Parking/data/raw/'
proc_loc = '/home/colin/Desktop/SF_Parking/data/processed/'
image_loc= '/home/colin/Desktop/SF_Parking/reports/figures/analysis/model/'


mpl.rcParams['savefig.bbox'] = 'tight'
mpl.rcParams['figure.autolayout'] = True
mpl.rc('xtick', labelsize = 8 )

global conn
conn = sqlite3.connect(raw_loc + 'SF_Parking.db')


def load_data()
    sweep = pd.read_sql_query('Select t1.lineid, fromhour, tohour, weekday, totalpermonth,  distance, nhood, park_supply from street_sweep_data t1 '
                  'join street_volume_data t2 on t1.lineid = t2.lineid', conn)
    ticks = pd.read_sql_query("Select TicketNumber, TickIssueDate, TickIssueTime, lineid from ticket_data t1 join address_data t2 "
                         " on t1.address = t2.address where ViolationDesc = 'STR CLEAN'", conn)
    weekdaydict = {0: 'Mon', 1:'Tues', 2:'Wed', 3:'Thu', 4:'Fri', 5:'Sat', 6:'Sun'}

    ticks['weekday'] = ticks['TickIssueDate'].apply(lambda x: weekdaydict[pd.to_datetime(x).weekday()])

    sweep.drop_duplicates(subset = ['lineid', 'weekday'], inplace = True)

    byclean = ticks.groupby(by = ['lineid', 'weekday'], as_index = False)['TicketNumber'].agg('count')

    ticks = byclean.merge(sweep, left_on = ['lineid', 'weekday'], right_on = ['lineid', 'weekday'])
    ticks.dropna(subset= ['park_supply'], inplace = True)
    ticks = ticks[ticks.park_supply > 0]
    ticks['TicketNumber'] = ticks['TicketNumber'] / totalyears

    ticks['miles_swept_year'] = ticks['totalpermonth'] * 12 * ticks['distance']

    ticks['success_rate'] = ticks['TicketNumber'] / ticks['miles_sweeped_year']

    return ticks


def tick_per_month(ticks):
    ticks.groupby(by = 'totalpermonth')['success_rate'].mean().plot(kind = 'bar')
    plt.xlabel('Total Sweeps per Month')
    plt.ylabel('Tickets per Mile Swept')
    plt.title('Average Tickets per Mile swept number of sweeps per month')
    plt.show()
    plt.savefig(imageloc + 'TicksbySweep.png')
    return

def sweep_per_month(ticks):
    by_street = ticks.groupby('lineid')[['totalpermonth', 'distance', 'TicketNumber']].sum()
    by_street['miles_sweeped_year'] = by_street['totalpermonth'] * 12 * by_street['distance']
    by_street['success_rate'] = by_street['TicketNumber'] / by_street['miles_sweeped_year']
    by_street.groupby('totalpermonth')['success_rate'].mean().plot(kind = 'bar')
    plt.xlabel('Total Sweeps per Month')
    plt.ylabel('Sweep Success Rate')
    plt.show()
    plt.savefig(imageloc + 'SweepsPerMonth.png')
    return

def sweep_by_hour(ticks):
    by_hour = ticks.groupby(by = 'fromhour')['success_rate'].mean().plot(kind = 'bar')
    plt.title('Average Tickets per Mile Swept by Hour of Day Start')

def by_day_of_week(ticks):
    plt.figure(figsize = (10,6))
    daydict = {'Mon':1, 'Tues':2, 'Wed' :3, 'Thu': 4, 'Fri': 5, 'Sat': 6, 'Sun': 7}
    df = ticks.groupby(by = 'weekday',as_index = False)['success_rate'].mean()
    df['daynum'] = df['weekday'].map(daydict)
    df.sort_values(by = 'daynum', inplace = True)
    df.drop(columns = 'daynum', inplace = True)
    plt.bar(x = df['weekday'],height = df['success_rate'])
    plt.title('Average Tickets per Mile swept by Day of Week')


def main():
    ticks = load_data()
    tick_per_month(ticks)
    sweep_per_month(ticks)
    sweep_by_hour(ticks)







if __name__== '__main__':
    main()
