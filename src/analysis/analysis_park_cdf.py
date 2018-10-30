#!/usr/bin/python3
from scipy.stats import expon
import numpy as np
import pandas as pd
from scipy import stats
import matplotlib.pyplot as plt
import matplotlib as mpl
import sqlite3
import math
import datetime as dt
global conn
import pickle




raw_loc = '/home/colin/Desktop/SF_Parking/data/raw/'
proc_loc = '/home/colin/Desktop/SF_Parking/data/processed/'
image_loc= '/home/colin/Desktop/SF_Parking/reports/figures/analysis/model/'


mpl.rcParams['savefig.bbox'] = 'tight'
mpl.rcParams['figure.autolayout'] = True
mpl.rc('xtick', labelsize = 8 )

global conn
conn = sqlite3.connect(raw_loc + 'SF_Parking.db')
c = conn.cursor()
def delta_minutes(x,y):
    timedelta = y-x

    return timedelta.seconds / 60


def load_data():
    streetdata = pd.read_sql_query('Select lineid, distance, park_supply, speed_ea from street_volume_data', conn)

    ticket_data = pd.read_sql_query("Select * from ticket_data where ViolationDesc = 'RES/OT' ", conn)

    address_data = pd.read_sql_query('Select address, lineid from address_data', conn)

    df = ticket_data.merge(address_data, left_on = 'address', right_on = 'address')
    df = df.merge(streetdata, left_on = 'lineid', right_on = 'lineid')
    return df, streetdata

def create_initial_arrival_prob(df, streetdata):
    c.execute("Select Count(distinct lineid) from ticket_data t1 join address_data t2 on t1.address = t2.address")
    totalticks = c.fetchone()[0]
    c.execute("Select Count(distinct lineid) from ticket_data t1 join address_data t2 on t1.address = t2.address where violationdesc = 'RES/OT'")
    resticks = c.fetchone()[0]
    percent_res = resticks/totalticks

    df['TickDate']= df['TickIssueDate'].apply(lambda x: pd.to_datetime(x).date())
    tix_by_officer = df.groupby(by = ['TickDate','TickBadgeIssued'], as_index = False)['TicketNumber'].size().reset_index(name='counts')

    officer_by_day = tix_by_officer.groupby(by = ['TickDate'], as_index = False).size().reset_index(name='counts')
    avg_officers = officer_by_day['counts'].mean()
    avg_tix = tix_by_officer['counts'].mean()

    print("We're going to need some assumptions")
    choice = float(input('What would you like assume their freeflow speed?(as % of full traffic speed, default 50)')) / 100


    average_freeflow_speed = df['speed_ea'].mean() * choice
    choice = float(input('What would you like assume their total utilization is?(as % of total time, default 75)')) / 100

    validstreet = streetdata[streetdata.park_supply > 0 ]

    average_spots_per_mile = validstreet['park_supply'] / validstreet['distance']
    average_spots_per_mile = average_spots_per_mile.mean()
    total_spots_per_day = average_freeflow_speed * (6-avg_tix*2/60) * percent_res * average_spots_per_mile * choice
    mean_parking_spots = df['park_supply'].mean()
    total_spots = mean_parking_spots * df['lineid'].nunique()
    total_spots_checked = total_spots_per_day * avg_officers
    average_checks = total_spots_checked / total_spots
    arrival_rate = 10*60 / average_checks

    x = np.linspace(0,400)
    ax = plt.figure()
    prob = stats.expon.cdf(x=x, scale= arrival_rate)
    plt.plot(x, prob, color = "blue", linewidth = 3)
    plt.xlabel('Time(minutes)')
    plt.title("Probability of a parking enforcement officer passing your car initially")
    plt.xlim(0,400)
    plt.show()

    return arrival_rate



def create_return_distribution(df):
    df['TickDate'] = df['TickIssueDate'].apply(lambda x:  dt.datetime.strftime(pd.to_datetime(x),'%Y-%m-%d'))

    df = df[['TickBadgeIssued', 'TickIssueDate', 'TicketNumber', 'TickIssueTime', 'lineid', 'TickDate']]

    df = df.merge(df, left_on = ['TickDate', 'lineid', 'TickBadgeIssued'], right_on = ['TickDate', 'lineid', 'TickBadgeIssued'])


    df= df[(df.TicketNumber_x != df.TicketNumber_y) & (df.TickIssueTime_y > df.TickIssueTime_x)]
    df['delta'] = df.apply(lambda x: delta_minutes(pd.to_datetime(x['TickIssueDate_x']), pd.to_datetime(x['TickIssueDate_y'])), axis = 1)

    df = df[(df.delta > 120) & (df.delta < 180)]
    df['delta'].hist(bins = 'auto')
    plt.xlabel('Time from Initial Ticket')
    plt.ylabel('Frequency')
    plt.show()

    counts, bin_edges = np.histogram(df['delta'], bins = 'auto', normed = True)

    cdf = np.cumsum(counts)
    plt.figure(figsize = (10,6))
    plt.plot(bin_edges[1:], cdf/cdf[-1])
    plt.xlabel('Time after initial marking')
    plt.ylabel('Probability')
    plt.title('CDF of return probability after initial marking')
    plt.show()


    values = df['delta']
    probs = 1/ df['delta'].shape[0]
    combination = pd.DataFrame({'val': values, 'probs' : probs})
    df = combination.groupby(by = 'val', as_index = False)['probs'].sum()
    custom = stats.rv_discrete(values = (df['val'], df['probs']))

    return custom

def f(x, arrival):
    return -math.log(1.0 - x) / (1/arrival)



def create_simulated_data(arrival, custom):
    #Add initial arrival time
    x = np.random.random(size = 1000)
    firstpass = [f(x, arrival) for x in x]
    secondpass = custom.rvs(size = 1000)
    totalprob =  firstpass + secondpass

    counts, bin_edges = np.histogram(totalprob, bins = 'auto', normed = True)
    cdf = np.cumsum(counts)
    plt.plot(bin_edges[1:], cdf/cdf[-1])
    plt.title('CDF of Receiving Residential Overtime Ticket on Average SF Street')
    plt.ylabel("Cumulative Probability")
    plt.xlabel("Time(minutes)")
    plt.show()

    return secondpass


def plot_mean(mean, secondpass, title, color):
    x = np.random.random(size = 1000)
    firstpass_mean = [f(x, mean) for x in x]
    totalprob_mean =  firstpass_mean + secondpass
    counts_mean, bin_edges_mean = np.histogram(totalprob_mean, bins = 'auto', normed = True)
    cdf_mean = np.cumsum(counts_mean)
    plt.plot(bin_edges_mean[1:], cdf_mean/cdf_mean[-1], color =color, label = title)

    return


def split_by_pop(arrival_rate, secondpass, means):
    for i in range(1,11):
        mean= arrival_rate * means['base'] / means[i]
        title = 'pop' + str(i)
        color = plt.cm.RdYlGn(i/10)
        plot_mean(mean, secondpass, title, color)
    plt.legend()
    plt.xlabel('Time(minutes)')
    plt.ylabel('Probability')
    plt.xlim(120,600)
    plt.title('Probability of receiveing a ticket, split by OLS fitted volume populations')
    plt.show()

    return

def plot_mean_ci(mean, lci, uci, secondpass,  title, color):
    plt.figure(figsize = (10,6))
    x = np.random.random(size = 1000)
    firstpass_lci = [f(x, lci) for x in x]
    firstpass_mean = [f(x, mean) for x in x]
    firstpass_uci = [f(x, uci) for x in x]

    totalprob_lci =  firstpass_lci + secondpass
    totalprob_mean =  firstpass_mean + secondpass
    totalprob_uci =  firstpass_uci + secondpass


    counts_mean, bin_edges_mean = np.histogram(totalprob_mean, bins = 30, density = True)
    cdf_mean = np.cumsum(counts_mean)

    counts_low, bin_edges_low = np.histogram(totalprob_lci, bins = 30, density = True)
    cdf_low = np.cumsum(counts_low)

    counts_high, bin_edges_high = np.histogram(totalprob_uci, bins = 30, density = True)
    cdf_high = np.cumsum(counts_high)



    x_ = bin_edges_mean[1:]
    plt.plot(bin_edges_mean[1:], cdf_mean/cdf_mean[-1], color = color, label = title)
    plt.fill_between( x_,cdf_low/cdf_low[-1], cdf_high/cdf_high[-1], color = color, alpha = .25)


    return



def add_confidence_intervals(arrival_rate, secondpass,  means, stds):
    plt.figure(figsize = (10,6))
    arrival_lci = arrival_rate *   means['base'] / (means['base'] + 1.64*stds['base'])
    arrival_uci = arrival_rate *  means['base'] / (means['base'] - 1.64*stds['base'])

    arrival_best = arrival_rate * means['base'] / means[1]
    arrival_best_lci = arrival_rate * means['base'] / (means[10] - 1.64*stds[10])
    arrival_best_uci = arrival_rate * means['base'] / (means[10] + 1.64*stds[10])

    arrival_worst = arrival_rate * means['base'] / means[10]
    arrival_worst_lci = arrival_rate * means['base'] / (means[1] - 1.64*stds[1])
    arrival_worst_uci = arrival_rate * means['base'] / (means[1] + 1.64*stds[1])

    x = np.random.random(size = 1000)
    plt.figure(figsize = (10,6))
    plot_mean_ci(arrival_rate, arrival_lci, arrival_uci,secondpass,  'Baseline', 'blue')
    plot_mean_ci(arrival_best, arrival_best_lci, arrival_best_uci, secondpass, 'Best', 'green')
    plot_mean_ci(arrival_worst, arrival_worst_lci, arrival_worst_uci,secondpass,  'Worst', 'red')
    plt.title('CDF of Receiving Residential Overtime Ticket on different model street populations, SF Street W 90% confidence intervals')
    plt.ylabel("Cumulative Probability")
    plt.xlabel("Time(minutes)")
    plt.legend()
    plt.set_xlim(120,480)
    plt.xticks(np.arange(120,480,30))
    plt.show()


def main():

    with open(proc_loc + 'means.pickle', 'rb') as handle:
        means = pickle.load(handle)
    with open(proc_loc + 'stds.pickle', 'rb') as handle:
        stds = pickle.load(handle)


    df, streetdata = load_data()
    print('Creating initial arrival probability')
    arrival_rate = create_initial_arrival_prob(df, streetdata)

    print('Creatng second arrival probability')
    second_prob = create_return_distribution(df)

    secondpass = create_simulated_data(arrival_rate, second_prob)

    print('Lets look at the difference in populations that we fitted in our model')

    split_by_pop(arrival_rate, secondpass, means)

    add_confidence_intervals(arrival_rate, secondpass, means, stds)



if __name__== '__main__':
    main()
