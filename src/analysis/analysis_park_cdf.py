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
image_loc= '/home/colin/Desktop/SF_Parking/reports/figures/analysis/park/'


mpl.rcParams['figure.autolayout'] = True
mpl.rc('xtick', labelsize = 8 )

global conn
conn = sqlite3.connect(raw_loc + 'SF_Parking.db')
c = conn.cursor()
def delta_minutes(x,y):
    timedelta = y-x

    return timedelta.seconds / 60


def load_data_cdf():
    """Function is made to pull all data sources needed fot the entire analysis. It will merge these data sources into usable form, and also return just the street data.

    Returns
    -------
    df - dataframe
        dataframe with information on each ticket, the address, and the street link
    streetdata - DataFrame
        dataframe with street data only

    """
    streetdata = pd.read_sql_query('Select lineid, distance, park_supply, speed_ea from street_volume_data', conn)

    ticket_data = pd.read_sql_query("Select * from ticket_data where ViolationDesc = 'RES/OT' ", conn)

    address_data = pd.read_sql_query('Select address, lineid from address_data', conn)

    df = ticket_data.merge(address_data, left_on = 'address', right_on = 'address')
    df = df.merge(streetdata, left_on = 'lineid', right_on = 'lineid')
    return df, streetdata

def create_initial_arrival_prob(df, streetdata):
    """This will be the funciton to generate the initial arrival distribution. It will rely on a few inputs from the user, but the process is as follows.
        -generate percent of time spend on residential streets vs non-residential street_sweeping
        -find the average number of residential overtime tickets given by officer per days
        -find average number of officers patrolling per days
        -estimate how many spots per day a given officer will inspect
        -compare that with the total amount of spors available, and generate an arrival time.

    Parameters
    ----------
    df : dataframe
        ticket data merged with street info
    streetdata : dataframe
        street data only

    Returns
    -------
    float
        estimated time in minutes for each arrival on an average street

    """
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
    plt.savefig(image_loc + 'arrivalcdf.png')

    return arrival_rate



def create_return_distribution(df):
    """generate distribution of what time they will return, after marking your car initially. Will search for times when an officer gave out two tickets on the same street id between 2 and 3 hours and use that as our probability.

    Parameters
    ----------
    df : dataframe
        ticket data merged with street data

    Returns
    -------
    distribution
        cumulative distribution function of return time.

    """
    df['TickDate'] = df['TickIssueDate'].apply(lambda x:  dt.datetime.strftime(pd.to_datetime(x),'%Y-%m-%d'))

    df = df[['TickBadgeIssued', 'TickIssueDate', 'TicketNumber', 'TickIssueTime', 'lineid', 'TickDate']]

    df = df.merge(df, left_on = ['TickDate', 'lineid', 'TickBadgeIssued'], right_on = ['TickDate', 'lineid', 'TickBadgeIssued'])


    df= df[(df.TicketNumber_x != df.TicketNumber_y) & (df.TickIssueTime_y > df.TickIssueTime_x)]
    df['delta'] = df.apply(lambda x: delta_minutes(pd.to_datetime(x['TickIssueDate_x']), pd.to_datetime(x['TickIssueDate_y'])), axis = 1)

    df = df[(df.delta > 120) & (df.delta < 180)]
    df['delta'].hist(bins = 'auto')
    plt.title('Return Distribution After Marking Car')
    plt.xlabel('Time from Initial Ticket')
    plt.ylabel('Frequency')
    plt.grid(False)
    plt.savefig(image_loc + 'returndistro.png')
    plt.show()


    counts, bin_edges = np.histogram(df['delta'], bins = 'auto', density = True)

    cdf = np.cumsum(counts)

    plt.plot(bin_edges[1:], cdf/cdf[-1])
    plt.xlabel('Time after initial marking')
    plt.ylabel('Probability')
    plt.title('CDF of return probability after initial marking')
    plt.savefig(image_loc + 'returncdf.png')
    plt.show()


    values = df['delta']
    probs = 1/ df['delta'].shape[0]
    combination = pd.DataFrame({'val': values, 'probs' : probs})
    df = combination.groupby(by = 'val', as_index = False)['probs'].sum()
    custom = stats.rv_discrete(values = (df['val'], df['probs']))

    return custom

def f(x, arrival):
    """generates an esitmated arrivaltime based off an average arrival rate.

    Parameters
    ----------
    x : float
        random number between 0 and 1
    arrival : float
        mean arrival rate

    Returns
    -------
    float
        simluated arrival rate for one instance

    """
    return -math.log(1.0 - x) / (1/arrival)



def create_simulated_data(arrival, custom):
    """
    This function will take an arrival rate, pass it through the arrival function with 1000 samples, and pait them with 1000 return time samples. This will create a simulated dataset of how long it would take to get a residential overtime ticket.

    Parameters
    ----------
    arrival : float
        average arrival time, in minutes
    custom : type
        distribution of return rate

    Returns
    -------
    array
        array of return times to be combined with other arrivals

    """
    #Add initial arrival time
    x = np.random.random(size = 1000)
    firstpass = [f(x, arrival) for x in x]
    secondpass = custom.rvs(size = 1000)
    totalprob =  firstpass + secondpass

    counts, bin_edges = np.histogram(totalprob, bins = 'auto', density= True)
    cdf = np.cumsum(counts)
    plt.plot(bin_edges[1:], cdf/cdf[-1])
    plt.title('CDF of Receiving Residential Overtime Ticket on Average SF Street')
    plt.ylabel("Cumulative Probability")
    plt.xlabel("Time(minutes)")
    plt.xlim(120, 600)
    plt.savefig(image_loc + 'AverageCDFTotal.png')
    plt.show()

    return secondpass


def plot_mean(mean, secondpass, title, color):
    """This function will plot a cumulative distribution of how long it will take to receive a ticket. It will combine the given arrival rate, the second pass returns, and plot them on the axis given a specific color.

    Parameters
    ----------
    mean : float
        arrival rate passed
    secondpass : array
        sample of 1000 return rates
    title : string
        title of line to be plotted
    color : string(color)
        color of line to be plotted

    Returns
    -------
    plot
        added plot of line

    """
    x = np.random.random(size = 1000)
    firstpass_mean = [f(x, mean) for x in x]
    totalprob_mean =  firstpass_mean + secondpass
    counts_mean, bin_edges_mean = np.histogram(totalprob_mean, bins = 'auto', density = True)
    cdf_mean = np.cumsum(counts_mean)
    plt.plot(bin_edges_mean[1:], cdf_mean/cdf_mean[-1], color =color, label = title)

    return


def split_by_pop(arrival_rate, secondpass, means):
    """This function will take the average arrival rate, as well as all population means from the model creation stage. It will combine these to generate new arrival rates each population, and plot them all on the same axis.

    Parameters
    ----------
    arrival_rate : float
        average time for arrival
    secondpass : array
        array of return times
    means : dictionary
        dictionary of all population means from model phase.

    Returns
    -------
    none
        prints a plot

    """
    for i in range(1,11):
        mean= arrival_rate * means[i] / means['base']
        title = 'pop' + str(i)
        color = plt.cm.RdYlGn(1-i/10)
        plot_mean(mean, secondpass, title, color)
    plt.legend()
    plt.xlabel('Time(minutes)')
    plt.ylabel('Probability')
    plt.xlim(120,600)
    plt.title('Probability of receiveing a ticket, split by OLS fitted volume populations')
    plt.savefig(image_loc + 'SplitByPopCDF.png')
    plt.show()

    return

def plot_mean_ci(mean, lci, uci, secondpass,  title, color, ax):
    """This function will be used to create condifence intervals on the ticket probability distribution. It will take the lower and upper confidence intervals to create new distributions, and will fill the area between them.

    Parameters
    ----------
    mean : float
        average arrival rate
    lci : float
        lower confidence interval of arrival
    uci : float
        upper confidence interval of arrival
    secondpass : array
        array of return distributions
    title : string
        title of populations
    color : stirng(color)
        color to plot

    Returns
    -------
    none
        plots new lines

    """
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
    ax.plot(bin_edges_mean[1:], cdf_mean/cdf_mean[-1], color = color, label = title)
    ax.fill_between( x_,cdf_low/cdf_low[-1], cdf_high/cdf_high[-1], color = color, alpha = .25)

    return



def add_confidence_intervals(arrival_rate, secondpass,  means, stds):
    """Function to generate total plot that includes confidence intervals. will generate upper and lower arrival rates, and pass through plotting function.

    Parameters
    ----------
    arrival_rate : float
        average arrival rate
    secondpass : array
        return distribution
    means : dictionary
        all population means from model phase
    stds : dictionary
        all standard deviations from model phase

    Returns
    -------
    none
        plots mean and conficence intervals

    """

    pops = {'worst': (10, 'red'), 'best': (1, 'green'), 'base':('base', 'blue')}

    for key, value in pops.items():
        val = value[0]
        color = value[1]
        mean =  arrival_rate * means['base']/ means[val]
        lci = arrival_rate *  means['base'] / (means[val] + 1.64 *stds[val])
        uci = arrival_rate *  means['base']/ (means[val] - 1.64 * stds[val])
        x = np.random.random(size = 1000)
        firstpass_lci = [f(x, lci) for x in x]
        firstpass_mean = [f(x, mean) for x in x]
        firstpass_uci = [f(x, uci) for x in x]

        totalprob_lci =  firstpass_lci + secondpass
        totalprob_mean =  firstpass_mean + secondpass
        totalprob_uci =  firstpass_uci + secondpass


        counts_mean, bin_edges_mean = np.histogram(totalprob_mean, bins =100, density = True)
        cdf_mean = np.cumsum(counts_mean)

        counts_low, bin_edges_low = np.histogram(totalprob_lci, bins = 100, density = True)
        cdf_low = np.cumsum(counts_low)

        counts_high, bin_edges_high = np.histogram(totalprob_uci, bins = 100, density = True)
        cdf_high = np.cumsum(counts_high)

        x_ = bin_edges_mean[1:]
        plt.plot(bin_edges_mean[1:], cdf_mean/cdf_mean[-1], color = color, label = key)
        plt.fill_between( x_,cdf_low/cdf_low[-1], cdf_high/cdf_high[-1], color = color, alpha = .25)



    plt.legend()
    plt.xlim(120,600)
    plt.xticks(np.arange(120,600,30))
    plt.title('Ticket Probabilty over time for different population groups')
    plt.savefig(image_loc + 'CDFwSD.png')
    plt.show()
    return


def main():
    """Function to run through entire script if ran.
    -load dataset
    -create arrival rates
    -create return rates
    -split by populations
    -add confidence intervals

    Returns
    -------
    type
        Description of returned object.

    """

    with open(proc_loc + 'means.pickle', 'rb') as handle:
        means = pickle.load(handle)
    with open(proc_loc + 'stds.pickle', 'rb') as handle:
        stds = pickle.load(handle)


    df, streetdata = load_data_cdf()
    print('Creating initial arrival probability')
    arrival_rate = create_initial_arrival_prob(df, streetdata)

    print('Creating second arrival probability')
    second_prob = create_return_distribution(df)

    secondpass = create_simulated_data(arrival_rate, second_prob)

    print('Lets look at the difference in populations that we fitted in our model')

    split_by_pop(arrival_rate, secondpass, means)

    add_confidence_intervals(arrival_rate, secondpass, means, stds)



if __name__== '__main__':
    main()
