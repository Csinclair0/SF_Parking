#!/usr/bin/python3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import itertools
import datetime as dt
import time
from scipy import stats
import sqlite3
import geopandas as gpd
import seaborn as sns
import statsmodels.api as sm
from statsmodels.graphics.gofplots import ProbPlot


raw_loc = '/home/colin/Desktop/SF_Parking/data/raw/'
proc_loc = '/home/colin/Desktop/SF_Parking/data/processed/'
image_loc = '/home/colin/Desktop/SF_Parking/reports/figures/analysis/'


global conn
conn = sqlite3.connect(raw_loc + 'SF_Parking.db')


def create_street_data():
    """Function to create all neccesary data to run analysis on street volume and ticket results.

    Returns
    -------
    dataframe
        dataframe of tickets.

    """
    #Lets categorize addresses by our street volume
    streets = pd.read_sql_query("Select distinct t2.lineid, nhood, distance, total_ea, vvol_busea, speed_ea, count(*) total_tickets "
                           'from ticket_data t1 join address_data t2 on t1.address = t2.address '
                           ' join street_volume_data t3 on t2.lineid = t3.lineid '
                           " Where ViolationDesc = 'RES/OT' group by t2.lineid", conn)
    c = conn.cursor()
    c.execute('Select Max(TickIssueDate), Min(TickIssueDate) from ticket_data')
    totaldays = c.fetchone()
    maxdate = time.strptime( totaldays[0], '%Y-%m-%d %H:%M:%S')
    mindate = time.strptime( totaldays[1], '%Y-%m-%d %H:%M:%S')
    totaldays = (time.mktime(maxdate) - time.mktime(mindate)) / (60*60*24)
    totalyears = totaldays /365

    streets['total_ea'] = streets['total_ea'] + 1
    streets['tickpermile'] = streets['total_tickets'] / (streets['distance']) / totalyears
    return streets


def show_street_map(streets):
    streetvolume = gpd.read_file(proc_loc + 'final_streets/SF_Street_Data.shp')
    streetvolume = streetvolume.to_crs(epsg = 4326)
    times = ['am', 'pm', 'ev', 'ea']
    for time in times:
        streetvolume['totalinv_' + time]  = streetvolume['total_'+time].apply(lambda x: np.log(1/(x+.5)))

    df = streetvolume.merge(streets, left_on = 'lineid', right_on = 'lineid')

    df.plot(figsize = (20,20), color = 'Red')
    plt.title('Streets identified as Residential Overtime Areas')
    plt.show()
    title = 'ResOTStreets.png'
    if storefigs == 'Y':
        plt.savefig(image_loc + title)
    plt.show()
    return




def show_street_plots(streets):
    """Short summary.

    Parameters
    ----------
    streets : dataframe
        created dataframe that includes all necessary analysis for street analysis.

    Returns
    -------
    none

    """

    choice = input('Would you like to see the log transform of features?')

    if choice == 'Y':
        fig, axplots = plt.subplots(2, figsize = (10, 10))
        log_volume = np.log(streets['total_ea'])
        log_tickets = np.log(streets['total_tickets'])
        vol_mean = log_volume.mean()
        tick_mean = log_tickets.mean()
        vol_std = log_volume.std()
        tick_std = log_tickets.std()
        vol_normals = stats.norm(loc = vol_mean, scale = vol_std)
        vol = np.linspace(vol_normals.ppf(0.01),
                                vol_normals.ppf(0.99), 100)

        tick_normals = stats.norm(loc = tick_mean, scale = tick_std)
        ticks = np.linspace(tick_normals.ppf(0.01),
                                vol_normals.ppf(0.99),
                                100)


        axplots[0].hist(log_volume, bins = 'auto', density = True)
        axplots[0].set_xlabel('Total Street Volume(log)')
        axplots[0].plot(vol, vol_normals.pdf(vol))

        axplots[1].hist(np.log(streets.total_tickets), bins = 'auto', density = True )
        axplots[1].set_xlabel('Total Tickets(log)')
        axplots[1].plot(ticks, tick_normals.pdf(ticks))
        fig.suptitle('Feature Normality Plots')
        title = 'FeatureNormality.png'
        if storefigs == 'Y':
            plt.savefig(image_loc + title)
        plt.show()



        choice = input('Would you like to see the scatter plot of street volume vs. tickets?')

        if choice == 'Y':
            plt.figure(figsize = (15, 15))
            plt.scatter(x = np.log(streets['total_ea']), y = streets['total_tickets'])
            plt.xlabel('Total Volume(log)')
            plt.ylabel('Total Tickets')
            plt.title('Scatter Plot of Street Volume vs. Total Tickets')
            title = 'VolvsTix.png'
            if storefigs == 'Y':
                plt.savefig(image_loc + title)
            plt.show()


            plt.figure(figsize = (15, 15))
            plt.scatter(x = np.log(streets['total_ea']), y = streets['tickpermile'])
            plt.title('Scatter Plot of Total Street Volume vs. Total Tickers per Mile per Year ')
            title = 'VolvsTixMile.png'
            if storefigs == 'Y':
                plt.savefig(image_loc + title)
            plt.show()

        return




def two_pop_test(streets):
    """This function will split the streets dataframe into two populations

    Parameters
    ----------
    streets : dataframe
        streets dataframe used for analysis.

    Returns
    -------
    none

    """
    df_lowvol = streets[streets.total_ea <=  np.percentile(streets['total_ea'], 50)]
    df_highvol = streets[streets.total_ea > np.percentile(streets['total_ea'], 50)]

    plt.figure(figsize = (8,8))
    tickets = [np.log(df_lowvol['tickpermile']),  np.log(df_highvol['tickpermile'])]
    plt.boxplot(tickets)
    plt.title('Box Plot of Tickets per Mile, split by street volume')
    plt.xticks(np.arange(1,3), labels = ('Lower Volume', 'Higher Volume'))
    plt.ylabel('Tickets per Mile (log)')
    plt.show()

    res = stats.ttest_ind(df_lowvol['tickpermile'], df_highvol['tickpermile'], equal_var = False)

    print('Comparing Means')
    print(res)

    return





def split_pop_test(streets, pops, fitted, baseline = False):
    """This function will take the street data, sort it by street volume, and bootstrap simulated data that will

    Parameters
    ----------
    streets : dataframe
        Description of parameter `street`.
    pops : int
        Description of parameter `pops`.
    fitted : Boolean
        Whether the plot is sorted by OLS fitted values or street volume

    Returns
    -------
    shows plot, returns nothing

    """
    plt.figure(figsize = (10, 10))
    means = {}
    stds = {}
    totalsize = streets.shape[0]
    #create baseline curve
    if baseline == True:
        for j in np.arange(1,1000):
            sample.append(df['tickpermile'].sample(n = 20).median())

        sample = np.array(sample)

        means['base'] = sample.mean()
        stds['base'] = sample.std()
        normals = stats.norm(loc = means['base'], scale = stds['base'])
        x = np.linspace(normals.ppf(0.01),
                    normals.ppf(0.99),
                    100)

        plt.plot(x, normals.pdf(x), label = 'Baseline', color = 'black', linestyle = '--')

    for i in np.arange(1,pops + 1):
        if i == 1:
            df = streets[0:int(totalsize * 1/pops)]
        else:
            df = streets[((i-1)/pops * totalsize).astype(int): (((i)/pops) * totalsize).astype(int)]

        sample = []
        for j in np.arange(1,1000):
            sample.append(df['tickpermile'].sample(n = 20).median())

        sample = np.array(sample)

        means[i] = sample.mean()
        stds[i] = sample.std()
        normals = stats.norm(loc = means[i], scale = stds[i])

        x = np.linspace(normals.ppf(0.01),
                            normals.ppf(0.99),
                            100)
        labelstr = 'population' + str(i)
        ax = plt.plot(x, normals.pdf(x), label = labelstr, color =  plt.cm.RdYlGn(i/10))
    plt.legend( loc = 0)
    plt.xlabel('Tickets per mile per year')
    plt.ylabel('Frequency')
    if fitted == True:
        plt.title('Frequency curves of sampled street populations sorted by OLS fitted values ')
    else:
        plt.title('Frequency curves of sampled street populations sorted by total volume ')
    plt.show()
    return


def diagnostic_plots(model_fit):
    model_fitted_y = model_fit.fittedvalues

    # residuals
    model_residuals = model_fit.resid

    # normalized residuals
    model_norm_residuals = model_fit.get_influence().resid_studentized_internal

    # absolute squared normalized residuals
    model_norm_residuals_abs_sqrt = np.sqrt(np.abs(model_norm_residuals))

    # absolute residuals
    model_abs_resid = np.abs(model_residuals)

    # leverage, from statsmodels internals
    model_leverage = model_fit.get_influence().hat_matrix_diag

        #All four diagnostic plots in one place
    fig, axarr = plt.subplots(2,2, figsize = (20,20))
    QQ = ProbPlot(model_norm_residuals)

    #residuals
    sns.residplot( model_fitted_y, 'tickpermile', data=streets,
                              lowess=True,
                              scatter_kws={'alpha': 0.5},
                              line_kws={'color': 'red', 'lw': 1, 'alpha': 0.8}, ax = axarr[0,0])
    axarr[0,0].set_xlabel('Fitted')
    axarr[0,0].set_ylabel('Residual')


    #Q-Q
    QQ.qqplot(line='45', alpha=0.5, color='#4C72B0', lw=1, ax = axarr[0,1])
    axarr[0,1].set_xlabel('Theoretical Quantile')
    axarr[0,1].set_ylabel('Residuals')
    axarr[0,1].set_xlim(-4,4)



    #scale - location
    axarr[1,0].scatter(model_fitted_y, model_norm_residuals_abs_sqrt, alpha=0.5)
    sns.regplot(model_fitted_y, model_norm_residuals_abs_sqrt,
                scatter=False,
                ci=False,
                lowess=True,
                line_kws={'color': 'red', 'lw': 1, 'alpha': 0.8}, ax = axarr[1,0])
    axarr[1,0].set_xlabel('Fitted')
    axarr[1,0].set_ylabel('Standardized')


    #Leverage
    axarr[1,1].scatter(model_leverage, model_norm_residuals, alpha=0.5)
    sns.regplot(model_leverage, model_norm_residuals,
                scatter=False,
                ci=False,
                lowess=True,
                line_kws={'color': 'red', 'lw': 1, 'alpha': 0.8}, ax = axarr[1,1])
    axarr[1,1].set_xlim(0,0.1)
    axarr[1,1].set_xlabel('Leverage')
    axarr[1,1].set_ylabel('Standardized Residuals')
    plt.show()

    return



def feature_analysis(streets):
    """Analysis section of exploring more features. We'll add more features and run some linear regressions.

    Returns
    -------
    type
        Description of returned object.

    """

    street_data = pd.read_sql_query('Select lineid, vvol_trkea, vvol_carea, vvol_busea, speed_ea from street_volume_data', conn)

    streets = streets.merge(street_data, left_on = 'lineid', right_on = 'lineid')

    print("Creating model with buses, trucks, cars, and freeflow speed. ")
    columns = ['vvol_trkea', 'vvol_carea', 'vvol_busea', 'speed_ea']

    model = sm.OLS.from_formula('tickpermile ~' + '+'.join(columns) , streets)
    res = model.fit()
    plt.rc('figure', figsize=(12, 7))
    plt.text(0.01, 0.05, str(res.summary()), {'fontsize': 10}, fontproperties = 'monospace')
    plt.axis('off')
    plt.tight_layout()
    plt.show()


    choice = input('Would you like to bootstrap some population means based off fitted values?')
    streets['fitted'] =  res.fittedvalues
    streets.sort_values(by = 'fitted', inplace = True)
    if choice == 'Y':
        choice = input('How Many Populations?')
        while count < 0 and done != "Y":
            count = input("How many populations would you like?")
            if count > 0:
                chart = split_pop_test(streets, count, True)
            else:
                print('Invalid input, put an integer')
            done = input('Are you done? (Y or N)')


    print('Would you like to see some diagnostic plots of the model?')
    if choice == 'Y':
        diagnostic_plots(res)

    print("Let's log fit the features and try again")

    df = streets
    for column in columns:
        df[column] = df[column] + 0.01

    formstring = 'tickpermile ~np.log(vvol_trkea)+np.log(vvol_carea)+np.log(vvol_busea)+np.log(speed_ea)'
    model = sm.OLS.from_formula(formstring , streets)
    res = model.fit()
    plt.rc('figure', figsize=(12, 7))
    plt.text(0.01, 0.05, str(res.summary()), {'fontsize': 10}, fontproperties = 'monospace')
    plt.axis('off')
    plt.tight_layout()
    plt.show()

    print('Would you like to see some diagnostic plots of the model?')
    if choice == 'Y':
        diagnostic_plots(res)

    streets['fitted'] =  res.fittedvalues
    streets.sort_values(by = 'fitted', inplace = True)

    choice = input('Would you like to bootstrap some population means based off fitted values?')
    streets['fitted'] =  res.fittedvalues
    streets.sort_values(by = 'fitted', inplace = True)
    if choice == 'Y':
        choice = input('How Many Populations?')
        while count < 0 and done != "Y":
            count = input("How many populations would you like?")
            if count > 0:
                chart = split_pop_test(streets, count, True, baseline = True)
            else:
                print('Invalid input, put an integer')
            done = input('Are you done? (Y or N)')































def main():
    print("Welcome to the initial analyis.")
    global storefigs
    storefigs = input('Would you like to store photos in the project folder?(Y or N)')
    streets = create_street_data()
    choice = input("Would you like to see a map of all the streets we've identified as Residential Overtime Areas?(Y or N)")
    if choice == 'Y':
        show_street_map(streets)
        print("Compare this to the file in the report folder, titled 'sf_permit_areas.pdf'")

    choice = input('Would you like to see some initial charts of the street data?')

    if choice == 'Y':
        show_street_plots(streets)

    print("First, we'll look solely at total volume")
    choice = input('Would you like to test the difference between two populations?')

    if choice == 'Y':
        two_pop_test(streets)

    choice = input('Would you like to see split into more population groups, and bootstrap the decision data?')
    streets.sort_values(by = 'total_ea', inplace = True)
    if choice == "Y":
        count = -1
        done = 'N'
        while count < 0 and done != "Y":
            count = int(input("How many populations would you like?"))
            split_pop_test(streets, count, False)
            done = input('Are you done? (Y or N)')


    choice = input('Would you like to explore more features?')
    print('Beginning feature analysis')

    if choice == 'Y':
        feature_analysis(streets)







if __name__== '__main__':
    main()
