#!/usr/bin/python3
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import itertools
import datetime as dt
import time
import re
import pickle
from scipy import stats
import sqlite3
import geopandas as gpd
import seaborn as sns
import statsmodels.api as sm
from statsmodels.graphics.gofplots import ProbPlot


raw_loc = '/home/colin/Desktop/SF_Parking/data/raw/'
proc_loc = '/home/colin/Desktop/SF_Parking/data/processed/'
image_loc= '/home/colin/Desktop/SF_Parking/reports/figures/analysis/model/'


mpl.rcParams['savefig.bbox'] = 'tight'
mpl.rcParams['figure.autolayout'] = True
mpl.rc('xtick', labelsize = 8 )

global conn
conn = sqlite3.connect(proc_loc + 'SF_Parking.db')


def create_street_data():
    """Function to create all neccesary data to run analysis on street volume and ticket results.

    Returns
    -------
    dataframe
        dataframe of tickets.

    """
    #Lets categorize addresses by our street volume
    streets = pd.read_sql_query("Select distinct t2.lineid, nhood, distance, total_ea, vvol_carea, vvol_trkea, vvol_busea, speed_ea, count(*) total_tickets "
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

def create_street_data_parking():
    """Function to create all neccesary data to run analysis on street volume and ticket results.

    Returns
    -------
    dataframe
        dataframe of tickets.

    """
    #Lets categorize addresses by our street volume
    streets = pd.read_sql_query("Select distinct t3.lineid, t3.streetname, nhood, distance, total_ea, vvol_carea, vvol_trkea, vvol_busea, speed_ea, oneway, count(*) total_tickets, park_supply "
                       'from ticket_data t1 join address_data t2 on t1.address = t2.address '
                       ' join street_volume_data t3 on t2.lineid = t3.lineid '
                       " Where ViolationDesc = 'RES/OT'  group by t3.lineid", conn)
    c = conn.cursor()
    c.execute('Select Max(TickIssueDate), Min(TickIssueDate) from ticket_data')
    totaldays = c.fetchone()
    maxdate = time.strptime( totaldays[0], '%Y-%m-%d %H:%M:%S')
    mindate = time.strptime( totaldays[1], '%Y-%m-%d %H:%M:%S')
    totaldays = (time.mktime(maxdate) - time.mktime(mindate)) / (60*60*24)
    totalyears = totaldays /365

    streets['parkpermile'] = streets['park_supply'] / streets['distance']
    streets_mean = streets[streets.park_supply > 0 ].groupby(by = ['nhood'], as_index = False)['parkpermile'].mean()
    streets_1 = streets[streets.park_supply > 0 ]
    streets_2 = streets[(streets.park_supply== 0) | (pd.isnull(streets.park_supply)) ]
    streets_2 = streets_2.merge(streets_mean, left_on = 'nhood', right_on = 'nhood')

    streets_2['park_supply'] = streets_2['parkpermile_y'] * streets_2['distance']
    streets_2.rename(columns = {'parkpermile_y':'parkpermile'}, inplace = True)
    streets_2.drop(columns =['parkpermile_x'], inplace = True)
    streets = streets_1.append(streets_2)
    streets['total_ea'] = streets['total_ea'] + 1

    streets['total_ea'] = streets['total_ea'] + 1
    streets['tickpermile'] = streets['total_tickets'] / (streets['distance']) / totalyears
    streets['tickperspot'] = streets['total_tickets'] / (streets['park_supply'] / 100) / totalyears
    streets = streets[streets.tickperspot < 6000]
    return streets



def show_street_map(streets):
    """Function to plot all streets identified as residential overtime candidates, for verification purposes.

    Parameters
    ----------
    streets : GeoDataFrame
        geodataframe of all streets identified

    Returns
    -------
    none
        shows plot

    """
    streetvolume = gpd.read_file(proc_loc + 'final_streets/SF_Street_Data.shp')
    streetvolume = streetvolume.to_crs(epsg = 4326)
    times = ['am', 'pm', 'ev', 'ea']
    for time in times:
        streetvolume['totalinv_' + time]  = streetvolume['total_'+time].apply(lambda x: np.log(1/(x+.5)))

    df = streetvolume.merge(streets, left_on = 'lineid', right_on = 'lineid')

    df.plot(figsize = (10,10), color = 'Red')
    plt.title('Streets identified as Residential Overtime Areas')
    plt.savefig(image_loc + 'idstreets.png')
    plt.show()
    return




def show_street_plots(streets):
    """function to show histogram data and scatter plot of street data.

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
        fig.savefig(image_loc + 'streetnormality.png')
        fig.show()



    choice = input('Would you like to see the scatter plot of street volume vs. tickets?')

    if choice == 'Y':
        fig = plt.figure(figsize = (15, 15))
        ax = fig.add_subplot(1,1,1)
        ax.scatter(x = np.log(streets['total_ea']), y = streets['total_tickets'])
        ax.set_xlabel('Total Volume(log)')
        ax.set_ylabel('Total Tickets')
        ax.set_title('Scatter Plot of Street Volume vs. Total Tickets')
        ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
        ax.set_axisbelow(True)
        ax.yaxis.grid(color='gray', linestyle='dashed', alpha = .5)
        fig.savefig(image_loc + 'volvstix.png')
        fig.show()




        fig = plt.figure(figsize = (15, 15))
        ax = fig.add_subplot(1,1,1)
        ax.scatter(x = np.log(streets['total_ea']), y = streets['tickpermile'])
        ax.set_xlabel('Total Volume(log)')
        ax.set_ylabel('Total Tickets')
        ax.set_title('Scatter Plot of Street Volume vs. Total Tickets per mile')
        ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
        ax.set_axisbelow(True)
        ax.yaxis.grid(color='gray', linestyle='dashed', alpha = .5)
        title = 'VolvsTixMile.png'
        fig.savefig(image_loc + 'volvstixmile.png')
        fig.show()

    return


def show_street_plots_parking(streets):
    """Will create street plots, while including parking availability as well.

    Parameters
    ----------
    streets : dataframe
        created dataframe that includes all necessary analysis for street analysis.

    Returns
    -------
    none
        plots volume

    """
    fig = plt.figure(figsize = (15, 15))
    ax = fig.add_subplot(1,1,1)
    ax.scatter(x = np.log(streets['total_ea']), y = streets['tickperspot'])
    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    ax.set_axisbelow(True)
    ax.yaxis.grid(color='gray', linestyle='dashed', alpha = .5)
    title = 'VolvsTix.png'
    ax.set_title('Scatter Plot of Total Street Volume vs. Total Tickets per 100 spots per Year')
    ax.set_ylabel('Tickets per 100 spots per year')
    ax.set_xlabel('Total Street Volume')
    fig.savefig(image_loc + 'volvsparkspots.png')
    fig.show()
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
    res = stats.ttest_ind(df_lowvol['tickpermile'], df_highvol['tickpermile'], equal_var = False)
    plt.savefig(image_loc +  'twopopbox.png')
    plt.show()
    print('Comparing Means')
    print(res)
    return



def split_pop_test(streets, pops, fitted, parking, modelname,modelsave baseline = False):
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
        sample = []
        for j in np.arange(1,1000):
            if parking == False:
                sample.append(streets['tickpermile'].sample(n = 20).median())
            else:
                sample.append(streets['tickperspot'].sample(n = 20).median())

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
            if parking == False:
                sample.append(df['tickpermile'].sample(n = 20).median())
            else:
                sample.append(df['tickperspot'].sample(n = 20).median())

        sample = np.array(sample)

        means[i] = sample.mean()
        stds[i] = sample.std()
        normals = stats.norm(loc = means[i], scale = stds[i])

        x = np.linspace(normals.ppf(0.01),
                            normals.ppf(0.99),
                            100)
        labelstr = 'Group ' + str(i)
        ax = plt.plot(x, normals.pdf(x), label = labelstr, color =  plt.cm.RdYlGn(1-i/10))
    plt.legend( loc = 0)
    if parking == False:
        plt.xlabel('Tickets per mile per year')
    else:
        plt.xlabel('Tickets per 100 spots per year')
    plt.ylabel('Frequency')

    if fitted == True:
        plt.title('Frequency curves of sampled street populations sorted by OLS fitted values,  ' + modelname)
    else:
        plt.title('Frequency curves of sampled street populations sorted by total volume ')
    plt.savefig(image_loc + modelsave)
    plt.show()

    return means, stds




def diagnostic_plots(model_fit, streets, model_name, modelsave):
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

    fig.suptitle('Diagnostic plots for ' + model_name)
    fig.tight_layout(rect=[0, 0.03, 1, 0.95])
    fig.show()
    fig.savefig(image_loc + modelsave)
    return


def feature_analysis(streets, parking):
    """Analysis section of exploring more features. We'll add more features and run some linear regressions.

    Returns
    -------
    type
        Description of returned object.

    """

    street_data = pd.read_sql_query('Select lineid, vvol_trkea, vvol_carea, vvol_busea, speed_ea from street_volume_data', conn)



    if parking == True:
        columns = ['vvol_trkea', 'vvol_carea', 'vvol_busea', 'speed_ea', 'parkpermile', 'distance', 'oneway']
        print("Creating model with buses, trucks, cars, parking density, street distance, one way, and freeflow speed. ")
    else:
        columns = ['vvol_trkea', 'vvol_carea', 'vvol_busea', 'speed_ea']
        print("Creating model with buses, trucks, cars, and freeflow speed. ")

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
        done = 'N'
        while done != "Y":
            count = int(input("How many populations would you like?"))
            if count > 0:
                means, stds = split_pop_test(streets, count, True,False, 'base model')
                print('The difference between our best and worst population means was {:.1%}'.format((1 - (means[10]/ means[1]))))
            else:
                print('Invalid input, put an integer')
            done = input('Are you done? (Y or N)')


    choice = input('Would you like to see some diagnostic plots of the model?')
    if choice == 'Y':
        if parking == True:
            title = 'base model with parking included'
            imagetitle = 'basemodelparkingdiagnostics.png'
        else:
            title = 'base model'
            imagetitle = 'basemodeldiagnostics.png'
        diagnostic_plots(res, streets, title, imagetitle)

    return



def log_feature_analysis(streets, parking):
    print("Let's log fit the features and try again")
    if parking == True:
        formstring = 'tickperspot ~np.log(vvol_trkea)+np.log(vvol_carea)+np.log(vvol_busea)+np.log(speed_ea) + np.log(parkpermile) + oneway'
    else:
        formstring = 'tickpermile ~np.log(vvol_trkea)+np.log(vvol_carea)+np.log(vvol_busea)+np.log(speed_ea)'
    model = sm.OLS.from_formula(formstring , streets)
    res = model.fit()
    plt.rc('figure', figsize=(12, 7))
    plt.text(0.01, 0.05, str(res.summary()), {'fontsize': 10}, fontproperties = 'monospace')
    plt.axis('off')
    plt.tight_layout()
    plt.show()



    streets['fitted'] =  res.fittedvalues
    streets.sort_values(by = 'fitted', inplace = True)

    choice = input('Would you like to bootstrap some population means based off fitted values?')
    if choice == 'Y':
        done = 'N'
        while  done != "Y":
            count = int(input("How many populations would you like?"))
            if count > 0:
                means, stds = split_pop_test(streets, count, True, False, 'log model')
                print('The difference between our best and worst population means was {:.1%}'.format((1 - (means[10]/ means[1]))))
            else:
                print('Invalid input, put an integer')
            done = input('Are you done? (Y or N)')

    choice = input('Would you like to see some diagnostic plots of the model?')
    if choice == 'Y':
        if parking == True:
            title = 'log model with parking included'
            imagetitle = 'logmodelparkingdiagnostics.png'
        else:
            title = 'log model'
            imagetitle = 'logmodelparkingdiagnostics.png'
            streets.drop(columns = 'fitted', inplace = True)
        diagnostic_plots(res, streets, title, imagetitle)
    return streets


def interaction_model(streets):

        columns = ['vvol_carea', 'vvol_trkea', 'vvol_busea', 'speed_ea', 'parkpermile', 'distance', 'oneway']
        formulastring = 'tickperspot ~ '

        formulastring += '+'.join(columns)

        for combo in itertools.combinations(columnlist, 2):
            formulastring += '+' + combo[0] + '*' + combo[1]
        model = sm.OLS.from_formula(formulastring , streets)
        res = model.fit()
        plt.rc('figure', figsize=(12, 7))
        plt.text(0.01, 0.05, str(res.summary()), {'fontsize': 10}, fontproperties = 'monospace')
        plt.axis('off')
        plt.tight_layout()
        plt.show()



def final_model(streets):
    """creates final model that we will use.

    Parameters
    ----------
    streets : dataframe
        dataframe of streets, number of tickets, and attributes up to this point

    Returns
    -------
    means, stds
        Dataframe of mean values and standard deviations of our 10 populations.

    """

    formstring = 'tickperspot ~np.log(vvol_trkea)+np.log(vvol_carea)+np.log(vvol_busea)+np.log(speed_ea) + np.log(parkpermile)'\
        ' + parkpermile:distance + oneway'
    model = sm.OLS.from_formula(formstring , streets)
    res = model.fit()
    plt.figure(figsize = (12, 7))
    plt.rc('figure', figsize=(12, 7))
    plt.text(0.01, 0.05, str(res.summary()), {'fontsize': 10}, fontproperties = 'monospace')
    plt.axis('off')
    plt.tight_layout()
    plt.show()
    streets['fitted'] =  res.fittedvalues
    streets.sort_values(by = 'fitted', ascending = True, inplace = True)
    means, stds = split_pop_test(streets, 10, True,True, 'final model', baseline = True)
    print('The difference between our best and worst population means was {:.1%}'.format((1 - (means[10]/ means[1]))))
    return means, stds



def main():
    print("Welcome to the initial analyis.")
    print('Loading Data into usable form')
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
            means, stds = split_pop_test(streets, count, False, False, 'volume only model',  str(count) + 'PopVolSorted.png')
            print('The difference between our best and worst population means was {:.1%}'.format((1 - (means[10]/ means[1]))))
            done = input('Are you done? (Y or N)')


    choice = input('Would you like to explore more features?')
    if choice == 'Y':
        print('Beginning feature analysis')
        feature_analysis(streets, False)

    choice = input('Would you like to log fit the features and try again?')
    if choice == 'Y':
        df = streets
        columns = ['vvol_trkea', 'vvol_carea', 'vvol_busea', 'speed_ea']
        for column in columns:
            df[column] = df[column] + 0.01
        streets= log_feature_analysis(streets, False)




    choice = input('Would you like to include parking availability?')
    streets = create_street_data_parking()
    if choice == "Y":
        choice = input('Would you like to see a new scatter plot including parking?')

        if choice == 'Y':
            show_street_plots_parking(streets)


        choice = input('Would you like to see split into population groups based on volume , and bootstrap the decision data?')
        streets.sort_values(by = 'total_ea', inplace = True)
        if choice == "Y":
            count = -1
            done = 'N'
            while count < 0 and done != "Y":
                count = int(input("How many populations would you like?"))
                means, stds = split_pop_test(streets, count, False, True, 'volume only model w parking', str(count) + 'PopVolSortedParking.png')
                print('The difference between our best and worst population means was {:.1%}'.format((1 - (means[10]/ means[1]))))
                done = input('Are you done? (Y or N)')


        choice = input('Would you like to explore more features?')
        if choice == 'Y':
            print('Beginning feature analysis')
            feature_analysis(streets, True)

        df = streets
        columns = ['vvol_trkea', 'vvol_carea', 'vvol_busea', 'speed_ea']
        for column in columns:
            df[column] = df[column] + 0.01

        choice = input('Would you like to log fit the features and try again?')
        if choice == 'Y':
            streets = log_feature_analysis(streets, True)

        choice = input('Would you like to include all interaction effects in a model?')

        if choice == 'Y':
            interaction_model(streets)

        print("We're going to create the final model now")

        means, stds = final_model(streets)

        with open(proc_loc + 'means.pickle', 'wb') as handle:
            pickle.dump(means, handle, protocol=pickle.HIGHEST_PROTOCOL)

        with open(proc_loc + 'stds.pickle', 'wb') as handle:
            pickle.dump(stds, handle, protocol=pickle.HIGHEST_PROTOCOL)

        choice = input('Would you like to save the model?(Y or N)')
        if choice == 'Y':
            streets.to_pickle(proc_loc + 'FinalModel.pkl')




    print('You have completed the initial analysis')


    return

if __name__== '__main__':
    main()
