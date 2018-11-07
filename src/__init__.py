import db.create_ticket_data
from explore.explore_data import *
from analysis.analysis_model import *
from analysis.analysis_park_cdf import *
from analysis.street_cleaning import *
from extras.extras import *
import warnings
warnings.filterwarnings('ignore')



def main():
    """This is the main process of the project. Here you will go through the following process, skipping anything unwantedself.
    1.Create ticket Database
    2. Add Street Volume
    3. Explore Data with characters
    4. Initial Analysis W/O parking
    5. Final Analysis W Parking
    6. Street Cleaning
    7. Extra features

    Returns
    -------
    type
        Description of returned object.

    """


    print("This program will conduct an analysis of San Francisco On Street Parking Tickets.")
    print("How much you do will depend on user input, please answer all open questions (non-numeric input) with Y or N")

    choice = input("Do you need to create the Database?")


    if choice =="Y":

        print("Importing and processing data")

        db.create_ticket_data.main()
        #db.create_street_data.main()

        choice = input('Would you like to do some exploratory analyis?')




    #Explore
    choice = input("Would you like to do some exploring?")
    if choice == 'Y':

        print('Loading Data in usable form for analysis')

        ticket_data, address_data = load_data_explore()
        choice = input('Welcome to the Exploratory Section. You wanna See some charts? We got plenty')

        if choice == 'Y':
            generate_plots(ticket_data, address_data)


        choice = input('Would you like to look up some license plates? ')
        if choice == 'Y':
            while choice == 'Y':

                querystring = input('What license plate?')
                create_ticket_map(querystring, ticket_data)

                choice = input('Would you like another?')



        choice = input('Would you like to create some heatmaps? ')

        if choice == 'Y':
            while choice == 'Y':

                querystring = input('What would you like to filter on? (Please refer to readme for instructions)')
                create_heatmap_query(querystring)

                choice = input('Would you like another?')

        streetvolume = gpd.read_file(proc_loc + 'final_streets/SF_Street_Data.shp')
        streetvolume = streetvolume.to_crs(epsg = 4326)

        choice = input('Would you like to see a volume plot? ')
        if choice == 'Y':
            volume_maps(ticket_data, streetvolume)


        choice = input('Would you like to plot some tickets colored by type? ')
        if choice == 'Y':
            colored_ticket_map(ticket_data, address_data, streetvolume)


        print('You have made it through the Exploratory section!')






    #Model Creation
    choice = input('Would you like to create the model?')

    if choice == 'Y':
        print("Welcome to the initial analyis.")

        print('Loading Data into usable form')

        streets = create_street_data()
        choice = input("Would you like to see a map of all the streets we've identified as Residential Overtime Areas?")

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
                means, stds = split_pop_test(streets, count, False, False, 'volume only model')
                print('The difference between our best and worst population means was {:.1%}'.format((1 - (means[10]/ means[1]))))

                done = input('Are you done? ')


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




        print("We're now going to include street parking availabiliy.")

        streets = create_street_data_parking()

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

                means, stds = split_pop_test(streets, count, False, True, 'volume only model w parking')
                print('The difference between our best and worst population means was {:.1%}'.format((1 - (means[10]/ means[1]))))

                done = input('Are you done? ')


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

        choice = input('Would you like to save the model?')

        if choice == 'Y':
            streets.to_pickle(proc_loc + 'FinalModel.pkl')




    #Parking CDF
    choice = input('Would you like to estimate how long you can park before receiving a ticket?')

    if choice == 'Y':
        print('Loading Data ')
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



    #Street Cleaning Anlysis
    choice = input('Would you like to create some plots on street cleaning?')

    if choice == 'Y':
        print('Loading Data into form for street cleaning')

        ticks = load_data_cleaning()
        print('creating total tickets per month ')

        tick_per_month(ticks)

        print('creating sweep success by number of sweeps')

        sweep_per_month(ticks)

        print('creating sweep success by hour of day')

        sweep_by_hour(ticks)

        print('creating sweep success by day of week')

        by_day_of_week(ticks)



    #Extra Functions
    choice = input('Would you like to use one of the extra functions?')

    if choice == 'Y':
        weekdaydict = {'Mon':1,'Tues':2, 'Wed':3, 'Thurs': 4, 'Fri': 5, 'Sat':6, 'Sun': 7}
        print("Preparing all neccesary datasets")
        address_data, streetvolume, nhoods, streetsweeping = load_data_extra()
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

                resOT = input('Would you like to avoid residential overtime?')

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
                runagain = input('Your entry was invalid, would you like to try again ?')



            runagain = input('Would you like to do another?')



    print('Well thats all I got for you! Thanks for exploring!')


    return



if __name__ =="__main__":
    main()
