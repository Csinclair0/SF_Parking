import db.create_ticket_data
import db.create_street_data
from explore.explore_data import *
from analysis.analysis_model import *
from analysis.analyis_park_cdf import *
from analysis.street_cleaning import *
from extras.extras import *




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
    print("")
    print("This program will conduct an analysis of San Francisco On Street Parking Tickets.")
    choice = input("Do you already have the processed data? (Y or N)")


    if choice =="N":
        print("")
        print("Importing and processing data")
        print("")
        db.create_ticket_data.main()
        #db.create_street_data.main()

        choice = input('Would you like to do some exploratory analyis?(Y or N)')


        #Explore
        if choice == 'Y'
            print("")
            print('Loading Data in usable form for analysis')
            print("")
            ticket_data, address_data = load_data()
            choice = input('Welcome to the Exploratory Section. You wanna See some charts? We got plenty.(Y or N)')
            print("")
            if choice == 'Y':
                generate_plots(ticket_data, address_data)


            choice = input('Would you like to look up some license plates? (Y or N)')
            if choice == 'Y':
                while choice == 'Y':
                    print("")
                    querystring = input('What license plate?')
                    create_ticket_map(querystring, ticket_data)
                    print("")
                    choice = input('Would you like another?')


            print("")
            choice = input('Would you like to create some heatmaps? (Y or N)')
            if choice == 'Y':
                while choice == 'Y':
                    print("")
                    querystring = input('What would you like to filter on? (Please refer to readme for instructions)')
                    create_heatmap_query(querystring)
                    print("")
                    choice = input('Would you like another?')

            streetvolume = gpd.read_file(proc_loc + 'final_streets/SF_Street_Data.shp')
            streetvolume = streetvolume.to_crs(epsg = 4326)
            print("")
            choice = input('Would you like to see a volume plot? (Y or N)')
            if choice == 'Y':
                volume_maps(ticket_data, streetvolume)

            print("")
            choice = input('Would you like to plot some tickets colored by type? (Y or N)')
            if choice == 'Y':
                colored_ticket_map(ticket_data, address_data, streetvolume)

            print("")
            print('You have made it through the Exploratory section!')




        #Model Creation
        choice = input('Do you already have the model Created?(Y or N)')
        if choice == 'N':
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
                    means, stds = split_pop_test(streets, count, False, False, 'volume only model')
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




            choice = input("We're now going to include street parking availabiliy.")
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
                interaction_model()

            print("We're going to create the final model now")

            means, stds = final_model(streets)

            choice = input('Would you like to save the model?(Y or N)')
            if choice == 'Y':
                streets.to_pickle(proc_loc + 'FinalModel.pkl')




            #Parking CDF
            choice = input('Would you like to estimate how long you can park before receiving a ticket?')
            if choice == 'Y':
                    with open(proc_loc + 'means.pickle', 'rb') as handle:
                        means = pickle.load(handle)
                    with open(proc_loc + 'stds.pickle', 'rb') as handle:
                        stds = pickle.load(handle)

                    df, streetdata = load_data()
                    print('Creating initial arrival probability')
                    arrival_rate = create_initial_arrival_prob(df, streetdata)

                    print('Creating second arrival probability')
                    second_prob = create_return_distribution(df)

                    secondpass = create_simulated_data(arrival_rate, second_prob)

                    print('Lets look at the difference in populations that we fitted in our model')

                    split_by_pop(arrival_rate, secondpass, means)

                    add_confidence_intervals(arrival_rate, secondpass, means, stds)










if __name__ =="__main__":
    main()
