import db.create_ticket_data
import db.create_street_data
#from explore.explore_data import *
#from analysis.analysis_initial import *
#from analysis.analyis_park import *
#from extras.extras import *




def main():
    print("")
    print("This program will conduct an analysis of San Francisco On Street Parking Tickets.")
    choice = input("Do you already have the processed data? (Y or N)")


    if choice =="N":
        print("")
        print("Importing and processing data")
        print("")
        db.create_ticket_data.main()
        db.create_street_data.main()


    #explore_data.main()


if __name__ =="__main__":
    main()
