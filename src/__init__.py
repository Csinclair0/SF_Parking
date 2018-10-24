from db.create_ticket_data import *
from db.create_street_data import *
from explore.explore_data import *
from analysis.analysis_initial import *
from analysis.analyis_park import *
from extras.extras import *

if __name__ =="__main__":
    print("")
    print("This program will conduct an analysis of San Francisco On Street Parking Tickets.")
    choice = input("Do you already have the processed data? (Y or N)")

    if choice =="N":
        print("")
        print("Importing and processing data")
        print("")
