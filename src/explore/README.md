# Explore Data
This is the exploratory section of the San Francisco Parking Analysis. This will present charts and figures generated during the exploratory phase, as well as provide instructions on how to work through the analysis yourself.

## Instructions

This source is meant to walk through the initial exploratory analysis while also providing the user some input to modify the charts. The first section will be the charts, and will provide the ability to create all charts shown below. At each step, the user will be prompted how many attributes they would like to show. There are only so many colors, so I have also provided the defaults that I used in my charts with each statement. The defaults are provided after the input in the form '(def 10)'. Below shows all the charts generated in this section.

![Tickets by Violation](/reports/figures/explore/TickByViolation.png)

![Tickets by Vehicle Make](/reports/figures/explore/TickByVehMake.png)

![Tickets by Neighborhood](/reports/figures/explore/TickByNhood.png)

![Tickets by Hour](/reports/figures/explore/TickByHour.png)

![Street Clean By Hour](/reports/figures/explore/StreetCleanByHour.png)

![Tickets by hour by Neighborhood](/reports/figures/explore/ByHourByHood.png)

![Tickets by day by type](/reports/figures/explore/ByDayByType.png)

![Vehicle Type Share by Nhood](/reports/figures/explore/VehByNhood.png)

![Ticket Type Share by Nhood](/reports/figures/explore/ShareByHood.png)


![Meters by neighborhood](/reports/figures/explore/MetbyNhood.png)


The next section will be looking up specific license plates, and plotting all of their tickets on a map. This will use folium to put markers on each ticket, and the marker will contain the ticket time and violation desc. The user will be prompted to input a license plate on their own.  See below an example using My own license plate. The file will be saved as an html file and open in browser.

![My Tickets](/reports/maps/7XCS244.html)

The next section will allow the user to create heatmaps, given a query argument. The query argument should be in the form of what would follow a WHERE statement in SQL. Such as "VehMake == 'TSMR'"(shown below, plotting all teslas. )If you don't want to filter anything, just put '1=1'. This will also be saved in an HTML and opened in browser. Let's look at a few below.

![Residential Overtime](/reports/maps/ViolationDescRESOT.html)

This heatmap shows residential overtime tickets, and validates that our cleaning process functioned properly since only a small amount fall outside the limits of the actual residential permit ares. Below is the Tesla Plot.

![Tesla Tickets](/reports/maps/VehMakeTSLA.html)

The next option is to plot the actual street volume on a matplotlib chart, as well as all tickets. The user will be prompted to ask how many tickets they would like to add to the chart.

![Volume Map](/reports/maps/VolumeMap.html)

the last option is to plot tickets on the map and color and their type. The user will be allowed to input how many tickets they would like. an example is shown below.

![Ticket Map](/reports/maps/ColorTicketMap.html){target="_blank"}.

And that is it for the exploratory section! If you didn't get enough, you'll have the ability to generate some more maps in the extras section.
