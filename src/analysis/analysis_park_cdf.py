def delta_minutes(x,y):
    timedelta = y-x

    return timedelta.seconds / 60


def create_ticket_prob():
    streetdata = pd.read_sql_query('Select lineid, distance, park_supply, speed_ea from street_volume_data', conn)

    ticket_data = pd.read_sql_query("Select * from ticket_data where ViolationDesc = 'RES/OT' ", conn)

    address_data = pd.read_sql_query('Select address, lineid from address_data', conn)

    df = ticket_data.merge(address_data, left_on = 'address', right_on = 'address')
    df = df.merge(streetdata, left_on = 'lineid', right_on = 'lineid')

    c.execute("Select Count(distinct lineid) from ticket_data t1 join address_data t2 on t1.address = t2.address")
    totalticks = c.fetchone()[0]
    c.execute("Select Count(distinct lineid) from ticket_data t1 join address_data t2 on t1.address = t2.address where violationdesc = 'RES/OT'")
    resticks = c.fetchone()[0]

    f['TickDate']= df['TickIssueDate'].apply(lambda x: pd.to_datetime(x).date())
    tix_by_officer = df.groupby(by = ['TickDate','TickBadgeIssued'], as_index = False)['TicketNumber'].size().reset_index(name='counts')

    officer_by_day = tix_by_officer.groupby(by = ['TickDate'], as_index = False).size().reset_index(name='counts')
    avg_officers = officer_by_day['counts'].mean()
    avg_tix = tix_by_officer['counts'].mean()


    average_freeflow_speed = df['speed_ea'].mean() * .50

    validstreet = streetdata[streetdata.park_supply > 0 ]

    average_spots_per_mile = validstreet['park_supply'] / validstreet['distance']
    average_spots_per_mile = average_spots_per_mile.mean()
    print(average_spots_per_mile)
    total_spots_per_day = average_freeflow_speed * (6-avg_tix*2/60) * percent_res * average_spots_per_mile


    mean_parking_spots = df['park_supply'].mean()

    total_spots = mean_parking_spots * df['lineid'].nunique()

    total_spots_checked = total_spots_per_day * avg_officers

    average_checks = total_spots_checked / total_spots

    arrival_rate = 10*60 / average_checks


    from scipy.stats import expon
    x = np.linspace(0,400)
    ax = plt.figure()
    prob = stats.expon.cdf(x=x, scale= arrival_rate)
    plt.plot(x, prob, color = "blue", linewidth = 3)
    plt.title("Probability of a parking enforcement officer passing your car initially")
    plt.xlim(0,400)
    plt.show()


    df['TickDate'] = df['TickIssueDate'].apply(lambda x:  dt.datetime.strftime(pd.to_datetime(x),'%Y-%m-%d'))

    df = df[['TickBadgeIssued', 'TickIssueDate', 'TicketNumber', 'TickIssueTime', 'lineid', 'TickDate']]

    df = df.merge(df, left_on = ['TickDate', 'lineid', 'TickBadgeIssued'], right_on = ['TickDate', 'lineid', 'TickBadgeIssued'])


    df= df[(df.TicketNumber_x != df.TicketNumber_y) & (df.TickIssueTime_y > df.TickIssueTime_x)]
    df['delta'] = df.apply(lambda x: delta_minutes(pd.to_datetime(x['TickIssueDate_x']), pd.to_datetime(x['TickIssueDate_y'])), axis = 1)

    df = df[(df.delta > 120) & (df.delta < 240)]
    df['delta'].hist(bins = 'auto')
    plt.xlabel('Time from Initial Ticket')
    plt.ylabel('Frequency')
    plt.show()

    counts, bin_edges = np.histogram(df['delta'], bins = 'auto', normed = True)

    cdf = np.cumsum(counts)
    plt.plot(bin_edges[1:], cdf/cdf[-1])
    plt.xlabel('Time after initial marking')
    plt.ylabel('Probability')
    plt.title('CDF of return probability after initial marking')
    plt.show()




def create_simulated_data():
    #Add initial arrival time
    x = np.random.random(size = 1000)

    def f(x, arrival):
    return -math.log(1.0 - x) / (1/arrival)

    firstpass = [f(x) for x in x]
    #firstpass = firstpass.rvs(size = 1000)
    #Create discrete random variable from 2nd arrival rate distribution
    values = df['delta']
    probs = 1/ df['delta'].shape[0]
    combination = pd.DataFrame({'val': values, 'probs' : probs})
    df = combination.groupby(by = 'val', as_index = False)['probs'].sum()
    custom = stats.rv_discrete(values = (df['val'], df['probs']))
    secondpass = custom.rvs(size = 1000)

    totalprob =  firstpass + secondpass
