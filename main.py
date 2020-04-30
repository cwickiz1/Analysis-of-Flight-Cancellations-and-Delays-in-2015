import pymongo
import math
import datetime
from dijkstra import Dijkstra, shortestPath


EXIT_CODE = 'q'

def validate_airline(db,val):
    search = list(db.airlines.find({'IATA_CODE': val}))
    if(len(search) == 0):
        return 1
    return 0


def validate_airport(db,val):
    search = list(db.airports.find({'IATA_CODE': val}))
    if(len(search) == 0):
        return 1
    return 0


def validate_day_month_year(db,day,month,year):
    now = datetime.datetime.now()
    if (year < 0 or year > now.year) or (month < 1 or month > 12):
        return 1
    days = len(db.flights.distinct('DAY', {'YEAR': year, 'MONTH': month}))
    if (day < 1 or day > days):
        return 1
    return 0


def validate_month_year(month,year):
    now = datetime.datetime.now()
    if (year < 0 or year > now.year) or (month < 1 or month > 12):
        return 1
    return 0


def compare_dates(m1, d1, y1, m2, d2, y2):
    if y1 < y2:
        return True
    elif y1 == y2:
        if m1 < m2:
            return True
        elif m1 == m2:
            if d1 <= d2:
                return True
            else:
                return False
        else:
            return False
    else:
        return False


#get average delay of airline
def execute_query_1(db):
    airline = input("\nEnter airline IATA Code: ").upper()

    while validate_airline(db,airline):
        airline = input("\nReenter airline: ").upper()

    month = int(input("Enter month: "))
    year = int(input("Enter year: "))

    while validate_month_year(month,year):
        month = int(input("Reenter month: "))
        year = int(input("Reenter year: "))

    val = list(db.flights.aggregate([
                                { '$match': {'AIRLINE': airline, 'MONTH': month, 'YEAR': year} },
                                { '$group': { '_id' : 'null', 'avg_delay' : { '$avg': "$ARRIVAL_DELAY" } } }
                                ]))
    try:
        avg = val[0]['avg_delay']
        airline = db.airlines.find_one({'IATA_CODE': airline})["AIRLINE"]
        print(f"\nAverage delay of {airline} for {month}/{year} is {avg}")
    except:
        print("\nNo valid entries for %d/%d" % (month, year))


#Get Best 5 Days of the Month
def execute_query_2(db):
    month = int(input("\nEnter month: "))
    year = int(input("Enter year: "))

    while validate_month_year(month,year):
        month = int(input("\nReenter month: "))
        year = int(input("Reenter year: "))

    val = list(db.flights.aggregate([
                                { '$match': {'MONTH': month, 'YEAR': year}},
                                { '$group': { '_id' : "$DAY", 'avg_delay' : { '$avg': "$ARRIVAL_DELAY" } } },
                                { '$sort' : { 'avg_delay' : 1 } }
                                ]))
    try:
        for index in range(5):
            entry = val[index]
            print("\nDay: %d\tAvg Delay: %.2f" % (entry['_id'], entry['avg_delay']))
    except:
        print("\nNo valid entries for %d/%d" % (month, year))

#Get Direct Distance
def execute_query_3(db):
    #ABE ABI
    airport1 = input("\nEnter first Airport IATA Code: ").upper()

    while validate_airport(db,airport1):
        airport1 = input("\nInvalid form for first Airport (IATA Code in form of 3 alphabetical characters), re-enter: ").upper()

    airport2 = input("\nEnter second Airport IATA Code: ").upper()

    while validate_airport(db,airport2):
        airport2 = input("\nInvalid form for first Airport (IATA Code in form of 3 alphabetical characters), re-enter: ").upper()

    air1 = db.airports.find_one({'IATA_CODE': airport1})
    air2 = db.airports.find_one({'IATA_CODE': airport2})
    rad = 6371
    lat1 = math.radians(air1['LATITUDE'])
    lat2 = math.radians(air2['LATITUDE'])
    lat_diff = math.radians(air2['LATITUDE']-air1['LATITUDE'])
    long_diff = math.radians(air2['LONGITUDE']-air1['LONGITUDE'])
    a = math.sin(lat_diff/2) * math.sin(lat_diff/2) + math.cos(lat1) * math.cos(lat2) * math.sin(long_diff/2) * math.sin(long_diff/2);
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = (rad * c) * 0.62137
    print("\nDistance between %s and %s is %.2f miles" % (air1['AIRPORT'], air2['AIRPORT'], d))

#Get most cancelled
#For a date range get top 5 most cancelled airlines
def execute_query_4(db):
    while True:
        month1 = int(input("\nEnter start month: "))
        day1 = int(input("Enter start day: "))
        year1 = int(input("Enter start year: "))

        month2 = int(input("\nEnter end month: "))
        day2 = int(input("Enter end day: "))
        year2 = int(input("Enter end year: "))

        if (not validate_day_month_year(db,day1,month1,year1)) and (not validate_day_month_year(db,day2,month2,year2)):
            if compare_dates(month1, day1, year1, month2, day2, year2):
                break
            else:
                print("\nStart date %d/%d/%d after End date %d/%d/%d" % (month1, day1, year1, month2, day2, year2))
        else:
            print("\nInvalid date entered")

    val = list(db.flights.aggregate([
                                { '$match': { '$and': [ { 'MONTH': { '$gte': month1, '$lte': month2 } }, { 'DAY': { '$gte': day1, '$lte': day2 } }, { 'YEAR': { '$gte': year1, '$lte': year2 } } ] } },
                                { '$group': { '_id' : "$AIRLINE", 'sum_cancel' : { '$sum': "$CANCELLED" } } },
                                { '$sort' : { 'sum_cancel' : -1 } }
                                ]))
    try:
        for index in range(5):
            entry = val[index]
            airline = db.airlines.find_one({'IATA_CODE': entry['_id']})['AIRLINE']
            print("\nRank: %d\nAirline: %s\n# of Cancellations: %d" % (index+1, airline, entry['sum_cancel']))
    except:
        print("\nNo valid entries for %d/%d/%d to %d/%d/%d\n" % (month1, day1, year1, month2, day2, year2))

#Get Statistics of Departure Airport
def execute_query_5(db):
    airport = input("\nEnter departure airport: ").upper()

    while validate_airport(db,airport):
        airport = input("\nInvalid form for first Airport (IATA Code in form of 3 alphabetical characters), re-enter: ").upper()

    month = int(input("\nEnter month: "))
    year = int(input("Enter year: "))

    while validate_month_year(month,year):
        month = int(input("\nReenter month: "))
        year = int(input("Reenter year: "))

    days = len(db.flights.distinct('DAY', {'YEAR': year, 'MONTH': month}))
    if days == 0:
        print("\nNo valid entries for %d/%d" % (month, year))
        return
    airport_name = db.airports.find_one({'IATA_CODE': airport})["AIRPORT"]
    strings = []
    for day in range(1,days+1):
        data = list(db.flights.find({'ORIGIN_AIRPORT': airport, 'DAY': day, 'MONTH': month, 'YEAR': year, 'CANCELLED': 0, 'DIVERTED': 0}))

        taxi_total = 0
        taxi_max = 0
        taxi_min = float('inf')
        departure_total = 0;
        departure_max = float('-inf')
        departure_min = float('inf')
        length = len(data)
        for i in data:
            taxi_total += int(i['TAXI_OUT'])
            departure_total += int(i['DEPARTURE_DELAY'])
            if taxi_max < int(i['TAXI_OUT']): taxi_max = int(i['TAXI_OUT'])
            if taxi_min > int(i['TAXI_OUT']): taxi_min = int(i['TAXI_OUT'])
            if departure_max < int(i['DEPARTURE_DELAY']): departure_max = int(i['DEPARTURE_DELAY'])
            if departure_min > int(i['DEPARTURE_DELAY']): departure_min = int(i['DEPARTURE_DELAY'])

        taxi_total = taxi_total/length
        departure_total = departure_total/length
        sd_taxi = 0
        sd_departure = 0
        for i in data:
            sd_taxi += (int(i['TAXI_OUT'])-taxi_total)**2
            sd_departure += (int(i['DEPARTURE_DELAY'])-departure_total)**2

        sd_taxi = math.sqrt(sd_taxi/length)
        sd_departure = math.sqrt(sd_departure/length)
        strings.append("%d\t|%d\t\t|%.2f\t|%.2f\t|%.2f\t|%.2f\t\t|%.2f\t|%.2f\t|%.2f\t|%.2f" % (day, length, taxi_total, taxi_max, taxi_min, sd_taxi, departure_total, departure_max, departure_min, sd_departure))
    print(f"\nDeparture data for {airport_name}\n")
    print("\nDay\t|# of Flights\t|Taxi Out\t\t\t\t|Departure Delay")
    print("\t\t\t|Avg\t|Max\t|Min\t|SD\t\t|Avg\t|Max\t|Min\t|SD")
    for entry in strings:
        print(entry)

#Get Statistics of Arrival Airport
def execute_query_6(db):
    airport = input("\nEnter arrival airport: ").upper()

    while validate_airport(db,airport):
        airport = input("\nInvalid form for first Airport (IATA Code in form of 3 alphabetical characters), re-enter: ").upper()

    month = int(input("\nEnter month: "))
    year = int(input("Enter year: "))

    while validate_month_year(month,year):
        month = int(input("\nReenter month: "))
        year = int(input("Reenter year: "))

    days = len(db.flights.distinct('DAY', {'YEAR': year, 'MONTH': month}))
    if days == 0:
        print("\nNo valid entries for %d/%d\n" % (month, year))
        return
    airport_name = db.airports.find_one({'IATA_CODE': airport})["AIRPORT"]
    strings = []
    for day in range(1,days+1):
        data = list(db.flights.find({'DESTINATION_AIRPORT': airport, 'DAY': day, 'MONTH': month, 'YEAR': year, 'CANCELLED': 0, 'DIVERTED': 0}))
        taxi_total = 0
        taxi_max = 0
        taxi_min = float('inf')
        arrival_total = 0
        arrival_max = float('-inf')
        arrival_min = float('inf')
        length = len(data)
        for i in data:
            taxi_total += int(i['TAXI_IN'])
            arrival_total += int(i['ARRIVAL_DELAY'])
            if taxi_max < int(i['TAXI_IN']): taxi_max = int(i['TAXI_IN'])
            if taxi_min > int(i['TAXI_IN']): taxi_min = int(i['TAXI_IN'])
            if arrival_max < int(i['ARRIVAL_DELAY']): arrival_max = int(i['ARRIVAL_DELAY'])
            if arrival_min > int(i['ARRIVAL_DELAY']): arrival_min = int(i['ARRIVAL_DELAY'])

        taxi_total = taxi_total/length
        arrival_total = arrival_total/length
        sd_taxi = 0
        sd_arrival = 0
        for i in data:
            sd_taxi += (int(i['TAXI_IN'])-taxi_total)**2
            sd_arrival += (int(i['ARRIVAL_DELAY'])-arrival_total)**2

        sd_taxi = math.sqrt(sd_taxi/length)
        sd_arrival = math.sqrt(sd_arrival/length)
        strings.append("%d\t|%d\t\t|%.2f\t|%.2f\t|%.2f\t|%.2f\t\t|%.2f\t|%.2f\t|%.2f\t|%.2f" % (day, length, taxi_total, taxi_max, taxi_min, sd_taxi, arrival_total, arrival_max, arrival_min, sd_arrival))
    print(f"\nArrival data for {airport_name}\n")
    print("\nDay\t|# of Flights\t|Taxi In\t\t\t\t|Arrival Delay")
    print("\t\t\t|Avg\t|Max\t|Min\t|SD\t\t|Avg\t|Max\t|Min\t|SD")
    for entry in strings:
        print(entry)


#Find Flight Path
def execute_query_7(db):
    graph = {}
    year = int(input("\nEnter year: "))
    month = int(input("Enter month: "))
    day = int(input("Enter travel date: "))

    while validate_day_month_year(db,day,month,year):
        year = int(input("\nReenter year: "))
        month = int(input("Reenter month: "))
        day = int(input("Reenter travel date: "))

    vertexes = list(db.airports.find({}, { 'IATA_CODE': 1 } ))
    for v in vertexes:
        graph[v['IATA_CODE']] = {}

    edges = db.flights.aggregate([
                                { '$match': {'MONTH': month, 'DAY': day, 'YEAR': year, 'CANCELLED': 0, 'DIVERTED': 0} },
                                { '$group': { '_id' : {'origin':"$ORIGIN_AIRPORT", 'destination':"$DESTINATION_AIRPORT", 'dist':"$DISTANCE"} } },
                                ])

    for i in edges:
        graph[i['_id']['origin']][i['_id']['destination']] = i['_id']['dist']

    orig = input("Enter origin airport: ").upper()
    while validate_airport(db,orig):
        orig = input("Invalid form for first Airport (IATA Code in form of 3 alphabetical characters), re-enter: ").upper()

    dest = input("Enter destination airport: ").upper()
    while validate_airport(db,dest):
        dest = input("Invalid form for first Airport (IATA Code in form of 3 alphabetical characters), re-enter: ").upper()

    try:
        distances, path = shortestPath(graph,orig,dest)
        print("\nFlight path from %s to %s" % (orig,dest))
        path_str = path[0]
        for v in range(1,len(path)):
            path_str = path_str+" ----> "+path[v]
        print(f"\n{path_str}")
        print("\nTotal flight distance (miles): %d" % distances[dest])
        head = path[0]
        for index in range(1,len(path)):
            data = list(db.flights.find({'ORIGIN_AIRPORT': head ,'DESTINATION_AIRPORT': path[index] ,'MONTH': month, 'DAY': day, 'YEAR': year, 'CANCELLED': 0, 'DIVERTED': 0}))
            orig_name = db.airports.find_one({'IATA_CODE': head})['AIRPORT']
            dest_name = db.airports.find_one({'IATA_CODE': path[index]})['AIRPORT']
            print("\nFlight %s to %s ---- Distance: %d" % (orig_name, dest_name, graph[head][path[index]]))
            print("\nFlight #\tDeparture Time\tArrival Time")
            for ent in data:
                print("%d\t\t|%d\t\t|%d" % (ent['FLIGHT_NUMBER'], ent['SCHEDULED_DEPARTURE'], ent['SCHEDULED_ARRIVAL']))
            head = path[index]
    except:
        print("\nNo path between %s and %s" % (orig, dest))


#Worst Airplane to Fly (Min 10 flights)
def execute_query_8(db):
    year = int(input("\nEnter year: "))
    month = int(input("Enter month of travel: "))

    while validate_month_year(month,year):
        year = int(input("\nReenter year: "))
        month = int(input("Reenter month: "))

    average = list(db.flights.aggregate([
                                { '$match': {'MONTH': month, 'YEAR': year} },
                                { '$group': { '_id' : "$TAIL_NUMBER", 'avg_delay' : { '$avg': "$ARRIVAL_DELAY" }, 'count': { '$sum': 1 }}},
                                { '$sort' : { 'avg_delay' : -1 } }
                                ]))
    try:
        average = [x for x in average if x['count'] > 10]
        worst = average[0]
        print("\nWorst plane to fly %s had an average delay of %.2f minutes for %d flight(s) for %d/%d" % (worst['_id'], worst['avg_delay'], worst['count'], month, year))
    except:
        print("\nNo valid entries for %d/%d" % (month, year))

def help(db):
    for v in COMMANDS:
        print("\n%s: %s\n" % (v, COMMANDS.get(v)[1]))

COMMANDS = {
        '1': (execute_query_1, "Enter the IATA Code of an airline and a year to get the average delay of the airline for that year"),
        '2': (execute_query_2, "Given a month, return the date with the least average delay and the average delay"),
        '3': (execute_query_3, "Given two airport IATA Codes, return the direct distance between them"),
        '4': (execute_query_4, "For a date range find how many cancellations each airline had and rank top 5"),
        '5': (execute_query_5, "For a given Departure Airport, return the average, min, max and standard deviation of each section of delay, broken down by day"),
        '6': (execute_query_6, "For a given Arrival Airport, return the average, min, max and standard deviation of each section of delay, broken down by day"),
        '7': (execute_query_7, "Given two airport ids, how many flights it takes to connect them within a day of travel"),
        '8': (execute_query_8, "For a given month in a year, which specific airplane has the worst delay average with a minimum of 10 flights"),
        "help": (help, "Ask for definitions of COMMANDS")
}

def main():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['flights']
    print("\nFlight Delay Database")
    command = input(f"\nSelect command (1-8), help for query info, {EXIT_CODE} to exit: ")
    while command != EXIT_CODE:
        try:
            run = COMMANDS.get(command)[0]
            run(db)
        except ValueError:
            print("\nNon-numbers entered for date")
        except:
            print("\nCommand error")
        command = input(f"\nSelect command (1-8), help for query info, {EXIT_CODE} to exit: ")

    client.close()

main()
