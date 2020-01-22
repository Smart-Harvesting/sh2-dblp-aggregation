import psycopg2
import sys
import time
#import pprint
import datetime

# pip install geotext
from geotext import GeoText

debug = False;

def log(msg, obj="", res='y'):
    if debug:
        if res == "y":
            pprint.pprint("[+] " + str(msg) + str(obj))
        if res == "e":
            pprint.pprint("[-] " + str(msg) + str(obj))

#connection parameters
host = '${hostName}'
port = '${port}'
dbname = '${dbName}'
user = '${userName}'
password = '${password}'

#table and column names used
records_table = '${dblp_historical_records_table}'
proceedings_view = '${dblp_conference_proceedings_view}'
stream_key_col = '${stream_key_col}'
record_key_col = '${record_key_col}'
record_title_col = '${record_title_col}'
cite_key_col = '${cite_key_col}'
event_country_col='${event_country_col}'

conn = psycopg2.connect(user=user, password=password, host=host, port=port, dbname=dbname)
    
cur = conn.cursor()

print("[+] Running..")
t0 = time.time()
#get all conference proceedings
select_stmt = '''
select distinct procs.{}, hist.{}, hist.{} from {} procs join {} hist on hist.{} = procs.{};
'''.format(stream_key_col, record_key_col, record_title_col, proceedings_view, records_table, record_key_col, cite_key_col)

print(select_stmt)

cur.execute(select_stmt)
res = cur.fetchall()
for record in res:
    key = record[1]
    title = record[2]
    #find geolocations in text
    place = GeoText(title)
    result = place.countries
    found_countries = []
    if len(result) < 1:
        # Trying to find Stuff by hand..
        log("No location found -> Trying to identify manually...", obj="", res='e')
        log("Title: ", obj=title, res='e')
        #self.log("Searching for USA and UK", obj="", res='y')
        if "USA" in title:
            log("Found 'USA'", obj="", res='y')
            if "USA" not in found_countries:
                found_countries.append("USA")
        if "UK" in title:
            log("Found 'UK'", obj="", res='y')
            if "UK" not in found_countries:
                found_countries.append("UK")
        if "Netherlands" in title:
            log("Found 'Netherlands'", obj="", res='y')
            if "Netherlands" not in found_countries:
                found_countries.append("Netherlands")
    else:
        for country in result:
            if country not in found_countries:
                found_countries.append(country)

    if len(found_countries) == 1:
        stmt_template = "update {} set {}=\'{}\' where {}=\'{}\';"
        stmt = stmt_template.format(records_table, event_country_col, found_countries[0], record_key_col, key)
        cur.execute(stmt)
        conn.commit()
    else:
        log("Found none or more than 1 country in title")

t1 = time.time()
print("[+] Done - Total Time: " + str(t1-t0))
