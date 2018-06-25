import json
import sys
import psycopg2
from datetime import datetime
import time
from urllib.request import urlopen, Request
import os
import threading
from collections import defaultdict
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# for loading data from file raw
DIR='D:\\My\\Python\\Projects\\Pol'
# ticker
GET_TICKER="https://poloniexes.com/public?command=returnTicker"
# time to get data
period_sleep=5
SPLIT_BY_COUNT_ROWS = 100000
COUNT_THREADS = 9

sql_select_count = 'select count(*) from ticker'

# POSTGRES

#==============================================================================
# hostname = '127.0.0.1'
# username = 'postgres'
# password = 'postgres'
# database = 'postgres'
# port=5432
# conn = psycopg2.connect( host=hostname, user=username, password=password, dbname=database, port=port )
# cursor = conn.cursor()
# conn.autocommit = False
#==============================================================================
def cleanDb():
#    dbOpen()
    cursor.execute("drop table if exists ticker")
    cursor.execute("drop table if exists pair")
    cursor.execute("drop table if exists depth")
    cursor.execute('''CREATE TABLE if not exists ticker (_id bigserial NOT NULL PRIMARY KEY, id bigserial NOT NULL, created timestamp NOT NULL, pair varchar(16) NOT NULL,
		 		 		 		 		 last double precision NOT NULL, lowestAsk double precision NOT NULL, highestBid double precision NOT NULL, percentChange double precision NOT NULL, baseVolume
		 		 		 		 		 double precision NOT NULL, quoteVolume double precision NOT NULL, isFrozen double precision NOT NULL, high24hr double precision NOT NULL, low24hr double precision NOT NULL);
		 		 		 		 		 DROP SEQUENCE IF EXISTS ticker_id_seq CASCADE;
		 		 		 		 		 CREATE SEQUENCE ticker_id_seq;
		 		 		 		 		 ALTER TABLE ticker ALTER COLUMN _id SET DEFAULT nextval('ticker_id_seq');
		 		 		 		 		 SELECT setval('ticker_id_seq', MAX(_id)) FROM ticker;COMMIT;''')
    cursor.execute('''CREATE TABLE if not exists pair (
                 _id        bigserial NOT NULL PRIMARY KEY,
                 created    timestamp NOT NULL,
                 pair       varchar(16) NOT NULL,
                 ask1       double precision,
                 askVolume1 double precision,
                 ask2       double precision,
                 askVolume2 double precision,
                 ask3       double precision,
                 askVolume3 double precision,
                 ask4       double precision,
                 askVolume4 double precision,
                 ask5       double precision,
                 askVolume5 double precision,
                 bid1       double precision,
                 bidVolume1 double precision,
                 bid2       double precision,
                 bidVolume2 double precision,
                 bid3       double precision,
                 bidVolume3 double precision,
                 bid4       double precision,
                 bidVolume4 double precision,
                 bid5       double precision,
                 bidVolume5 double precision,
                 isFrozen   serial NOT NULL,
                 seq        serial NOT NULL);
                     DROP SEQUENCE IF EXISTS depth_ask_id_seq CASCADE;
		 		 		CREATE SEQUENCE pair_ask_id_seq;
		 		 		ALTER TABLE pair ALTER COLUMN _id SET DEFAULT nextval('pair_id_seq');
		 		 		SELECT setval('pair_id_seq', MAX(_id)) FROM pair;COMMIT;''')
    cursor.execute('''CREATE TABLE if not exists depth (
                 _id        bigserial NOT NULL PRIMARY KEY,
                 created    timestamp NOT NULL,
                 pair       varchar(16) NOT NULL,
                 ask1       double precision,
                 askVolume1 double precision,
                 ask2       double precision,
                 askVolume2 double precision,
                 ask3       double precision,
                 askVolume3 double precision,
                 ask4       double precision,
                 askVolume4 double precision,
                 ask5       double precision,
                 askVolume5 double precision,
                 bid1       double precision,
                 bidVolume1 double precision,
                 bid2       double precision,
                 bidVolume2 double precision,
                 bid3       double precision,
                 bidVolume3 double precision,
                 bid4       double precision,
                 bidVolume4 double precision,
                 bid5       double precision,
                 bidVolume5 double precision,
                 isFrozen   serial NOT NULL,
                 seq        serial NOT NULL);

                 DROP SEQUENCE IF EXISTS depth_id_seq CASCADE;
		 		 	CREATE SEQUENCE depth_id_seq;
		 		 	ALTER TABLE depth ALTER COLUMN _id SET DEFAULT nextval('depth_id_seq');
		 		 	SELECT setval('depth_id_seq', MAX(_id)) FROM depth;COMMIT;''')


def loadTickerOld(filename,filedata):
    created = int(filedata[0])
    json_data = filedata[2]
    data = json.loads(json_data)
    with open('sql_'+filename, 'a') as file:
        for pair in list(data):
            arr = []
            row=data[pair]
    #        arr.append(None)
            arr.append(datetime.fromtimestamp(created))
            arr.append(pair)
            if len(row)==9:
                arr.append(0)
            for item in row:
                arr.append(row[item])
    #        print(arr.pop)
    #            print(item+"="+str(row[item]))
            counter=0
            vals=''
            for item in arr:
                counter+=1
                vals+="'"+str(item)+"'"
                if counter != len(arr):
                    vals+=","
    #        print(vals)

#            cursor.execute('''insert into ticker(created,pair,id,last,lowestAsk,highestBid,
#                            percentChange,baseVolume,quoteVolume,isFrozen,high24hr,low24hr) values(%s)''' % vals)
            file.write('''insert into ticker(created,pair,id,last,lowestAsk,highestBid,percentChange,baseVolume,quoteVolume,isFrozen,high24hr,low24hr) values(%s);\n''' % vals)


def loadTicker(filename,filedata):
    created = int(filedata[0])
    json_data = filedata[2]
    data = json.loads(json_data)
    with open('sql_'+filename, 'a') as file:
        outdict = defaultdict(list)
        for pair in list(data):
            outdict['created']=datetime.fromtimestamp(created)
            outdict['pair']=pair
            row=data[pair]
            outdict.update(row)
            file.write(formatInsertOutput(outdict, 'ticker'))

#            file.write('''insert into ticker(created,pair,id,last,lowestAsk,highestBid,percentChange,baseVolume,quoteVolume,isFrozen,high24hr,low24hr) values(%s);\n''' % vals)
'''
        outdict = defaultdict(list)
        for pair in list(data):
            outdict['created']=datetime.fromtimestamp(created)
            outdict['pair']=pair
            asks=data[pair]['asks']
            for i in range(5):
                outdict['ask'+str(i+1)]=asks[i][0]
                outdict['askVolume'+str(i+1)]=asks[i][1]
#            print(dict(outdict))
            bids=data[pair]['bids']
            counter_bid = 0
            for i in range(min(len(bids),5)):
                counter_bid += 1
                outdict['bid'+str(i+1)]=bids[i][0]
                outdict['bidVolume'+str(i+1)]=bids[i][1]
            if counter_bid < 5:
                for _ in range(5-counter_bid):
#                    print("HERE2:",str(counter_bid+j+1), "counter_bid:",counter_bid, "j:", j)
                    counter_bid += 1
                    outdict['bid'+str(counter_bid)]='null'
                    outdict['bidVolume'+str(counter_bid)]='null'
            outdict["isFrozen"]=data[pair]['isFrozen']
            outdict['seq']=data[pair]['seq']

            file.write(formatInsertOutput(outdict, table))
'''

def test():
    cursor.execute('''insert into ticker(created,pair,id,last,lowestAsk,highestBid,
                    percentChange,baseVolume,quoteVolume,isFrozen,high24hr,low24hr)
                    values('1463919840','BTC_1CR','0','0.00037321','0.00043299','0.00037008','-0.13259424','0.08739452','206.35032853','0','0.00043499','0.00037321');''')
    get_count()

def get_count():
    sql = 'select count(*) from ticker'
    cursor.execute(sql)
    return cursor.fetchone()[0]

def get_count_all():
    print("tables:\n")
    for table in ['depth_bid','ticker','pair_ask','pair_bid','depth_ask']:
        sql = 'select count(*) from '+table
        cursor.execute(sql)
        print(table+':',cursor.fetchone()[0])

def getFiles(DATA_DIR, filename_filter):
    files = []
    for (dirpath, dirnames, filenames) in os.walk(DATA_DIR):
        for file in filenames:
            if file.find(filename_filter)>0 and file.endswith('.txt'):
                files.append(file)
        break
    return files

def formatInsertOutput(dictin, table):
    counter=0
    result = []
    result.append('insert into ')
    result.append(table)
    result.append('(')
    for key, value in dictin.items():
        counter+=1
        result.append(str(key))
        if counter != len(dictin):
            result.append(', ')
    counter=0
    result.append(') values(')
    for key, value in dictin.items():
        counter+=1
        result.append("'")
        result.append(str(value))
        result.append("'")
        if counter != len(dictin):
            result.append(', ')
    result.append(');\n')
    return ''.join(result)

#def toStr2(arr):
#    counter=0
#    result=''
#    for item in arr:
#        counter+=1
#        result+="'"+str(item)+"'"
#        if counter != len(arr):
#            result+=","
#    return result


def loadPairsOrDepth(filename,filedata, table):
    created = int(filedata[0])
    json_data = filedata[2]
    data = json.loads(json_data)
    with open('sql_'+filename, 'a') as file:
        outdict = defaultdict(list)
        for pair in list(data):
            outdict['created']=datetime.fromtimestamp(created)
            outdict['pair']=pair
            asks=data[pair]['asks']
            for i in range(5):
                outdict['ask'+str(i+1)]=asks[i][0]
                outdict['askVolume'+str(i+1)]=asks[i][1]
            bids=data[pair]['bids']
            counter_bid = 0
            for i in range(min(len(bids),5)):
                counter_bid += 1
                outdict['bid'+str(i+1)]=bids[i][0]
                outdict['bidVolume'+str(i+1)]=bids[i][1]
            if counter_bid < 5:
                for _ in range(5-counter_bid):
                    counter_bid += 1
                    outdict['bid'+str(counter_bid)]='null'
                    outdict['bidVolume'+str(counter_bid)]='null'
            outdict["isFrozen"]=data[pair]['isFrozen']
            outdict['seq']=data[pair]['seq']

            file.write(formatInsertOutput(outdict, table))


def loadTickerfromURL(url):
    response = urlopen(url)
    json_data = response.read()
    response.close()
    data = json.loads(json_data)
    count_prev = get_count()
    for pair in list(data):
        arr = []
        row=data[pair]
        arr.append(datetime.now())
        arr.append(pair)
        for item in row:
            arr.append(row[item])
        counter=0
        vals=toStr(arr)

        cursor.execute('''insert into ticker(created,pair,id,last,lowestAsk,highestBid,
                        percentChange,baseVolume,quoteVolume,isFrozen,high24hr,low24hr) values(%s)''' % vals)

    conn.commit()
    count_after = get_count()
    print("loaded returnTicker data with",str((count_after-count_prev)),"records, total",str(count_after),"records")


def loadFile(filename,file):
    count_ticker=0
    count_depth=0
    count_pairs=0
    with open(file, "r") as f:

        data = []
        for line in f:
            arr = line.split(";")
            data.append(arr)
        for n, line in enumerate(data):
            if n % 1000 == 0 and n > 0:
                print('worked out lines:',n)
            if line[1]=='ticker':
                count_ticker+=1
                loadTicker(filename,line)
            elif line[1]=='pairs':
                count_pairs+=1
                loadPairsOrDepth(filename,line,'pair')
            elif line[1]=='depth':
                count_depth+=1
                loadPairsOrDepth(filename,line,'depth')
#        conn.commit()
        print("countTicker: "+str(count_ticker)+", countPairs: "+str(count_pairs)+", countDepth: "+str(count_depth))


def getFilesToLoad(DATA_DIR):
    # get files from directory
    temp = []
    for (dirpath, dirnames, filenames) in os.walk(DIR):
        temp.extend(filenames)
        break

    #delete temp files
    for filename in temp:
        if (filename.find('_sql')>0) and filename.endswith('.txt'):
            os.remove(os.path.join(DATA_DIR,filename))
            '''filename.startswith('sql_results_raw') or '''

#            pass

    files = []
    for filename in temp:
        if filename.startswith('results_raw') and filename.endswith('.txt'):
            files.append(filename)

    print("list of loading files",files)
    count_files=0
    for filename in files:
        if filename.endswith('.txt'):
            count_files+=1
#            count_prev = get_count()
#            print("loading from file("+str(count_files)+" of "+str(len(arr2))+")",filename,"...")
            loadFile(filename,os.path.join(DATA_DIR,filename))
#            count_after = get_count()
#            print("file("+str(count_files)+" of "+str(len(arr2))+")",filename,"loaded with",str((count_after-count_prev)),"records, total",str(count_after),"records")

# split filedata to smallest files
def getSQLUpload():

    files = []
    for (dirpath, dirnames, filenames) in os.walk(DIR):
        for file in filenames:
            if file.find('sql_results_raw')>=0 and file.endswith('.txt'):
                files.append(file)
        break

    for filename in files:
        with open(filename, 'r') as file:
#        cursor.executemany(file.readlines())
            temp_file = open('test.txt', 'w')
            count=0
            try:
                for i, line in enumerate(file):
                    if i % SPLIT_BY_COUNT_ROWS == 0:
                        temp_file.close()
                        count+=1
                        temp_file = open('t'+str(count)+'_'+filename, 'w')
                    else:
                        temp_file.write(line);
            finally:
                temp_file.close()
#            cursor.execute(i)


def insert_func2(conn_insert,filename):
    name = threading.currentThread().getName()
    conn = conn_insert.getconn()
    c = conn.cursor()
    try:
        with open(filename, 'r') as file:
            for line in file:
                c.execute(line)
    except psycopg2.ProgrammingError as err:
        print(name, ": an error occurred; skipping this insert")
        print(err)
    conn.commit()

def insert_func(filename):
    name = threading.currentThread().getName()
    with open(filename, 'r') as file:
        counter = 0
        while file.readline():
            counter += 1
            if counter % 10000 == 0:
                print(name+": "+filename+", "+str(counter))
        print(filename+", "+str(counter))

def do_thread():

#    files = []
#    for (dirpath, dirnames, filenames) in os.walk(DIR):
#        for file in filenames:
#            if file.find('_sql_results_raw')>0 and file.endswith('.txt'):
#                files.append(file)
#        break
    files = []
    for (dirpath, dirnames, filenames) in os.walk(DIR):
        for file in filenames:
            if file.find('_sql')>=0 and file.endswith('.txt'):
                files.append(os.path.join(DIR,file))
        break
    threads = []
    conn_insert = ThreadedConnectionPool(COUNT_THREADS, COUNT_THREADS,  host=hostname, user=username, password=password, dbname=database, port=port )
    print("Creating INSERT threads:")
    for _id in range(COUNT_THREADS):
        print('Thread-'+str(_id+1), 'created')
        t = threading.Thread(None, insert_func2, 'Thread-'+str(_id+1), (conn_insert,'t'+str(_id+1)+"_sql_results_raw.0.txt"))
        t.setDaemon(0)
        threads.append(t)
    print(threads)

    ## really start the threads now
    for t in threads:
        print('Thread-',t,'started')
        t.start()

    # and wait for them to finish
    for t in threads:
        t.join()
        print(t.getName(), "exited OK")


    conn.commit()

def do_thread2():

#    files = []
#    for (dirpath, dirnames, filenames) in os.walk(DIR):
#        for file in filenames:
#            if file.find('_sql_results_raw')>0 and file.endswith('.txt'):
#                files.append(file)
#        break
    files = []
    for (dirpath, dirnames, filenames) in os.walk(DIR):
        for file in filenames:
            if file.find('_sql')>=0 and file.endswith('.txt'):
                files.append(os.path.join(DIR,file))
        break
    threads = []
    conn_insert = ThreadedConnectionPool(COUNT_THREADS, COUNT_THREADS,  host=hostname, user=username, password=password, dbname=database, port=port )
    print("Creating INSERT threads:")
    for _id in range(COUNT_THREADS):
        print('Thread-'+str(_id+1), 'created')
        t = threading.Thread(None, insert_func2, 'Thread-'+str(_id+1), (conn_insert,'t'+str(_id+1)+"_sql_results_raw.0.txt"))
        t.setDaemon(0)
        threads.append(t)
    print(threads)

    ## really start the threads now
    for t in threads:
        print('Thread-',t,'started')
        t.start()

    # and wait for them to finish
    for t in threads:
        t.join()
        print(t.getName(), "exited OK")


    conn.commit()

def main(argv):
#    create or clean db
#    cleanDb()

#   load files
#    for i in range(1000):
#        print("id:", i)
    getFilesToLoad(DIR)
    getSQLUpload()
#    do_thread()
#    get_count_all()
#   load ticker from poloniex
#    while True:
#         loadTickerfromURL(GET_TICKER)
#         print("waiting",period_sleep,"seconds ...")
#         time.sleep(int(period_sleep))



if __name__ == "__main__":
		 main(sys.argv[1:])
