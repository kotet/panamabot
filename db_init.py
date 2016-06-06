import pandas as pd
import sqlite3

conn = sqlite3.connect('./panama.db')
cur = conn.cursor()


def Addresses():
    cur.execute('create table Addresses(id int unsigned primary key,address text,countries text);')
    f = pd.read_csv('offshore_leaks_csvs/Addresses.csv',low_memory=False)
    for i in range(0,f.shape[0]):
        query = 'insert into Addresses(id,address,countries) values(?,?,?);'
        cur.execute(query,(str(f.ix[i,'node_id']),str(f.ix[i,'address']),str(f.ix[i,'countries'])))

def Entities():
    cur.execute('create table Entities(id int unsigned primary key,name text);')
    f = pd.read_csv('offshore_leaks_csvs/Entities.csv',low_memory=False)
    for i in range(0,f.shape[0]):
        query = 'insert into Entities(id,name) values(?,?);'
        cur.execute(query,(str(f.ix[i,'node_id']),str(f.ix[i,'name'])))

def Intermediaries():
    cur.execute('create table Intermediaries(id int unsigned primary key,name text);')
    f = pd.read_csv('offshore_leaks_csvs/Intermediaries.csv',low_memory=False)
    for i in range(0,f.shape[0]):
        query = 'insert into Intermediaries(id,name) values(?,?);'
        cur.execute(query,(str(f.ix[i,'node_id']),str(f.ix[i,'name'])))

def Officers():
    cur.execute('create table Officers(id int unsigned primary key,name text);')
    f = pd.read_csv('offshore_leaks_csvs/Officers.csv',low_memory=False)
    for i in range(0,f.shape[0]):
        query = 'insert into Officers(id,name) values(?,?);'
        cur.execute(query,(str(f.ix[i,'node_id']),str(f.ix[i,'name'])))

def all_edges():
    cur.execute('create table all_edges(edge_id INTEGER PRIMARY KEY AUTOINCREMENT,node1 INT UNSIGNED,rel_type text,node2 INT);')
    f = pd.read_csv('offshore_leaks_csvs/all_edges.csv',low_memory=False)
    for i in range(0,f.shape[0]):
        query = 'insert into all_edges(node1,rel_type,node2) values(?,?,?);'
        cur.execute(query,(str(f.ix[i,'node_1']),str(f.ix[i,'rel_type']),str(f.ix[i,'node_2'])))
        
def translation():
    cur.execute('create table translation(rel_type text,translation text);')
    cur.execute('select distinct rel_type from all_edges;')
    query = 'insert into translation(rel_type,translation) values(?,?)'
    for rel_type in cur.fetchall():
        cur.execute(query,(rel_type[0],""))

print('Loading Addresses.csv ...')
Addresses()
print('Loading Entities.csv ...')
Entities()
print('Loading Intermediaries.csv ...')
Intermediaries()
print('Loading Officers.csv ...')
Officers()
print('Loading all_edges.csv ...')
all_edges()
print('Creating translation table ...')
translation()

conn.commit()
conn.close()
