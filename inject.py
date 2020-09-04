from bs4 import BeautifulSoup as soup
import requests
import csv
import os
import sys
import psycopg2


def get_gene_name(x):

    action = []
    gene_name =[]

    for i in x.findAll("div",{"class": "row"}):
        for j in (i.div.dl.findAll("div",{"class":"badge badge-pill badge-action"})):
            action.append(j.text)


    for k in x.findAll("div",{"class": "row"}):
        for l in (k.findAll("div",{"class":"col-sm-12 col-lg-7"})):
             gene_name.append((l.dl.findAll("dd",{"class":"col-md-7 col-sm-6"}))[2].text)


    return (gene_name,action)

def get_name_action(z):
    comp_lst = []
    dist = {}
    if len(z.find_all(id='targets')) != 0:
       tar = ((((z.find_all(id='targets'))[0]).findAll("div", {'class': 'bond-list'})[0]).findAll("div", {'class': 'bond card'}))

       for i in tar:
            comp_lst.append(get_gene_name(i))
            if (len(get_gene_name(i)[0]) > 0):
                 dist.update({get_gene_name(i)[0][0] : get_gene_name(i)[1]})

    return dist

def get_smile(y):
    smile = []
    sim = (y.findAll("div", {"class": "card-content px-md-4 px-sm-2 pb-md-4 pb-sm-2"}))[0]

    for i in (sim.findAll("dd", {"class": "col-xl-10 col-md-9 col-sm-8"})):
        for j in ((i.findAll("div", {"class": "wrap"}))):
            smile.append(j.text)
    if len(smile) != 0:
        return (smile[2])
    else:
        return "null"

def get_external_links(w):
    external_lst = []
    final_external = []
    dist2 ={}
    external = (w.findAll("dd", {"class": "col-xl-10 col-md-9 col-sm-8"}))
    for i in external:
        for j in (i.findAll("dl", {"class": "inner-dl"})):
            external_lst.append(j)
    for i in (external_lst):
        if len(i.findAll("dt", {"id": "description"})) == 0:
            data_dd = (i.find_all("dd"))
            data_dt = (i.find_all("dt"))

            for l, j in zip(data_dt, data_dd):
                # final_external.append([i.text, j.a.text])
                dist2.update({l.text: j.a["href"]})

    return dist2

def create_table(cur):

    cur.execute("create schema test4;")
    cur.execute("set search_path to test4;")
    cur.execute("create table iden_smiles(identifier varchar(10) CONSTRAINT firstkey  PRIMARY KEY, smile varchar(120));")
    cur.execute("create table gene_action(identifier varchar(10) not null, gene_name varchar(50) not null, action char(50));")
    cur.execute("create table external_links(identifier varchar(10) not null, name varchar(50) not null, link text);")

def inject_data(cur, data):
    cur.execute("set search_path to test4;")
    for j in data:
        cur.execute("insert into iden_smiles values (%s,%s);", (j[1], j[2]))
        for k,l in j[3].items():
            if len(l) != 0 :
                cur.execute("insert into gene_action values (%s,%s,%s);", (j[1], k, l))
            else:
                cur.execute("insert into gene_action values (%s,%s,%s);", (j[1], k, "null"))
        for x, y in j[4].items():
             cur.execute("insert into external_links values (%s,%s,%s);", (j[1], x, y))

def main():
        endpoint = 'https://www.drugbank.ca/drugs/'
        identifier = ['DB00619', 'DB01048', 'DB14093', 'DB00173', 'DB00734', 'DB00218', 'DB05196','DB09095', 'DB01053', 'DB00274']
        #identifier =['DB00173']
        final_data =[]
        for i in identifier:
                URL = endpoint+i
                final =[]
                '''response = uReq(request)
                page_data = response.read()
                response.close()'''
                page = requests.get(URL).text
                data = soup(page,"lxml")
                final.append(URL)
                final.append(URL.split("/")[-1])
                final.append(get_smile(data))
                final.append(get_name_action(data))
                final.append(get_external_links(data))
                final_data.append(final)

        con = psycopg2.connect(
            host= sys.argv[1],
            database= sys.argv[2],
            user=sys.argv[3],
            password=sys.argv[4])


        cur = con.cursor()
        cur.execute("SELECT version();")
        record = cur.fetchone()
        print("You are connected to - ", record, "\n")
        create_table(cur)
        inject_data(cur,final_data)
        con.commit()
        con.close()
        print("PostgreSQL connection is closed")

if __name__ == "__main__":
    print("This program will perform data web scrapping operation and injecting the data to the postgresql")
    main()
