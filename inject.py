from bs4 import BeautifulSoup as soup
import requests
import psycopg2


def get_action(x):

    # return single gene_name and action/actions
    action = []
    gene_name =[]

    for i in x.findAll("div",{"class": "row"}):
        for j in (i.div.dl.findAll("div",{"class":"badge badge-pill badge-action"})):
            action.append(j.text)

    for k in x.findAll("div",{"class": "row"}):
        for l in (k.findAll("div",{"class":"col-sm-12 col-lg-7"})):
             gene_name.append((l.dl.findAll("dd",{"class":"col-md-7 col-sm-6"}))[2].text)

    return (gene_name,action)

def get_gene_name_action(z):

    # get the list of gene_name and action associated with it
    comp_lst = []
    dist = {}
    if len(z.find_all(id='targets')) != 0:
       tar = ((((z.find_all(id='targets'))[0]).findAll("div", {'class': 'bond-list'})[0]).findAll("div", {'class': 'bond card'}))

       for i in tar:
           #go through each gene_name and action taken on it
            comp_lst.append(get_action(i))
            if (len(get_action(i)[0]) > 0):
                 dist.update({get_action(i)[0][0] : get_action(i)[1]})

    # return dictionary where key is gene_name and value is list of action/actions
    return dist

def get_smiles(y):
    # get all the SMILES associated with the identifier
    smile = []
    sim = (y.findAll("div", {"class": "card-content px-md-4 px-sm-2 pb-md-4 pb-sm-2"}))[0]

    for i in (sim.findAll("dd", {"class": "col-xl-10 col-md-9 col-sm-8"})):
        for j in ((i.findAll("div", {"class": "wrap"}))):
            smile.append(j.text)

    # check if SMILES is associated with the identifier
    if len(smile) != 0:
        return (smile[2])
    else:
        return "null"

def get_external_links(w):
    external_lst = []
    dist2 ={}
    external = (w.findAll("dd", {"class": "col-xl-10 col-md-9 col-sm-8"}))

    # get all the external links provided in the web-page for particular identifier
    for i in external:
        for j in (i.findAll("dl", {"class": "inner-dl"})):
            external_lst.append(j)

    # fetch the name of the link and the URL for the link and add it to a dictionary
    # key is name of the link and value is the URL
    for i in (external_lst):
        if len(i.findAll("dt", {"id": "description"})) == 0:
            data_dd = (i.find_all("dd"))
            data_dt = (i.find_all("dt"))

            for l, j in zip(data_dt, data_dd):
                dist2.update({l.text: j.a["href"]})

    return dist2

def create_table(cur):
    #Create the schemas and the tables in the postgreSQL Database
    cur.execute("DROP SCHEMA IF EXISTS DrugBank CASCADE;")
    cur.execute("create schema DrugBank;")
    print("DrugBank schema created")
    cur.execute("set search_path to DrugBank;")
    cur.execute("DROP TABLE IF EXISTS identifier_smiles")
    cur.execute("create table identifier_smiles(identifier varchar(10) CONSTRAINT firstkey  PRIMARY KEY, smiles varchar(120));")
    print("identifier_smiles table created")
    cur.execute("DROP TABLE IF EXISTS identifier_genename_action")
    cur.execute("create table identifier_genename_action(identifier varchar(10) not null, gene_name varchar(50) not null, action text[]);")
    print("identifier_genename_action table created")
    cur.execute("DROP TABLE IF EXISTS identifier_external_links")
    cur.execute("create table identifier_external_links(identifier varchar(10) not null, name varchar(50) not null, link text);")
    print("identifier_external_links table created")

def inject_data(cur, data):
    # inject the data into postgres database
    cur.execute("set search_path to DrugBank;")
    for j in data:
        cur.execute("insert into identifier_smiles values (%s,%s);", (j[1], j[2]))
        for k,l in j[3].items():
            #Check if their is corresponding action on the given gene name
            if len(l) != 0 :
                cur.execute("insert into identifier_genename_action values (%s,%s,%s);", (j[1], k, l))
            else:
                cur.execute("insert into identifier_genename_action values (%s,%s,%s);", (j[1], k, []))
        for x, y in j[4].items():
             cur.execute("insert into identifier_external_links values (%s,%s,%s);", (j[1], x, y))

def main():

        #get the postgreSQL connection details from user
        server_text = input("Enter a PostgreSQL server name: ")
        SERVER = (server_text)
        db_text = input("Enter a PostgreSQL database name: ")
        DB = (db_text)
        user_text = input("Enter a PostgreSQL user name: ")
        USER = (user_text)
        port_text = input("Enter a PostgreSQL port number: ")
        PORT = int(port_text)
        password_text = input("Enter a PostgreSQL password: ")
        PASSWORD = (password_text)

        #URL to drug bank
        endpoint = 'https://www.drugbank.ca/drugs/'
        # list of identifiers
        identifier = ['DB00619', 'DB01048', 'DB14093', 'DB00173', 'DB00734', 'DB00218', 'DB05196','DB09095', 'DB01053', 'DB00274']
        final_data =[]
        print("Initiating web scrapping.... \n")

        for i in identifier:
                #perform web-scrapping operation on the drugbank website
                # fetch the details (Identifier, SMILES, gene_name, action, external links)
                URL = endpoint+i
                final =[]
                page = requests.get(URL)
                # check if the page exists
                if page.reason == "OK":
                        data = soup(page.text,"lxml")
                        final.append(URL)
                        final.append(URL.split("/")[-1])  # get the URL
                        final.append(get_smiles(data))    # get the SMILES for the respective identifier
                        final.append(get_gene_name_action(data))   # get the gene name and action for the respective identifier
                        final.append(get_external_links(data))   # get the external link for the respective identifier
                        final_data.append(final)   #append al the details to a single list
        print("Web scrapping completed \n")
        #Validate the connection to postgres
        try:
                con = psycopg2.connect(
                    host= SERVER,
                    database= DB,
                    user=USER,
                    port = PORT,
                    password=PASSWORD)
        except (Exception, psycopg2.Error) as error:
                print("Unable to connect to postgres !!!! \n")
                print(error)
                return False

        cur = con.cursor()
        cur.execute("SELECT version();")
        record = cur.fetchone()
        print("You are connected to - ", record, "\n")
        print("Injecting data to PostgreSQL ...... \n")
        create_table(cur)
        inject_data(cur,final_data)
        con.commit()
        con.close()
        print("Completed injection \n")
        print("PostgreSQL connection is closed \n")


if __name__ == "__main__":
    print("This program will perform web scrapping operation and injecting the data to the PostgreSQL")
    main()
