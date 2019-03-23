import sqlite3
import csv

#Read data from CSV into a new database called XYZ.db
DBNAME = 'XYZ.db'

#start of funct to set up db
def db_setup():

    #start of attempt to create DB
    try:
        conn = sqlite3.connect('DBNAME')
        cur = conn.cursor()
    except Exception as e:
        print("Error creating XYZ db: ",e)
        conn.close()
    #end of attempt to create DB

    #start of attempt to drop QRS table
    try:
        statement = '''
        DROP TABLE IF EXISTS 'QRS';
        '''
        cur.execute(statement)
        conn.commit()
    except Exception as e:
        print("Error dropping QRS table: ",e)
        conn.close()
    #end of attempt to drop QRS table

    #start of attempt to create QRS table
    try:
        statement = '''
        CREATE TABLE 'QRS' (
        'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'col1' TEXT,
        'col2' INTEGER,
        'col3' REAL
        );
        '''
        cur.execute(statement)
        conn.commit()
    except Exception as e:
        print("Error creating QRS table: ",e)
        conn.close()
    #end of attempt to create QRS table
#end of funct to set up db

def db_insert():

    conn = sqlite3.connect('DBNAME')
    cur = conn.cursor()

    #start insert of ABC.csv
    try:
        with open('ABC.csv', encoding='utf-8') as thisCSV:
            csvReader = csv.reader(thisCSV)
            next(csvReader, None)
            for row in csvReader:
                #here's where you process rows & row data using list manipulation

                insertion = (None,row[0],row[1],row[2])
                statement = 'INSERT INTO QRS '
                statement += 'VALUES (?,?,?,?)'
                cur.execute(statement,insertion)
                conn.commit()
    except Exception as e:
        print("Error inserting ABC.csv: ",e)
        conn.close()
    #end insert of ABC.csv



if __name__ == "__main__":
    db_setup()
    db_insert()
