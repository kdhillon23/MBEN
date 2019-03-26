import sqlite3
import csv

#Read data from CSV into a new database called XYZ.db
DBNAME = 'XYZ.db'

#start of funct to set up db
def db_setup():

    #start of attempt to create DB
    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
    except Exception as e:
        print("Error creating XYZ db: ",e)
        conn.close()
    #end of attempt to create DB

    #start of attempt to drop QRS table
    try:
        statement = '''
        DROP TABLE IF EXISTS 'Course';
        '''
        cur.execute(statement)
        conn.commit()
    except Exception as e:
        print("Error dropping Course table: ",e)
        conn.close()
    #end of attempt to drop QRS table

    #start of attempt to create QRS table
    try:
        statement = '''
        CREATE TABLE 'Course' (
        'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'Framework' TEXT,
        'Subject' INTEGER,›
        'Course' REAL
        );
        '''
        cur.execute(statement)
        conn.commit()
    except Exception as e:
        print("Error creating Course table: ",e)
        conn.close()

    #end of attempt to create QRS table
#end of funct to set up db

def db_insert():

    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    #start insert of ABC.csv
    try:
        with open('Sheet1.csv', encoding='utf-8') as thisCSV:
            csvReader = csv.reader(thisCSV)
            next(csvReader, None)
            for row in csvReader:
                #here's where you process rows & row data using list manipulation

                insertion = (None,row[0],row[3])
                statement = 'INSERT INTO Course '
                statement += 'VALUES (?,?,)'
                cur.execute(statement,insertion)
                conn.commit()
    except Exception as e:
        print("Error inserting Sheet1.csv: ",e)
        conn.close()
    #end insert of ABC.csv



if __name__ == "__main__":
    db_setup()
    db_insert()
