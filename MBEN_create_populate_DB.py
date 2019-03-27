import sqlite3
import csv

#Read data from CSV into a new database called XYZ.db
DBNAME = 'MBEN.db'

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

    #start of attempt to drop Subject table
    try:
        statement = '''
        DROP TABLE IF EXISTS 'Subject';
        '''
        cur.execute(statement)
        conn.commit()
    except Exception as e:
        print("Error dropping Subject table: ",e)
        conn.close()
    #end of attempt to drop Subject table

    #start of attempt to drop Course table
    try:
        statement = '''
        DROP TABLE IF EXISTS 'Course';
        '''
        cur.execute(statement)
        conn.commit()
    except Exception as e:
        print("Error dropping Course table: ",e)
        conn.close()
    #end of attempt to drop Course table

    #start of attempt to drop Domain table
    try:
        statement = '''
        DROP TABLE IF EXISTS 'Domain';
        '''
        cur.execute(statement)
        conn.commit()
    except Exception as e:
        print("Error dropping Domain table: ",e)
        conn.close()
    #end of attempt to drop Domain table

    #start of attempt to drop Competency table
    try:
        statement = '''
        DROP TABLE IF EXISTS 'Competency';
        '''
        cur.execute(statement)
        conn.commit()
    except Exception as e:
        print("Error dropping Competency table: ",e)
        conn.close()
    #end of attempt to drop Competency table

    #start of attempt to create Subject table
    try:
        statement = '''
        CREATE TABLE 'Subject' (
        'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'Name' TEXT
        );
        '''
        cur.execute(statement)
        conn.commit()
    except Exception as e:
        print("Error creating Subject table: ",e)
        conn.close()
    #end of attempt to create Subject table

    #start of attempt to create Course table
    try:
        statement = '''
        CREATE TABLE 'Course' (
        'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'Name' TEXT
        );
        '''
        cur.execute(statement)
        conn.commit()
    except Exception as e:
        print("Error creating Course table: ",e)
        conn.close()
    #end of attempt to create Course table

    #start of attempt to create Domain table
    try:
        statement = '''
        CREATE TABLE 'Domain' (
        'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'Name' TEXT
        );
        '''
        cur.execute(statement)
        conn.commit()
    except Exception as e:
        print("Error creating Domain table: ",e)
        conn.close()
    #end of attempt to create Domain table

    #start of attempt to create Competency table
    try:
        statement = '''
        CREATE TABLE 'Competency' (
        'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'Name' TEXT
        );
        '''
        cur.execute(statement)
        conn.commit()
    except Exception as e:
        print("Error creating Competency table: ",e)
        conn.close()
    #end of attempt to create Competency table
#end of funct to set up db

#start of funct to insert CSV into db
def db_insert():

    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    #start insert of Sheet1.csv
    try:
        with open('Sheet1.csv', encoding='utf-8') as thisCSV:
            csvReader = csv.reader(thisCSV)
            next(csvReader, None)
            for row in csvReader:

                subject = row[1]
                course = row[2]
                domain = row[3]
                competency = row[4]

                #start insert of Subject
                try:
                    statement = "SELECT Id from Subject WHERE Name='{}'".format(subject)
                    cur.execute(statement)
                    found = list(cur)
                    if len(found) == 0:
                        raise Exception('Found existing Subject')
                except:
                    print('Inserting Subject', subject)
                    insertion = (None, subject)
                    statement = 'INSERT INTO Subject '
                    statement += 'VALUES (?,?)'
                    cur.execute (statement,insertion)
                    conn.commit()
                    subject_found = cur.lastrowid #var to be used when inserting 'kernel data'
                #end insert of Subject

                #start insert of Course
                try:
                    statement = "SELECT Id from Course WHERE Name='{}'".format(course)
                    cur.execute(statement)
                    found = list(cur)
                    if len(found) == 0:
                        raise Exception('Found existing Course')
                except:
                    print('Inserting Course', course)
                    insertion = (None, course)
                    statement = 'INSERT INTO Course '
                    statement += 'VALUES (?,?)'
                    cur.execute (statement,insertion)
                    conn.commit()
                    course_found = cur.lastrowid #var to be used when inserting 'kernel data'
                #end insert of Course

                #start insert of Domain
                try:
                    statement = "SELECT Id from Domain WHERE Name='{}'".format(domain)
                    cur.execute(statement)
                    found = list(cur)
                    if len(found) == 0:
                        raise Exception('Found existing Domain')
                except:
                    print('Inserting Domain', domain)
                    insertion = (None, domain)
                    statement = 'INSERT INTO Domain '
                    statement += 'VALUES (?,?)'
                    cur.execute (statement,insertion)
                    conn.commit()
                    domain_found = cur.lastrowid #var to be used when inserting 'kernel data'
                #end insert of Domain

                #start insert of Competency
                try:
                    statement = "SELECT Id from Competency WHERE Name='{}'".format(competency)
                    cur.execute(statement)
                    found = list(cur)
                    if len(found) == 0:
                        raise Exception('Found existing Competency')
                except:
                    print('Inserting Competency', competency)
                    insertion = (None, competency)
                    statement = 'INSERT INTO Competency '
                    statement += 'VALUES (?,?)'
                    cur.execute (statement,insertion)
                    conn.commit()
                    competency_found = cur.lastrowid #var to be used when inserting 'kernel data'
                #end insert of Competency
    except Exception as e:
        print("Error inserting Sheet1.csv: ",e)
        conn.close()
    #end insert of Sheet1.csv
#end of funct to insert CSV into db

if __name__ == "__main__":
    db_setup()
    db_insert()