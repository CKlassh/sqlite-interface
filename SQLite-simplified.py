"""
Title   :   CRUD : Create, Read, Update and Delete Database tables
Author  :   Cameron
Date    :   2020.03.31
Time    :   22Minutes

Simplified SQLite3 uses for reference.
"""

import sqlite3
import time
import datetime
import pandas as pd

# Create the connection. con is pretty standard. This will create the .db file
con = sqlite3.connect('Simple DB.db')

# Create the cursor. c is pretty standard
# The cursor does all the execution commands
c = con.cursor()

def main():
    """
    Main Choice for user to navigate, create and add to database tables
    """
    # Main Menu
    print('\nDatabase Creator and Viewer')
    menuChoice = input('Create or View table:[C / V]')

    # If C chosen, goto user_created()
    if menuChoice == 'C':
        user_Created()

    # if V chosen, Display the table in the db and ask the user to type out the name.
    elif menuChoice == 'V':
        print('\nCurrent Tables')
        sql_fetch()
        tableChoice = input('\nType the name of the chosen Table: ')
        read_from_db(tableChoice)

    else:
        print('Invalid Entry')
        main()


"""
CRUD (Create, Read, Update and Delete)
"""
def create_table(tableName, customColumns):
    """
    Create a table in a database. Create it, if it doesnt exist. Define the columns and the data types. Through a cursor.
    Data types are(5): REAL (Like a float), TEXT, INTEGER, NULL, BLOB (blob of data stored exactly as it was input)
    Example: CREATE TABLE IF NOT EXISTS table1 (
            id INTEGER PRIMARY KEY,
            project_name TEXT NOT NULL,
            begin_date TEXT,
            end_date TEXT, )
    """
    # Create a table from user_define() with columns (customColumns) defaulting an id primary key as best practice
    c.execute('CREATE TABLE IF NOT EXISTS ' +tableName+' (id INTEGER PRIMARY KEY, '+customColumns+')')

def user_Created():
    """
    Function for user to define a table name, and input column and type.
    """
    tableName = input('Enter Table Name: ')
    customColumns = input('Enter First Column and TYPE seperated by a space\nTypes[REAL|TEXT|INTEGER|NULL|BLOB]: ')
    ans = 'y'
    while ans == 'y':
        ans = input('Do you have a New Column and type?[y/n]')
        if ans == 'y':
            print('Columns: '+ customColumns)
            data = input('Enter Column Name & TYPE in Caps seperated by a space\nTypes [REAL|TEXT|INTEGER|NULL|BLOB]: ')
            customColumns += ", "+data
    return create_table(tableName, customColumns)

def data_entry(tableChoice):
    """
    Add items to the table. Use the cursor to execute the INSERT command.
    Commit the data, then close the cursor and the connection.
    """
    unix = time.time()
    # datestamp = str(datetime.datetime.fromtimestamp(unix).strftime('%Y-%m-%d %H:%M:%S'))
    names = [description[:] for description in c.description]
    print(names)
    #c.execute("INSERT INTO" +tableChoice+ " (unix, datestamp, keyword, value) VALUES(?, ?, ?, ?)",(unix, datestamp, keyword, value) )
    #con.commit() # Saving the info (No need to close connections until the end)

def read_from_db(tableChoice):
    """
    Helper function to read data from the DB.
    In this case, we're fetching all and defining the rows we want "value = 3"
    then printing it out in a neater layout.
    """
    # Commented out native sql quary
    # c.execute('SELECT * FROM '+tableChoice)
    # data = c.fetchall() # <--- putting the fetchall data into a variable
    # for row in data:
    #     print(row)

    # Pandas quary much cleaner
    df = pd.read_sql_query("select * from "+tableChoice, con)
    print(df)
    ans = input('Do you want to add to this Table?[y/n]: ')
    if ans == 'n':
        return
    else:
        data_entry(tableChoice)

def del_and_update():
    """
    DELETE to delete, UPDATE to SET new data. SELECT good option to review before commiting deletions
    WARNING: There is no undo here, any changes or deleted info is lost for good.
    Use defensive programming here to protect db. And back up regularily.
    Soft Delete, flag it as deleted rather then clearing the data.
    """
    c.execute('SELECT * FROM table1')
    [print(row) for row in c.fetchall()]

    # c.execute('UPDATE stuffToPlot SET value = 99 WHERE value=2')
    # conn.commit()

    # c.execute('SELECT * FROM stuffToPlot')
    # [print(row) for row in c.fetchall()]

    # c.execute('DELETE FROM stuffToPlot WHERE value = 99')
    # conn.commit()

    c.execute('SELECT * FROM tablel WHERE value = 3')
    [print(row) for row in c.fetchall()]
    print('#' * 50)

def sql_fetch():
    """
    Simple function to print out all the tables that exist in a db
    """
    c.execute('SELECT name from sqlite_master WHERE type= "table"')
    print(c.fetchall())
"""
Close out connections to DB when operations are done
"""

#create_table() #comment out as not needed once the file is made.
#data_entry() # Commented out, as this was just to manually add data.

if __name__ == '__main__':
   main()


    # read_from_db()
    # del_and_update()
    # sql_fetch()

c.close()
con.close()
