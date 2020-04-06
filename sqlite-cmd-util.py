"""
Title   :   CRUD : Create, Read, Update and Delete Database tables
Author  :   Cameron
Date    :   2020.04.02
Time    :   8H

Simplified SQLite3 uses for reference.
"""

import sqlite3
import time
import datetime
import pandas as pd

class databaseInteract:
    """
    Objects for creating, viewing, editing and deleting database tables
    WARNING: if importing this class, end your operation with a closeDB() call.
    """
    def __init__(self, database):
        """
        Initialize the requested DB file and create a cursor
        Input   :   STR : database file name (database.db)
        """
        self.con = sqlite3.connect(str(database))   # Create connection variant
        self.c = self.con.cursor()                       # Create Cursor variant
    
    def __enter__(self):
        return self

    def __exit__(self, exec_type, exc_val, exc_tb):
        self.con.commit()
        self.con.close()
    """
    CRUD (Create, Read, Update and Delete)
    """   
    def viewDB(self):
        """
        Simple function to print out all the tables that exist in a db
        """
        print('\n.:Tables in DataBase:')
        self.c.execute('SELECT name from sqlite_master WHERE type= "table"')
        print(self.c.fetchall())
    
    def viewTable(self, tableChoice):
        df = pd.read_sql_query("select * from "+tableChoice, self.con)
        print(df)
    
    def createTable(self, tableName, customColumns):
        """
        Create a table in a database. Create it, if it doesnt exist. Define the columns and the data types. Through a cursor.
        Data types are(5): REAL (Like a float) | TEXT | INTEGER | NULL | BLOB (blob of data stored exactly as it was input)
        Example: CREATE TABLE IF NOT EXISTS table1 (
        id INTEGER PRIMARY KEY,
        project_name TEXT NOT NULL,
        begin_date TEXT,
        end_date TEXT )
        """
        self.c.execute('CREATE TABLE IF NOT EXISTS ' +tableName+' (id INTEGER PRIMARY KEY, '+customColumns+')')
        return
    
    def insertToTable(self, tableChoice, operators, values):
        """
        Input   : tableChoice   : name of the table in the DB
        Input   : operators     : ?,?,? for each value to pass
        Input   : values        : a string of values
        """
        self.con.execute("INSERT OR IGNORE INTO "+tableChoice+" VALUES("+operators+')', values)
        return
    
    def closeDB(self):
        """
        Function to close out the DB when done operation.
        Run this at the end of quary.
        """
        self.c.close()
        self.con.close()




class dbMenu(databaseInteract):
        """
        If needed, a helper object acting as built-in navigator to interact with CRUD class
        """
        def __init__(self, database):
            super().__init__(database)
            self.menu()

        def menu(self):
            """
            Menu function, displays tables in the database, then gives you the option to create another table, or view an existing one.
            """
            databaseInteract.viewDB(self)
            menuChoice = input('Create or View table?[C / V / Q]: ')

            # If Create is chosen, goto createTable()
            if menuChoice == 'C':
                self.userCreatedTable()

            # if V chosen, goto viewTable().
            elif menuChoice == 'V':
                databaseInteract.viewDB(self)
                tableChoice = input('\nType the name of the chosen Table: ')
                databaseInteract.viewTable(self, tableChoice)

                # Add to current table option
                ans = input('Do you want to add to this Table?[y/n]: ')
                if ans == 'n':
                    self.menu()
                else:
                    self.userCreatedInsert(tableChoice)

            # User Quits: Close out DB
            elif menuChoice == 'Q':
                databaseInteract.closeDB(self)

            # User Choses invalid option, back to top of menu.
            else:
                print('Invalid Entry')
                self.menu()
            
        def userCreatedTable(self):
            """
            Function for user to define a table name, and input column and type.
            Input   : STR   : Prompts for table name and column headers
            Output  : STR   : tablename, customColumns
            """
            tableName = input('Enter Table Name: ')
            customColumns = input('Enter First Column and TYPE seperated by a space\nTypes[REAL|TEXT|INTEGER|NULL|BLOB]: ')
            ans = 'y'
            while ans == 'y':
                ans = input('Do you have a New Column and type?[y/n]: ')
                if ans == 'y':
                    print('Columns: '+ customColumns)
                    data = input('Enter Column Name & TYPE in Caps seperated by a space\nTypes [REAL|TEXT|INTEGER|NULL|BLOB]: ')
                    customColumns += ", "+data
            return databaseInteract.createTable(self, tableName, customColumns)

        def userCreatedInsert(self, tableChoice):
            """
            A helper function to create a string of values to input into sql database
            Input   :   str : The name of the table working on
            Output  :   str : Pass along table choice, and the generated string of values to insert
            """
            print('\n.:Columns in Table:')
            df = pd.read_sql_query("SELECT * FROM "+tableChoice, self.con)
            values = []
            operators = ""
            for column in df:
                # Create (?,?) operator variable
                operators += ',?'

                # Print the column ID and ask for a value to enter
                print(column)
                temp = input("Enter Value: ")

                # if it was an int, keep it int.
                try:
                    values.append(int(temp))
                except:
                    values.append(temp)
                
            # sending (?,?) operators and column values to insertToTable()
            self.insertToTable(tableChoice, operators[1:], values)

            # Choose to add another
            ans = input('Another Entry?[y/n]: ')
            # if YES, go to top of menu.
            if ans == 'y':
                self.userCreatedInsert(tableChoice)
            
            #Otherwise commit the changes and go back to main menu
            else:
                self.con.commit()
                dbMenu.menu(self)



# Object calls for testing
# db = dbMenu('test.db')
# db.closeDB()

# test = databaseInteract.dbMenu("Simple DB.db")
