#!/usr/bin/env python
# coding: utf-8

# In[ ]:
# Zach showed me a much nicer way to put stuff in a db

import mysql.connector
import pandas as pd
import os
import re
import glob


def check_dates(db_dict, table_name):
        #returns a complete (deduped) list of dates from the table table_name
        #print("Checking for dates already in the database. Data from these dates will not be extract")
        cnx = mysql.connector.connect(user=db_dict['USER'], password=db_dict['PASSWORD'],
                                  host=db_dict['HOST'],
                                  database=db_dict['DATABASE_NAME'])
        cursor = cnx.cursor()
        #insert a check dates function here!!
        
        query = 'SELECT date FROM {} GROUP BY date'.format(table_name)
        cursor.execute(query)
        rows = cursor.fetchall()
        cnx.commit()
        cnx.close()
        return [row[0] for row in rows]
    
def get_files(storage_folder, subdirectory):
    #This returns a complete list of csv files within the given subdirectory.  
    #It prepends the subdirectory name to each filename.
    cwd = os.getcwd()
    os.chdir(storage_folder + '/' + subdirectory)
    
    files=[]
    for filename in glob.glob('*.csv'):
        files.append(os.getcwd() + '/' + filename)
    os.chdir(cwd)
    return files
    
#addresses the issue where article names were recently changed.  The name within the url is constant.
def fix_url(article):
    x=re.findall('/[^/]*/$',article)
    try:
        x=x[0]
    except:
        x=re.findall('/[^/]*$',article)
        try:
            return x[0]
        except:
            return article
    return x

#creating flexible line-insertion query
def insert_lines_template(table_name, columns):
    ss = ''
    for name in columns:
        ss += r"%s,"
    ss = ss[:-1]
    ss = " (" + ss + ");"
    query = '''insert into {0} ({1}) VALUES'''.format(table_name, ', '.join(columns)) + ss
    return query

def truncate_cols(df, long_cols=['query', 'page']):
    #this is to avoid an entry size issue when inserting the row into the db.  
    #queries shouldn't be longer than is allowed for VARCHAR(200) MySQL data type, and if they are, they're probably 
    #mistakes!  I'll leave them in the db just in case, though
    
    for name in long_cols:
        if name in df.columns:
            df[name] = df[name].apply(lambda x: x[:200])
    
    return df
def insert_files(files, db_dict, table_name, directory : str = '', do_date_check = True):
    '''Inserts files from a list of files into a specific database table.  Data columns should be of the 
    appropriate datatype for the table.
    parameters:
    files is a string or a list of files
    db_dict must contain values for {'USER':,'PASSWORD':,'HOST':,'DATABASE_NAME':,'TABLE_NAME':}
    directory is a string.  It should denote the directory/ where  the files are stored.  Use it if the directory
    name is not included in the file names.
    check_dates is a boolean.  Should be marked True if you want to avoid insert data from an already existing date in the database
    return:
    None'''
    
    #how do i make sure it inserts them into the correct columns?
    #TO IMPLEMENT: THE ABOVE!
    
    #turning files into a list  if it's not already one

    if type(files) == str:
        files = [files]
        
    if do_date_check == True:
        print('Checking dates.')
        dates=check_dates( db_dict, table_name )  
    
    for file in files:
   
        if do_date_check == True and file[-14:-4] in dates:
            print('The date {} is already represented in the table {} and will not be added.'.format(file[-14:-4], table_name))
        else:
            if do_date_check == True and file[-14:-4] not in dates:
                print(file, ' is from date not already in database and will be inserted.')
                
            df = pd.read_csv(file)
            if 'Unnamed: 0' in df.columns:
                df.drop(columns = ['Unnamed: 0'], inplace=True)
            print("inserting file ", file, ' containing ', len(df), ' rows.') 
            
            df['page'] = df['page'].apply(lambda x: fix_url(x))
            print('Urls have been corrected.  e.g. ', df['page'][2])
            df = truncate_cols(df) 
 
            data = [tuple(row) for row in df.values]

            #now insert the dataframe into the table.  Warning: if the table is large, you'll need to change your database configuration
            #if it's VERY large, then you'll have to break it into pieces to use this technique
            cnx = mysql.connector.connect(user=db_dict['USER'], password=db_dict['PASSWORD'],
                              host=db_dict['HOST'],
                              database=db_dict['DATABASE_NAME'])
            cursor = cnx.cursor()

            query = insert_lines_template(table_name,df.columns)

            #this row could be turned into a loop, where slices of data are fed into the cursor.  It's not necessary here, but...
            #imagine that df contained a huge amount of data...
            start = 0
            increment = 10

            while start < len(df):
                finish = start + increment
                finish = min(len(df),finish)
                cursor.executemany(query, data[start:finish])
                start = finish
                cnx.commit()
            
    try:
        cnx.close()
    except:
        print('unable to close the connection')
        return
            

