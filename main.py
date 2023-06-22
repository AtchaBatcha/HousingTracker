import datetime

import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
import json
# from functions import *
import sqlalchemy
import mysql.connector


def db_connection():

    engine = sqlalchemy.create_engine('mysql+mysqlconnector://housing_market:Atchabatcha1@localhost/housing_data')

    return engine

def retrieve_all_db_records():

    """Retrieves all records from the rightmove database, if duplicate house_id's are present,
    take the most recent one based on date added to db"""

    engine = db_connection()
    query = """SELECT T.*
                FROM (
                     SELECT *,
                     ROW_NUMBER() OVER (PARTITION BY house_id ORDER BY dateaddedtodb DESC) AS ROWNumber
                    FROM rightmove_houses
                     ) AS T
                WHERE ROWNumber = 1;"""

    with engine.connect() as connection:
        df = pd.read_sql(query, connection,index_col='ID')

    df.drop(columns='ROWNumber', inplace=True)

    return df


def sql_insert_dataframe(df):

    engine = db_connection()

    df = df.rename(columns={'id':'house_id'})

    with engine.connect() as connection:
        df.to_sql('rightmove_houses', connection, if_exists='append', index=False)


def test_connection():

    engine = db_connection()
    try:
        connection = engine.connect()
        print('connection successful')
        connection.close()
    except:
        print('connection failed')


def expand_list_dictionary(input_list:list) -> list:
    """Function takes in a list of dictionaries, if the dictionary has any nested dictionary items within,
    this function expands these elements into a new list, followed by the deletion of the original nested item"""

    output_list = []

    for element in input_list:

        dict_to_append = {}
        keys_to_remove = []
        for keys, values in element.items():

            if type(values) == dict:
                keys_to_remove.append(keys)
                for sub_key, sub_value in values.items():
                    if not type(sub_value) == list:
                        dict_to_append[sub_key] = sub_value

        element.update(dict_to_append) #appends expanded dictionary elements

        for keys in keys_to_remove:
            del element[keys]

        output_list.append(element)

    return output_list


def convert_to_datetime(df, list_cols):
    """Function receives a dataframe with list of cols to be converted from string format
        (YYYY-MM-DDTHH:MM:SSZ where T and Z are letters) to a datetime format"""

    for col in list_cols:
        df[col] = pd.to_datetime(df[col], format='%Y-%m-%dT%H:%M:%SZ')

    return df


def reduce_list_dict(input_list: list) -> list:
    """Function to reduce a list of dictionaries down to some desired set of keys"""
    useful_keys = {'id',
                   'bedrooms',
                   'bathrooms',
                   'propertySubType',
                   'listingUpdateReason',
                   'listingUpdateDate',
                   'amount',
                   'firstVisibleDate',
                   'longitude',
                   'latitude',
                   'propertyUrl',
                   'displayAddress',
                   'transactionType',
                   'students',
                   'auction',
                   'displayStatus',
                   }

    new_list = []

    for house in input_list:
        list_pairs = {}
        for keys in useful_keys:
            list_pairs[keys] = house[keys]

        new_list.append(list_pairs)

    return new_list

def scrape_rightmove_data():

    def request_page(page_index: int, return_count=False):

        url = f'https://www.rightmove.co.uk/property-for-sale/find.html?searchType=SALE&' \
              f'locationIdentifier=REGION%5E399&index={page_index}&insId=1&radius=0.0&' \
              f'minPrice=&maxPrice=&minBedrooms=&maxBedrooms=&displayPropertyType=&' \
              f'maxDaysSinceAdded=&includeSSTC=true&_includeSSTC=on&sortByPriceDescending=' \
              f'&primaryDisplayPropertyType=&secondaryDisplayPropertyType=&oldDisplayPropertyType=&' \
              f'oldPrimaryDisplayPropertyType=&newHome=&auction=false'

        r = requests.get(url)
        doc =BeautifulSoup(r.text, 'html.parser')

        script = doc.find('script', string=re.compile(r'window\.jsonModel'))

        main_string = script.string

        properties_string = main_string[len('window.jsonModel = '):]

        page_dictionary = json.loads(properties_string)

        property_count = int(page_dictionary['resultCount'])
        properties = page_dictionary['properties']

        if return_count is not False:
            return properties, property_count

        return properties

    page_index = 0

    properties_list_dict = []

    properties, property_count = request_page(page_index, True)

    properties_list_dict += properties

    for i in range(24,property_count-24,24):
        properties = request_page(i)
        properties_list_dict +=properties

    with open('./data/property_list_22Jun.json', 'w') as f:
        json.dump(properties_list_dict,f)

    file_path = './data/property_list_22Jun.json'

    with open(file_path, 'r') as file:
        property_list = json.load(file)


    property_list_expanded = expand_list_dictionary(property_list)

    property_list_reduced = reduce_list_dict(property_list_expanded)


    property_list_df = pd.DataFrame(property_list_reduced)


    convert_to_date_time_cols = ['listingUpdateDate', 'firstVisibleDate']
    property_list_df = convert_to_datetime(property_list_df,convert_to_date_time_cols)

    property_list_df['dateaddedtodb'] = pd.to_datetime(datetime.datetime.now())
    property_list_df['lastcheck'] = pd.to_datetime(datetime.datetime.now())

    return property_list_df

def update_database(property_list_df, db_records_df):
    """This function contains the main workflow and performs the following steps
        1) Current data is retrieved from the sql database and stored in a pandas dataframe
        2) Fresh data from the website is scrapped and also stored in a pandas dataframe
        3) A check is made between the 2 dataframe against the housing_id column. New ID's are seperated into a dataframe
        3b) If ID is already present in database, a comparison is made between the database and the newly scraped data,
            if no change, only the last_checked_datetime column is updated. If there has been a change, then a new entry
            is added to the database with the new entry.
        3c) If an ID is present in the database but NOT in the scrapped data, the house is assumed sold and added as a new entry

        In this way the database can keep track of changes in the market
        """

    """To do list: Create fake updates on several entries, would also serve well as part of a test suite"""
    



#
# property_list_df = scrape_rightmove_data()
db_records_df = retrieve_all_db_records()
db_records_df.to_csv('./data/db_records.csv')

#
# sql_insert_dataframe(property_list_df)

