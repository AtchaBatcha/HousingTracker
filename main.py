import datetime
import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
import numpy as np
from database_functions import *
from helper_functions import *
import yaml
import random
import time

def scrape_rightmove_data(properties_list_dict, region):

    """Main data scraping function, task is to access the rightmove website using a given URL for a certain region
     (Currently Dagenham) and scrape web data. To get all entries, the function must iterate for all pages on the link
     will also need to use selenium


     """
    def request_page(page_index: int, region:str, return_count=False):

        url = f'https://www.rightmove.co.uk/property-for-sale/find.html?searchType=SALE&' \
              f'locationIdentifier=REGION%{region}&index={page_index}&numberOfPropertiesPerPage=100&insId=1&radius=0.0&' \
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

        property_count = int(page_dictionary['resultCount'].replace(',',''))
        properties = page_dictionary['properties']

        if return_count is not False:
            return properties, property_count

        return properties

    page_index = 0

    properties, property_count = request_page(page_index, region, True)

    properties_list_dict += properties


    if property_count > 24:
        for i in range(24, property_count-24, 24):
            properties = request_page(i, region)
            if properties is not None:
                properties_list_dict += properties
            print(i)
            time.sleep(random.randint(1, 5))  # put in to get around scraper detectors

    return properties_list_dict

def read_json(file_path):

    """Read a json files contained scraped data, mainly used for backup purposes"""

    with open(file_path, 'r') as file:
        property_list = json.load(file)

    property_list_expanded = expand_list_dictionary(property_list)

    property_list_reduced = reduce_list_dict(property_list_expanded)

    property_list_df = pd.DataFrame(property_list_reduced)


    convert_to_date_time_cols = ['listingUpdateDate', 'firstVisibleDate']
    property_list_df = convert_to_datetime(property_list_df,convert_to_date_time_cols)

    property_list_df.rename(columns={'id':'house_id'}, inplace=True)

    property_list_df = property_list_df.drop_duplicates()

    return property_list_df

def clean_property_dictionary(property_list_dict: list):
    """After data is scraped, data then needs to be cleaned
     input: list of dictionaries containing property data from web scrape
     output: dataframe """

    property_list_expanded = expand_list_dictionary(property_list_dict)

    property_list_reduced = reduce_list_dict(property_list_expanded)

    property_list_df = pd.DataFrame(property_list_reduced)

    convert_to_date_time_cols = ['listingUpdateDate', 'firstVisibleDate']
    property_list_df = convert_to_datetime(property_list_df, convert_to_date_time_cols)

    property_list_df.rename(columns={'id': 'house_id'}, inplace=True)

    property_list_df = property_list_df.drop_duplicates()

    return property_list_df




def check_updates(property_list_df: pd.DataFrame, db_records_df: pd.DataFrame, scrape_date=None) -> pd.DataFrame:
    """This function contains the main workflow and performs the following steps
        1) Current data is retrieved from the sql database and stored in a pandas dataframe
        2) Fresh data from the website is scrapped and also stored in a pandas dataframe
        3) A check is made between the 2 dataframe against the housing_id column. New ID's are seperated into a
         dataframe
        3b) If ID is already present in database, a comparison is made between the database and the newly scraped data,
            if no change, only the last_checked_datetime column is updated. If there has been a change, then a new entry
            is added to the database with the new entry.
        3c) If an ID is present in the database but NOT in the scrapped data, the house is assumed sold and added as a
         new entry

        In this way the database can keep track of changes in the market

        Note: The scrape date argument is useful for repopulating the the data format "dd/mm/yyyy"
        """

    unique_values_scrape = np.setdiff1d(property_list_df['house_id'], db_records_df['house_id'])
    unique_values_in_db = np.setdiff1d(db_records_df['house_id'], property_list_df['house_id'])

    common_values = np.intersect1d(property_list_df['house_id'], db_records_df['house_id'])

    new_additions_df = []

    """Series of if statements to manage various conditions"""

    if unique_values_scrape.any():
        """If id's are present in the website scrape and not in the database, implies newly added houses,
        This if statement sets up values for upload to DB as new entries"""
        new_houses = property_list_df.loc[property_list_df['house_id'].isin(unique_values_scrape)]
        new_houses = new_houses.copy()  # Copy used to silence copy warning from pandas, perhaps investigate this later
        new_houses = add_timestamps_to_df(new_houses, True, scrape_date)
        new_additions_df.append(new_houses)


    if unique_values_in_db.any():
        """If id's are present on the db but not on the website, add new row for house which includes removal
         from website"""
        """Need to first pull all houseid's with datetakenoffwebsite - ignore these"""

        houses_off_website = db_records_df.loc[db_records_df['house_id'].isin(unique_values_in_db)]
        houses_off_website = houses_off_website.copy()
        houses_off_website = add_timestamps_to_df(houses_off_website,True, scrape_date)

        houses_off_website.loc[:, 'datetakenoffwebsite'] = pd.to_datetime(datetime.datetime.now())
        new_additions_df.append(houses_off_website)



    if common_values.any():
        """If common_values, see if anything has changed. If a change is detected, then new entry is added to db"""
        common_houses_scrape = property_list_df[property_list_df['house_id'].isin(common_values)]
        common_houses_db = db_records_df[db_records_df['house_id'].isin(common_values)]
        common_houses_db = common_houses_db.copy()
        common_houses_db.reset_index(drop=True)

        common_houses_db.loc[:,'source'] = 'database'
        common_houses_scrape.loc[:,'source'] = 'webscrape'

        combined_df = pd.concat([common_houses_scrape,common_houses_db])
        columns_list = list(combined_df.columns)

        columns_to_remove = ['source', 'lastcheck', 'dateaddedtodb', 'datetakenoffwebsite', 'summary']

        for col in columns_to_remove:
            columns_list.remove(col)

        updated_houses =combined_df.drop_duplicates(subset=columns_list, keep=False)
        updated_houses = updated_houses.sort_values('house_id')

        updated_houses_to_upload = updated_houses[updated_houses['source']=='webscrape']

        updated_houses_to_upload = updated_houses_to_upload.drop(columns='source')
        updated_houses_to_upload = add_timestamps_to_df(updated_houses_to_upload,True)

        new_additions_df.append(updated_houses_to_upload)


    concat_df_for_db = pd.concat(new_additions_df)

    return concat_df_for_db

def main():
    """ As part of the backup system, all scrapes are stored as a json file. This is useful in the event that the
          database is lost, at which point the json files for each day the program is run can then be reloaded into the system

          Once saved, the json is then read using the read_json function, with the intention of converting the json data into
          a dataframe with only the data required. A list of the useful attributes can be found in the reduce_list_dict function"""

    with open('config.yaml','r') as file:
        config = yaml.safe_load(file)

    region_dictionary = {
                         'Leytonstone': '5E87521'
    }


    properties_list_dict =[]

    for i in region_dictionary.values():
        property_list_dict = scrape_rightmove_data(properties_list_dict, i)

    current_date_string = datetime.datetime.today().strftime('%Y-%m-%d')
    with open(f'data/scrape_jsons/property_list_test_{current_date_string}.json', 'w') as f:
        json.dump(property_list_dict, f)

    property_list_df = clean_property_dictionary(property_list_dict)

    # scrape_property_list_df = read_json('./data/scrape_jsons/property_list_2023-07-15.json')
    db_connection = SqlOperations(config)
    db_records_df = db_connection.retrieve_all_current_db_records()

    df = check_updates(property_list_df, db_records_df)
    db_connection.insert_dataframe(df)


if __name__ == '__main__':
    main()

