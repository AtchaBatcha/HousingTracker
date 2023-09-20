"""Contains functions which assist in general data manipulations"""
import pandas as pd
import datetime
import json

def expand_list_dictionary(input_list:list) -> list:
    """Function takes in a list of dictionaries, if the dictionary has any nested dictionary items within,
    this function expands these elements into a new list, followed by the deletion of the original nested item"""

    output_list = []

    for element in input_list:

        dict_to_append = {}
        keys_to_remove = []
        for keys, values in element.items(): # looking into each value in dictionary

            if type(values) == dict:
                keys_to_remove.append(keys)
                for sub_key, sub_value in values.items():
                    if not type(sub_value) == list:
                        dict_to_append[sub_key] = sub_value

            if type(values) == list:
                for value in values:
                    if type(value) == dict:
                        keys_to_remove.append(keys)
                        for sub_key, sub_value in value.items():
                            if not type(sub_value) == list:
                                dict_to_append[sub_key] = sub_value


        element.update(dict_to_append) #appends expanded dictionary elements

        for keys in keys_to_remove:
            del element[keys]

        output_list.append(element)

    return output_list


def convert_to_datetime(df, list_cols, test=False):
    """Function receives a dataframe with list of cols to be converted from string format
        (YYYY-MM-DDTHH:MM:SSZ where T and Z are letters) to a datetime format
        The test argument is only for when a test is being run, for normal operation, default to false"""


    if not test:
        for col in list_cols:
            df[col] = pd.to_datetime(df[col], format='%Y-%m-%dT%H:%M:%SZ')
    else:
        for col in list_cols:
            df[col] = pd.to_datetime(df[col], format='%Y-%m-%dT%H:%M')


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
                   'addedOrReduced',
                   'summary',
                   }

    new_list = []

    for house in input_list:
        list_pairs = {}
        for keys in useful_keys:
            list_pairs[keys] = house[keys]

        new_list.append(list_pairs)

    return new_list

def open_json_scraped(path):

    """Snippet of another function, used to load in a sample json"""

    with open(path, 'r') as file:
        property_list = json.load(file)

    property_list_expanded = expand_list_dictionary(property_list)

    property_list_reduced = reduce_list_dict(property_list_expanded)

    property_list_df = pd.DataFrame(property_list_reduced)

    convert_to_date_time_cols = ['listingUpdateDate', 'firstVisibleDate']
    property_list_df = convert_to_datetime(property_list_df,convert_to_date_time_cols)

    return property_list_df

def add_timestamps_to_df(df: pd.DataFrame, new_house=False, given_date=False) -> pd.DataFrame:
    """Purpose of function is to add the lastcheck and dateAddedtoDb timestamps"""
    """Gives current time as added to DB if this is a new row"""

    if new_house:
        if given_date:
            df.loc[:,'dateaddedtodb'] = pd.to_datetime(given_date, dayfirst=True, format= '%d/%m/%Y')
        else:
            df.loc[:, 'dateaddedtodb'] = pd.to_datetime(datetime.datetime.now())
    df.loc[:, 'lastcheck'] = pd.to_datetime(datetime.datetime.now())

    return df