import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
import json
from functions import *


def reduce_list_dict(input_list: list) -> list:
    """Function to reduce a list of dictionaries down to some desired set of keys"""
    useful_keys = {'id',
                   'bedrooms',
                   'location',
                   'bathrooms',
                   'propertySubType',
                   'listingUpdate',
                   'price',
                   'firstVisibleDate',
                   }

    new_list = []

    for house in input_list:
        list_pairs = {}
        for keys in useful_keys:
            list_pairs[keys] = house[keys]

        new_list.append(list_pairs)

    return new_list

def retrieve_data():

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

    reduced_list = reduce_list_dict(properties)

    with open('./data/property_dict.json', 'w') as f:
        json.dump(reduced_list,f)


# retrieve_data()


