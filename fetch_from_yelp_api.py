# -*- coding: utf-8 -*-
from __future__ import print_function

import boto3
import argparse
import json
import pprint
import requests
import sys
import urllib
import datetime

# Load 5000 Restaurants near Mahattan(including New Jersey, Queens, Newark and Brooklyn)
# Responses Return all required parameters of Assignment 1 Part 5(DynamoDB)
# Push data into DynamoDB

try:
    # For Python 3.0 and later
    from urllib.error import HTTPError
    from urllib.parse import quote
    from urllib.parse import urlencode
except ImportError:
    # Fall back to Python 2's urllib2 and urllib
    from urllib2 import HTTPError
    from urllib import quote
    from urllib import urlencode

API_KEY= "FkCaoe6Ar5tyQuIluXwEkFn7bVDg7tKGc9l579mFCpP8IZrDxziUQMf94kUDuznb5QRhPtdUdinm1SxNhg1EQb1g8tJn_Ba56g6IzdjfjS_gnQf4w5e3NM9vjnoqYHYx"
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.

SEARCH_LIMIT = 50
TOTAL_NUM = 0
COUNTER = 0




def request(host, path, api_key, url_params=None):
    """Given your API_KEY, send a GET request to the API.
    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        API_KEY (str): Your API Key.
        url_params (dict): An optional set of query parameters in the request.
    Returns:
        dict: The JSON response from the request.
    Raises:
        HTTPError: An error occurs from the HTTP request.
    """
    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }

    print(u'Querying {0} ...'.format(url))

    response = requests.request('GET', url, headers=headers, params=url_params)

    return response.json()



def search(api_key, term, location,offset):


    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': SEARCH_LIMIT,
        'offset':offset
    }
    return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)


def get_business(api_key, business_id):

    business_path = BUSINESS_PATH + business_id

    return request(API_HOST, business_path, api_key)


def query_api(cate_term, location,table):
    """Queries the API by the input values from the user.
    Args:
        term (str): The search term to query.
        location (str): The location of the business to query.
    """
    term = cate_term + " food"
    path_name = "r"
    for offset in range(0,200,50):
        response = search(API_KEY, term, location,offset)

        businesses = response.get('businesses')

        y = dict()
        y["yelp-restaurants"] = []


    

        if not businesses:
            print(u'No businesses for {0} in {1} found.'.format(term, location))
            return


        for res in businesses:
            PutRequest = dict()

            put_item = dict()
            
            
            global TOTAL_NUM
            global COUNTER

            if COUNTER == 20:
                COUNTER = 0
            TOTAL_NUM += 1
            print("Total res:",TOTAL_NUM)
            COUNTER += 1
            
            business_id = res['id']

            put_item["business_id"] = dict()
            put_item["business_id"]["S"] = str(business_id) #Business ID
            


            
            name = res['name']

            put_item["name"] = dict()
            put_item["name"]["S"] = name #Name
            
            address = res['location']['display_address']
            separator = ", "
            address_str = separator.join(address)
            put_item["address"] = dict()
            put_item["address"]["S"] = address_str #Address

                
            if 'coordinates' in res.keys():
                coordinates = res['coordinates']
                c_str = ""
                c_str += "latitue: "
                c_str += str(coordinates['latitude'])
                c_str = c_str + ", longtitude: " + str(coordinates['longitude'])
                put_item["coordinates"] = dict()
                put_item["coordinates"]["S"] = c_str #Coordinates

                
            number_of_reviews = res['review_count']
            put_item["number_of_reviews"] = dict()
            put_item["number_of_reviews"]["S"] = str(number_of_reviews) #Number_of_reviews
            
            if 'rating' in res.keys():
                rating = res['rating']
                put_item["rating"] = dict()
                put_item["rating"]["S"] = str(rating) #Rating
                
            zip_code = res['location']['zip_code']
            put_item["zip_code"] = dict()
            put_item["zip_code"]["S"] = str(zip_code) #Zip code


            put_item["category"] = dict()
            put_item["category"]["S"] = cate_term

            t = str(datetime.datetime.now())
            put_item["insertedAtTimestamp"] = dict()
            put_item["insertedAtTimestamp"]["S"] = t


            table.put_item(
                Item = {
                    'business_id': str(business_id),
                    'name': name,
                    'address': address_str,
                    'coordinates':c_str,
                    'number_of_reviews':str(number_of_reviews),
                    'rating':str(rating),
                    'zip_code':str(zip_code),
                    'category':cate_term,
                    'insertedAtTimestamp':t,
                    }
                )
            


def main():

    try:
        term = ["Chinese","Japanese","American","French","Mexican"]
        location = ["Manhattan","Brooklyn","Queens, NY","Newark","Jersey City"]
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('yelp-restaurants')
        print(table.creation_date_time)
        for t in term:
            
            query_api(t, location[3],table)
        print("Total restaurant find near Manhattan: ",TOTAL_NUM)
    except HTTPError as error:
        sys.exit(
            'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
                error.code,
                error.url,
                error.read(),
            )
        )


if __name__ == '__main__':
    main()
