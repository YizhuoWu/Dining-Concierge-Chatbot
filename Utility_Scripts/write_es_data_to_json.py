import boto3
import requests
import json
ENDPOINT = "https://search-player1-d3jkybybbtkkttph3oq65feyja.us-west-2.es.amazonaws.com"
dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
table = dynamodb.Table('yelp-restaurants')

scan = table.scan()
count = 0
headers = {'Content-Type' : 'application/json'} # copy from curl
counter = 1

while True:
    for item in scan["Items"]:

        print(item["category"],item["business_id"],count)
        y = dict()
        y["index"] = dict()
        y["index"]["_index"] = "yelp-restaurants"
        y["index"]["_id"] = counter
        counter+=1

        r =dict()
        r["category"] = item["category"]
        r["business_id"] = item["business_id"]

        count += 1
        with open("DATA.json",'a') as json_file:
            json.dump(y,json_file)
            json_file.write('\n')
        with open("DATA.json",'a') as json_file:
            json.dump(r,json_file)
            json_file.write('\n')

    if 'LastEvaluatedKey' in scan:
        # did not end
        scan = table.scan(ExclusiveStartKey=scan['LastEvaluatedKey'])
    else:
        break
    
print("a total of {} records is added".format(count))
