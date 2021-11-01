# importing requests package
import requests
from requests import Response
from kafka import KafkaProducer, producer
from time import sleep
import time
import sys
import json
import os
#import schedule

def NewsFromBBC(title):

    url = " https://newsapi.org/v1/articles"
    querystring = {"q":title,"lang":"en","page":"1","page_size":"25"}
    headers = {
    'x-newsapi-host': "bbc-news",
    'x-newsapi-key': "4dbc17e007ab436fb66416009dfb59a8"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)
    response = response.json()

    def json_serializer(newsDist):
        return json.dumps(newsDist).encode("utf-8")
    newsDist = {}
    i = 0
    try:
       for i in range(len(response['articles'])):
        newsDist.update(title=response['articles'][i]['title'],
                        date=response['articles'][i]['published_date'],
                        summary=response['articles'][i]['summary'],
                        category=response['articles'][i]['topic'],
                        source=response['articles'][i]['link'])
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=json_serializer)
        producer.send('news',json.dumps(newsDist))
        i = i+1
        time.sleep(10)
        print(json.dumps(newsDist))
    except KeyError:

       print("articles is unknown.")

#schedule.every(2).minutes.do(NewsFromBBC)
#while True:
 #   schedule.run_pending()
  #  time.sleep(60)

    

	