
import requests
from bs4 import BeautifulSoup
import time
import schedule

def rssdc():
    
 url = "http://www.deccanchronicle.com/rss_feed/"
 resp = requests.get(url)

 soup = BeautifulSoup(resp.content, features="xml")

 items = soup.findAll('item')

 news_items = []

 for item in items:
        news_item = {}
        news_item['category'] = item.category.text
        news_item['title'] = item.title.text
        news_item['summary'] = item.description.text
        news_item['source'] = item.link.text
        news_item['date'] = item.pubDate.text
   
        news_items.append(news_item)

 print(news_items)

schedule.every(3).minutes.do(rssdc)
while True:
    schedule.run_pending()
    time.sleep(60)