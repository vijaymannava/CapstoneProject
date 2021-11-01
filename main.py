
import newsrapidAPI
import bbcnews
import rssfeed
#from time import sleep
#import schedule

if __name__ == '__main__':
   title=["finance","sport","covid","cinima","technology","government","politics",
   "education","health","war","environment","economy","business","fashion",
   "entertainment"]
   for i in title:
      # print(i)
        newsrapidAPI.getNews(i)
        bbcnews.NewsFromBBC(i)
        rssfeed.rssdc()

#schedule.every(10).minutes.do(newsrapidAPI.getNews)
#schedule.every(5).minutes.do(bbcnews.NewsFromBBC)
#schedule.every(2).minutes.do(rssfeed.rssdc)
#while True:
   # schedule.run_pending()
   # time.sleep(60)