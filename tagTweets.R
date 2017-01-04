source('preprocess.R')
source('helper.R')
library(rmongodb)
library(plyr)
collection <- 'twitterstream.cwctweets'
newCollection <- 'twitterstream.cwcTweetsTagged'
mg <- mongo.create() ## Mongo DB Instance Created
print("MongoDB Connection Opened")
cur <- mongo.find(mg, ns=collection)

getTotalTweetsCount <- function(){
  mongo.count(mg, collection)    
}

mongoDestroy <- function(){
  mongo.destroy(mg)
  print("MongoDB Connection Closed")
}

convert <- function( totalTweets = getTotalTweetsCount()){
  tweetCounter <- 0
  while(tweetCounter < totalTweets && mongo.cursor.next(cur)){
    if(mongo.is.connected(mg)){
      #print(mongo.get.databases(mg))
      #print(mongo.get.database.collections(mg, 'twitterstream'))
      buf <- mongo.bson.buffer.create()
      tweets<-data.frame(stringsAsFactors = F)
      mongo.cursor.next(cur) 
      tmp = mongo.bson.to.list(mongo.cursor.value(cur))
      
      tmp.df = as.data.frame(t(unlist(tmp)), stringsAsFactors = F)
      text<-as.data.frame(tmp.df$tweet.txt)
      #print(text)
      tweets<-rbind.fill(tweets,text)
      
      colnames(tweets)<-c("text")
      #print(tweets)
      tweet <- tweets
      processed_tweet <- preprocess(tweet$text)
      sentiment <- as.list(getEmotionLabel(processed_tweet))
      tmp$tweet <- c(tmp$tweet, sentiment = sentiment$BEST_FIT)
      b <- mongo.bson.from.list(tmp)
      ok <- mongo.insert(mg, newCollection, b)
      
    }else {
      print("Not connected to MongoDB")
    }
    tweetCounter <- tweetCounter + 1
    
  }
}
convert()
mongoDestroy()