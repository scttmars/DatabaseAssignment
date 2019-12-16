#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May  5 14:02:25 2019

@author: scottmarsden
"""

import configparser

import re

# Get credentials to connect to database
config = configparser.ConfigParser()
config.read_file(open('/Users/scottmarsden/Desktop/credentials.txt')) # point to the correct place where this file is!
dbhost = config['csc']['dbhost']
dbuser = config['csc']['dbuser']
dbpw = config['csc']['dbpw']

# Tell the database server which schema to connect to
dbschema = 'smarsden' # fill in whatever schema you are using (not table, SCHEMA)

# Open database connection
dbconn = pymysql.connect(host=dbhost,
                         user=dbuser,
                         passwd=dbpw,
                         db=dbschema,
                         use_unicode=True,
                         charset='utf8mb4',
                         autocommit=True)
cursor = dbconn.cursor()


#Select Tweets query
selectQuery = "SELECT tid, ttext FROM ferguson_tweets"
#Insert mentions query
insertMentions = "Insert Ignore ferguson_tweets_mentions(mentionID,tid,mention) \
                  VALUES (%s, %s, %s)"
#Insert hashtags query
insertHashtags = "Insert Ignore ferguson_tweets_hashtag(hashtagID,tid,hashtag) \
                  VALUES (%s, %s, %s)"   
#Insert url query
insertUrl = "Insert Ignore ferguson_tweets_url(urlID,tid,url) \
                  VALUES (%s, %s, %s)"   
                  
cursor.execute(selectQuery)
rows = cursor.fetchall()


hashtagId = 1
mentionId = 1
urlId = 1

for tweet in rows:
    tweetID = tweet[0]
    tweetText = tweet[1]
    
    #insert hashtags
    # find hashtags 
    # hashtags start with hashtags and are at least one word character long
    hashtags = re.findall('#\w+', tweetText)
    if hashtags:
        # for each hashtag, print it and put it in the database
        
        for hashtag in hashtags:
            print("inserting", hashtag, "for tweet#", tweetID)
            cursor.execute(insertHashtags, (hashtagId,tweetID, hashtag))
            hashtagId = hashtagId + 1

    #insert mentions
    


    # find mentions 
    # mentions start with @ and are at least one word character long
    mentions = re.findall('@\w+', tweetText)
    if mentions:
        # for each mention, print it and put it in the database
        
        for mention in mentions:
            print("inserting", mention, "for tweet#", tweetID)
            cursor.execute(insertMentions, (mentionId,tweetID, mention))
            mentionId = mentionId + 1
            
    #insert url
    

    # find urls 
    # mentions start with http://t.co/ and are at least one word character long
    urls = re.findall('http://t.co/\w+', tweetText)
    if urls:
        # for each url, print it and put it in the database
        
        for url in urls:
            print("inserting", mention, "for tweet#", tweetID)
            cursor.execute(insertUrl, (urlId,tweetID, url))
            urlId = urlId + 1
            

        
dbconn.close()
    
    