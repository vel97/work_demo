#Scrapping twitter data using snscrape library
#Importing required libraries
import streamlit as st
import pandas as pd
import snscrape.modules.twitter as sntwitter
import pymongo
from pymongo import MongoClient

#Required functions
def scrap_twitter(tweet_search, start_date, end_date, limit):
    tweets_list = []
    for i,tweet in enumerate(sntwitter.TwitterSearchScraper(tweet_search +" "+"since:"+str(start_date)+"until:"+str(end_date)).get_items()):
        if i>limit:
            break
        tweets_list.append([tweet.date, tweet.id, tweet.content, tweet.user.username])
    return tweets_list
    
#Storing the pulled data from twitter in a dataframe
def create_dataframe(tweets_list):
    tweets_df = pd.DataFrame(tweets_list, columns=['Datetime', 'Tweet Id', 'Text', 'Username'])
    return tweets_df

#Converting to CSV and JSON format
def convert_csv(tweets_df):
    return tweets_df.to_csv().encode('utf-8')

def convert_json(tweets_df):
    return tweets_df.to_json().encode('utf-8')

#Declaring variables
#Title
st.title(':orange[Twitter Scrapper]')

#Tweet to be searched
tweet_search = st.text_input("Search hashtag")

#Dates input
start_date = st.date_input("Start date", key = "start_date")

end_date = st.date_input("End date", key = "end_date")

#No_of_tweets to be scrapped
limit = st.number_input("Number of tweets required", key = "limit")

#Scrapping the data
if st.button("Scrap the data"):
    tweets = scrap_twitter(tweet_search, start_date, end_date, limit)
    tweets_df = create_dataframe(tweets)
    st.dataframe(tweets)

#To download the data in required format from streamlit app
#CSV format
    tweets_csv = convert_csv(tweets_df)
    st.download_button(
    label="Download data as CSV",
    data=tweets_df.to_csv(),
    file_name=tweet_search+".csv",
    mime='text/csv',
)

#JSON format
    tweets_json = convert_json(tweets_df)
    st.download_button(
    label="Download data as JSON",
    data=tweets_df.to_json(),
    file_name=tweet_search+".json",
    mime='json',
)

#Pushing data to MongoDB
if st.button('Push to Database'):
    tweets = scrap_twitter(tweet_search, start_date, end_date, limit)
    tweets_df = create_dataframe(tweets)
    pm = MongoClient("mongodb://Guser:Guvi.com3@ac-zstnwxd-shard-00-00.e8i7hjr.mongodb.net:27017,ac-zstnwxd-shard-00-01.e8i7hjr.mongodb.net:27017,ac-zstnwxd-shard-00-02.e8i7hjr.mongodb.net:27017/?ssl=true&replicaSet=atlas-9s52qi-shard-0&authSource=admin&retryWrites=true&w=majority")
    pm1 = pm['Twitter_data']
    data_dict = tweets_df.to_dict('records')
    pmcollections1 = pm1[tweet_search]
    pmcollections1.insert_many(data_dict)
    st.write('Data pushed to Database Succesfully')

#Command to run the script.
#''' streamlit run Tweet_Srap_Streamlit.py '''#
