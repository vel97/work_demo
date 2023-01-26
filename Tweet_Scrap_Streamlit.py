#Scrapping twitter data using snscrape library
import streamlit as st
import pandas as pd
import snscrape.modules.twitter as sntwitter

#Declaring variables
tweet_search = input()
tweets_list = []

#Pulling data from twitter
for i,tweet in enumerate(sntwitter.TwitterSearchScraper(tweet_search +" "+"since:2021-01-01 until:2021-05-31").get_items()):
    if i>100:
        break
    tweets_list.append([tweet.date, tweet.id, tweet.content, tweet.user.username])

#Storing the pulled data from twitter in a dataframe
tweets_df = pd.DataFrame(tweets_list, columns=['Datetime', 'Tweet Id', 'Text', 'Username'])
st.dataframe(tweets_df)

# To download the data in required format from streamlit app
#CSV format
@st.cache
def convert_csv(tweets_df):
    return tweets_df.to_csv().encode('utf-8')

tweets_csv =convert_csv(tweets_df)

st.download_button(
    label="Download data as CSV",
    data=tweets_csv,
    file_name=tweet_search+".csv",
    mime='text/csv',
)

#JSON format
@st.cache
def convert_json(tweets_df):
    return tweets_df.to_json().encode('utf-8')

tweets_json = convert_json(tweets_df)
st.download_button(
    label="Download data as JSON",
    data=tweets_json,
    file_name=tweet_search+".json",
    mime='json',
)

# Data collection in Mongodb from streamlit 
if st.button('Push to Database'):
    import pymongo
    from pymongo import MongoClient
    import pandas as pd
    pm = MongoClient("mongodb://Guser:Guvi.com3@ac-zstnwxd-shard-00-00.e8i7hjr.mongodb.net:27017,ac-zstnwxd-shard-00-01.e8i7hjr.mongodb.net:27017,ac-zstnwxd-shard-00-02.e8i7hjr.mongodb.net:27017/?ssl=true&replicaSet=atlas-9s52qi-shard-0&authSource=admin&retryWrites=true&w=majority")
    pm1 = pm['Twitter_data']
    data_dict = tweets_df.to_dict('records')
    pmcollections1 = pm1[input()]
    pmcollections1.insert_many(data_dict)
    print('Data pushed to Database')
else:
    pass
