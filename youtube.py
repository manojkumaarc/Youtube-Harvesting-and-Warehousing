
# IMPORTING NECESSARY LIBRARIES
from googleapiclient.discovery import build
import pymongo
from sqlalchemy import create_engine
import mysql.connector as mysql 
import pymysql
import pandas as pd
import datetime
import isodate
import streamlit as st

# API KEY CONNECTION

def api_connect():
    api_key = "AIzaSyAKvPg0bnobb84jGgLoZkhOnei_csbbPAQ"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube

youtube = api_connect()

# FETCH CHANNEL INFORMATION
def get_channel_info(channel_id):
    request = youtube.channels().list(
                        part="snippet,contentDetails,statistics",
                        id = channel_id)
    response = request.execute()

    for i in response['items']:
        data = dict(channel_name=i["snippet"]["title"],
                    channel_id =i["id"],
                    subscribers = i["statistics"]['subscriberCount'],
                    views = i["statistics"]["viewCount"],
                    videos = i["statistics"]["videoCount"],
                    description = i["snippet"]['description'],
                    playlist_id = i["contentDetails"]["relatedPlaylists"]["uploads"]
                )
    return data
    
# FETCHING ID FOR ALL VIDEOS OF A CHANNEL
def get_video_ids(channel_id):
    video_ids=[] # create a list for video IDs
    response = youtube.channels().list(id=channel_id, 
                                        part='contentDetails').execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None # In first and last page, there will be no next_page_token

    # while true loop condition is used to run the funtion until it is true
    # it is stopped by using break function
    while True:
            response1 = youtube.playlistItems().list(
                                        part='snippet', 
                                        playlistId = playlist_id, 
                                        maxResults=50,
                                        pageToken=next_page_token).execute()
            # by default only 5 video ids can be taken; use maxResults = 50 to get 50 videos ids
            # to get list of video ids more than 50, count the number of items in response1 using for loop
            for i in range(len(response1['items'])):
                video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token = response1.get('nextPageToken')
            # get() will run until the values are available and returns none if no value is found
            if next_page_token is None:
                break
    return video_ids

# FETCHING VIDEO INFORMATION
def get_video_info(video_ids):
    video_data = []
    for id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=id
        )
        response=request.execute()

        for i in response['items']:
            data = dict(Channel_Name = i['snippet']['channelTitle'],
                        Channel_Id = i['snippet']['channelId'],
                        Video_Id = i['id'],
                        Title = i['snippet']['title'],
                        #Tags = i['snippet'].get('tags'), # some videos may not have tags, it returns none
                        Description = i['snippet'].get('description'),
                        Published_date = i['snippet']['publishedAt'],
                        Duration = i['contentDetails']['duration'],
                        Views = i['statistics']['viewCount'],
                        Comments = i['statistics'].get('commentCount'), #some videos may not have comments
                        Likes = i['statistics']['likeCount'])
        
            video_data.append(data)
    return video_data

        
# FETCHING COMMENT DETAILS
def get_comment_info(video_ids):
    comment_data = []
    # use try and except block because some videos have comments disabled
    try:
        for i in video_ids:
            request = youtube.commentThreads().list(
                part = "snippet",videoId = i,
                maxResults = 50
                )
            response=request.execute()
                #https://developers.google.com/youtube/v3/docs/commentThreads/list?apix=true

            for i in response['items']:
                data = dict(video_id = i['snippet']['videoId'],
                            comment_id = i['snippet']['topLevelComment']['id'],
                            comment_author = i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_text = i['snippet']['topLevelComment']['snippet']['textDisplay'],
                            commented_on = i['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                comment_data.append(data)
    except: 
        pass
    return comment_data

# FETCHING PLAYLIST DETAILS like playlist_id, its channel_id, published_data
# ref : https://developers.google.com/youtube/v3/docs/playlists/list?apix=true
def get_playlist_details(channel_id):
        next_page_token = None
        playlist_data=[]
        while True: 
                request = youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId = channel_id,
                        maxResults = 50,
                        pageToken = next_page_token
                )
                response = request.execute()

                for i in response['items']:
                        data = dict(playlist_id = i['id'],
                                playlist_title = i['snippet']['title'],
                                channel_id = i['snippet']['channelId'],
                                channel_name = i['snippet']['channelTitle'],
                                published_at = i['snippet']['publishedAt'],
                                playlist_videos =i['contentDetails'] ['itemCount'])
                        playlist_data.append(data)
                        
                next_page_token = response.get('nextPageToken')
                if next_page_token is None:
                        break
        return playlist_data    




# CONNECTION ESTABLISHING WITH MONGODB

client=pymongo.MongoClient("mongodb+srv://cmanoj1:12345@cluster0.ouxeklo.mongodb.net/?retryWrites=true&w=majority")
db = client["youtube_db"]


# DATA MIGRATION TO MONGODB (NoSQL)

def channel_details(channel_id): 
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_ids = get_video_ids(channel_id)
    vi_details = get_video_info(vi_ids) #interlinked with vi_ids
    com_details = get_comment_info(vi_ids) #interlinked with vi_ids


    collection1 = db['channel_details']
    collection1.insert_one({"channel_information":ch_details,
                            "playlist_information":pl_details,
                            "video_information":vi_details,
                            "comment_information":com_details})

    return "Successfully data migrated to MongoDB"

#TRANSFORMING THE UNSTRUCTURED DATA FROM MONGO DB
# INTO A DATAFRAME FOR THE FIRST TABLE CHANNEL_DETAILS
# # empty {} denotes that the item will be checkked
# in all channels
# mongodb creates _id for the collection. it is not required. so assign "_id" : 0
# we want only channel information. so assign to 1



#ESTABLISHING CONNECTION WITH MYSQL AND CREATING A TABLE FOR CHANNEL DETAILS
def channels_table():
    # CONNECTION
    mydb = pymysql.connect(host='localhost',
                                user='root',     
                                password='1234',
                                database='youtubedata'
                                )
    cursor = mydb.cursor()

    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()
    # CREATING TABLE CHANNELS IN MYSQL
    try:
        create_query = '''create table if not exists channels(channel_name varchar(100),
                        channel_id varchar(100) primary key,
                        subscribers bigint,
                        views bigint,
                        videos bigint,
                        description text,
                        playlist_id varchar(100))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("channel table is already created")
        
    ch_list=[]   
    db = client["youtube_db"]
    collection1 = db["channel_details"]
  
    for i in collection1.find({},{"_id":0, "channel_information":1}):
        ch_list.append(i["channel_information"])
    df= pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = '''insert into channels(
                        channel_name ,
                        channel_id,
                        subscribers,
                        views,
                        videos,
                        description,
                        playlist_id)
                        
                        values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['channel_name'],
                row['channel_id'],
                row['subscribers'],
                row["views"],
                row['videos'],
                row['description'],
                row['playlist_id'])

        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("Values are already nserted into the table Channel")



#ESTABLISHING CONNECTION WITH MYSQL AND CREATING A TABLE FOR PLAYLIST DETAILS
def playlist_table():
        # CONNECTION
    mydb = pymysql.connect(host='localhost',
                            user='root',     
                            password='1234',
                            database='youtubedata'
                            )
    cursor = mydb.cursor()

    drop_query = '''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()
    # CREATING TABLE PLAYLISTS IN MYSQL
    create_query = '''create table if not exists playlists
                (playlist_id varchar(200) primary key,
                playlist_title varchar(500),
                channel_id varchar(200),
                channel_name varchar(100),
                published_at DATETIME,
                playlist_videos int)'''
    cursor.execute(create_query)
    mydb.commit()

    pl_list=[] 
    db = client["youtube_db"]
    collection1 = db["channel_details"]
    for i in collection1.find({},{"_id":0, "playlist_information":1}):
        for j in range(len(i["playlist_information"])):
            pl_list.append(i["playlist_information"][j])
    df1 = pd.DataFrame(pl_list)

    for index, row in df1.iterrows():
        insert_query = '''insert into playlists(
                        playlist_id,
                        playlist_title,
                        channel_id,
                        channel_name,
                        published_at,
                        playlist_videos
                        )
                        
                        values(%s,%s,%s,%s,%s,%s)'''
                        
        values = (row['playlist_id'],
                    row['playlist_title'],
                    row['channel_id'],
                    row['channel_name'],
                    datetime.datetime.strptime(row['published_at'],'%Y-%m-%dT%H:%M:%SZ'),
                    row['playlist_videos']
                    )


    try:
        cursor.execute(insert_query,values)
        mydb.commit()
    except:
        print("Values are already nserted into the table playlists")

       



#ESTABLISHING CONNECTION WITH MYSQL AND CREATING A TABLE FOR VIDEOS DETAILS
def videos_table():
    # CONNECTION
    mydb = pymysql.connect(host='localhost',
                        user='root',     
                        password='1234',
                        database='youtubedata'
                        )
    cursor = mydb.cursor()

    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()
    # CREATING TABLE VIDEOS IN MYSQL
    create_query = '''create table if not exists videos
            ( channel_name varchar(100),
            channel_id varchar(80),
            video_id varchar(30) primary key,
            title varchar(150),
            description text,
            published_date datetime,
            duration TIME,
            views bigint,
            comments int,
            likes bigint)'''
    cursor.execute(create_query)
    mydb.commit()

    vi_list=[] 
    db = client["youtube_db"]
    collection1 = db["channel_details"]
    for i in collection1.find({},{"_id":0, "video_information":1}):
        for j in range(len(i["video_information"])):
            vi_list.append(i["video_information"][j])
    df2 = pd.DataFrame(vi_list)

    for index, row in df2.iterrows():
        insert_query = '''insert into videos(
                                            channel_name,
                                            channel_id,
                                            video_id,
                                            title,
                                            description,
                                            published_date,
                                            duration,
                                            views,
                                            comments,
                                            likes 
                                            )
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                        
        values = (row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    row['Description'],
                    datetime.datetime.strptime(row['Published_date'],'%Y-%m-%dT%H:%M:%SZ'),
                    str(isodate.parse_duration(row['Duration'])),
                    row['Views'],
                    row['Comments'],
                    row['Likes'],
                    )
        cursor.execute(insert_query,values)
        mydb.commit()
    


# CREATING TABLE FOR COMMENTS DETAILS IN MYSQL
def comments_table():
    mydb = pymysql.connect(host='localhost',
                        user='root',     
                        password='1234',
                        database='youtubedata'
                        )
    cursor = mydb.cursor()

    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''create table if not exists comments
            (video_id varchar(30),
            comment_id varchar(100) primary key,
            comment_author varchar(150),
            comment_text text,
            commented_on DATETIME)'''
    cursor.execute(create_query)
    mydb.commit()
    com_list=[] 
    db = client["youtube_db"]
    collection1 = db["channel_details"]
    for i in collection1.find({},{"_id":0, "comment_information":1}):
        for j in range(len(i["comment_information"])):
            com_list.append(i["comment_information"][j])
    df3 = pd.DataFrame(com_list)
    for index, row in df3.iterrows():
        insert_query = '''insert into comments(
                        video_id,
                        comment_id,
                        comment_author,
                        comment_text,
                        commented_on
                        )
                        values(%s,%s,%s,%s,%s)'''
                        
        values = (row['video_id'],
                    row['comment_id'],
                    row['comment_author'],
                    row['comment_text'],
                    datetime.datetime.strptime(row['commented_on'],'%Y-%m-%dT%H:%M:%SZ'))

        cursor.execute(insert_query,values)
        mydb.commit()


def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return "All four tables created successfully"


def display_channels_table():
    db = client["youtube_db"]
    collection1 = db["channel_details"]
    ch_list=[]
    for i in collection1.find({},{"_id":0, "channel_information":1}):
        ch_list.append(i["channel_information"])
    df= st.dataframe(ch_list)

    return df


def display_playlists_table():
    pl_list=[]
    db = client["youtube_db"]
    collection1 = db["channel_details"]
    for i in collection1.find({},{"_id":0, "playlist_information":1}):
        for j in range(len(i["playlist_information"])):
            pl_list.append(i["playlist_information"][j])
    df1 = st.dataframe(pl_list)

    return df1

def display_videos_table():
    vi_list=[] 
    db = client["youtube_db"]
    collection1 = db["channel_details"]
    for i in collection1.find({},{"_id":0, "video_information":1}):
        for j in range(len(i["video_information"])):
            vi_list.append(i["video_information"][j])
    df2 = st.dataframe(vi_list)

    return df2

def display_comments_table():
    com_list=[] 
    db = client["youtube_db"]
    collection1 = db["channel_details"]
    for i in collection1.find({},{"_id":0, "comment_information":1}):
        for j in range(len(i["comment_information"])):
            com_list.append(i["comment_information"][j])
    df3 = st.dataframe(com_list)

    return df3


# streamlit functions

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Techs Adopted")
    st.caption("Python Scripting-pandas, numpy")
    st.caption("Data Extraction using API")
    st.caption("Data Manipulation using MongoDB and MySQL")
    st.caption("Web Application using Streamlit")

channel_id = st.text_input("Enter the Channel ID")

if st.button("Collect and Store  data"):
    ch_ids=[]
    db=client["youtube_db"]
    collection1 = db["channel_details"]
    for i in collection1.find({},{"_id":0, "channel_information":1}):
        ch_ids.append(i["channel_information"]['channel_id'])

    if channel_id in ch_ids:
        st.success("Channel ID exists already")

    else:
        insert = channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to MySQL"):
    Tables = tables() 
    st.success(Tables)

show_table = st.radio("Select the table to view", (":red[Channels]", ":blue[Playlists]", ":green[Videos]", ":violet[Comments]"))

if show_table == ":red[Channels]": 
    display_channels_table()

elif show_table == ":blue[Playlists]": 
    display_playlists_table()

elif show_table == ":green[Videos]": 
    display_videos_table()

elif show_table == ":violet[Comments]": 
    display_comments_table()

# SQL CONNECTION

mydb = pymysql.connect(host='localhost',
                    user='root',     
                    password='1234',
                    database='youtubedata'
                    )
cursor = mydb.cursor()

# QUERIES

query = st.selectbox("Select your query", ("1. All the videos and channels names",
                                             "2. Channels with largest number of videos",
                                             "3. Top 10 most viewed videos",
                                             "4. Comments on each videos",
                                             "5. Most liked videos",
                                             "6. No. of likes for each video",
                                             "7. No. of views for each channel",
                                             "8. videos published in the year 2022",
                                             "9. Average duration of all videos in each channel",
                                             "10. Most commented Videos"))

mydb = pymysql.connect(host='localhost',
                    user='root',     
                    password='1234',
                    database='youtubedata'
                    )
cursor = mydb.cursor()

# create a multi string variable
if query == "1. All the videos and channels names":
    query1 ='''select title as videos, channel_name as ChannelName from videos'''
    cursor.execute(query1)
    mydb.commit()
    # store the query output in t1 variable
    t1 = cursor.fetchall()
    # convert t1 into dataframe
    df= pd.DataFrame(t1, columns = ["Video title", "channel name"])
    st.write(df)

elif query == "2. Channels with largest number of videos":
    query2 ='''select channel_name as ChannelName, videos as NoOfVideos from channels
                order by videos DESC'''
    cursor.execute(query2)
    mydb.commit()    
    t2 = cursor.fetchall()
    df2= pd.DataFrame(t2, columns = ["Channel Name", "No of videos"])
    st.write(df2)

elif query == "3. Top 10 most viewed videos":
    query3 ='''select title as videotitle, views as viewscount, channel_name as channelname from videos
               order by views desc
               limit 10'''
    cursor.execute(query3)
    mydb.commit()    
    t3 = cursor.fetchall()
    df3= pd.DataFrame(t3, columns = ["Video title", "Views", "channel name" ])
    st.write(df3)

elif query == "4. Comments on each videos":
    query4 ='''select title as VideoTitle, comments as CommentCount, channel_name as channelname from videos
            where comments is not null
            order by comments desc'''
    cursor.execute(query4)
    mydb.commit()    
    t4 = cursor.fetchall()
    df4= pd.DataFrame(t4, columns = ["Video title", "No of Comments","channel name"])
    st.write(df4)

elif query == "5. Most liked videos":
    query5 ='''select title as videos, channel_name as channelname, likes as LikeCount
    from videos where likes is not null
    order by likes desc'''
    cursor.execute(query5)
    mydb.commit()    
    t5 = cursor.fetchall()
    df5= pd.DataFrame(t5, columns = ["Video title", "channel name", "No. of Likes" ])
    st.write(df5)

elif query ==  "6. No. of likes for each video":
    query6 ='''select title as videos, channel_name as channelname, likes as LikeCount
    from videos'''
    cursor.execute(query6)
    mydb.commit()    
    t6 = cursor.fetchall()
    df6= pd.DataFrame(t6, columns = ["Video title", "channel name", "No. of Likes" ])
    st.write(df6)

elif query == "7. No. of views for each channel":
    query7 ='''select channel_name, views from channels'''
    cursor.execute(query7)
    mydb.commit()    
    t7 = cursor.fetchall()
    df7= pd.DataFrame(t7, columns = ["channel name", "No. of Views"])
    st.write(df7)


elif query == "8. videos published in the year 2022":
    query8 ='''SELECT title AS videos, channel_name AS channelname, published_date AS PublishedDate
                FROM videos
                WHERE EXTRACT(YEAR FROM published_date) = 2022'''
    cursor.execute(query8)
    mydb.commit()    
    t8 = cursor.fetchall()
    df8= pd.DataFrame(t8, columns = ["Video title", "channel name", "Published Date" ])
    st.write(df8)

elif query == "9. Average duration of all videos in each channel":
    query9 ='''select channel_name as channelname, TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(duration))), '%H:%i:%s') as averageDuration 
                from videos
                group by channel_name'''
    cursor.execute(query9)
    mydb.commit()    
    t9 = cursor.fetchall()
    df9= pd.DataFrame(t9, columns = ["channel name", "Average Duration"])
    T9 =[]
    for index, row in df9.iterrows():
        channel_title = row["channel name"]
        average_duration = row["Average Duration"]
        average_duration_str = str(average_duration)
        T9.append(dict(channel_title = channel_title, Avg_duration = average_duration_str))
    df1 = pd.DataFrame(T9)
    st.write(df1)

elif query ==  "10. Most commented Videos":
    query10 ='''select title as videos, channel_name as channelname, comments as commentCount from videos
    where comments is not null
    order by comments desc limit 10'''
    cursor.execute(query10)
    mydb.commit()    
    t10 = cursor.fetchall()
    df10= pd.DataFrame(t10, columns = ["Video title", "channel name", "No. of comments"])
    st.write(df10)

# command to run streamlit 
# py -m streamlit run youtube.py