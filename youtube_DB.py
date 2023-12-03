from googleapiclient.discovery import build
import pymongo
import pandas as pd
import pymysql 
from datetime import datetime
from datetime import timezone
from datetime import timedelta
from isodate import parse_duration
import streamlit as st
import re

# MongoDB client

client=pymongo.MongoClient('mongodb://localhost:27017')
mydb=client["YouTube_Data"]

# CODE - API KEY CONNECTION

def api_connect():
    
    api_id = "AIzaSyDiy5cRIfCO9IhGNPFBJnIsKdeKmR1pwIQ"
    api_service_name = "youtube"
    api_version = "v3"
    youtube=build(api_service_name,api_version,developerKey=api_id)
     
    return youtube
youtube = api_connect()

# CODE - CHANNEL INFORMATIONS

def channels_data(youtube_channel_id):
        request = youtube.channels().list(
                part = "snippet,contentDetails,statistics",
                id = youtube_channel_id
        )
        response = request.execute()

        for i in response["items"]:
                data=dict(channel_name=i["snippet"]["title"],
                        channel_id=i["id"],
                        subscription_Count=i["statistics"]["subscriberCount"],
                        channel_views=i["statistics"]["viewCount"],
                         channel_description=i["snippet"]["description"],
                        playlist_id=i["contentDetails"]["relatedPlaylists"]["uploads"],
                        total_videos=i["statistics"]["videoCount"]
                        )
        return data 
    
# CODE - VIDEO IDs

def video_data(channel_id):

    video_ids=[]

    response = youtube.channels().list(id=channel_id,
                                    part="contentDetails").execute()
    Playlist_ID = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    # nextPageToken is used for get the entire video ids from the channel
    # while loop is used for looping the entire tokens and get next page to get video ids

    nextPage_Token = None

    while True:
        Request_Video_IDs = youtube.playlistItems().list(
                                        part="snippet",
                                        playlistId=Playlist_ID,
                                        maxResults=50,
                                        pageToken=nextPage_Token).execute()
        
        # Request_Video_IDs["items"] for grtting the entire video IDs from the playlist
        # get function is used for eliminate the error to get until the video ids available
        # ids are stored in video_ids
        
        for i in range(len(Request_Video_IDs["items"])):
            video_ids.append(Request_Video_IDs["items"][i]["snippet"]["resourceId"]["videoId"])
        nextPage_Token = Request_Video_IDs.get("nextPageToken")
        if nextPage_Token is None:
            break
    return video_ids

# CODE - VIDEO DETAILS
# CODE - TIME CONVERSION
def iso8601_to_hms(iso8601_duration):
    matches = re.match(r'P(?:([0-9]+)D)?T?(?:([0-9]+)H)?(?:([0-9]+)M)?(?:([0-9]+)S)?', iso8601_duration)
    if matches:
        days = int(matches.group(1)) if matches.group(1) else 0
        hours = int(matches.group(2)) if matches.group(2) else 0
        minutes = int(matches.group(3)) if matches.group(3) else 0
        seconds = int(matches.group(4)) if matches.group(4) else 0

        return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    else:
        raise ValueError("Invalid ISO 8601 duration format")


def format_duration(duration):
    days, seconds = divmod(duration.total_seconds(), 86400)
    hours, remainder = divmod(duration.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return "{:02}:{:02}:{:02}".format(hours, minutes, seconds)

def get_video_details(Video_Info):
    video_details=[]
    for video_id in Video_Info:
        request = youtube.videos().list(
            part = "snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()
        
        for item in response["items"]:
            timestamp=item["snippet"]["publishedAt"]
            modified_timestamp = timestamp.replace('T', ' ').replace('Z', '')
            
            duration_iso8601 = item["contentDetails"]["duration"]
            # print("Duration ISO8601:", duration_iso8601)
            try:
                duration_hms = iso8601_to_hms(duration_iso8601)
                formatted_duration = format_duration(duration_hms)
            except ValueError as e:
                print(f"Error processing duration: {e}")
                formatted_duration = None
            
            data1 = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags=" ".join(item["snippet"].get("tags",["None"])),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet']['description'],
                        PublishedAt=modified_timestamp,
                        Duration=formatted_duration,
                        View_Count=item["statistics"].get("viewCount",None),
                        Like_Count=item["statistics"].get("likeCount",None),
                        Comment_count=item["statistics"].get("commentCount",None),
                        Dislike_Count=item["statistics"].get("dislikeCount",None),
                        Favorite_Count=item["statistics"].get("favoriteCount",None),
                        Caption_Status=item["contentDetails"]["caption"]
                        )
            video_details.append(data1)
    return video_details

# CODE - COMMENTS
def comment_details(VIDEO_IDs):
    comment_info=[]
    try:
        for video_IDs in VIDEO_IDs:
            request=youtube.commentThreads().list(
                        part = "snippet",
                        videoId=video_IDs,
                        maxResults=50
                        )
            response=request.execute()

            for item in response["items"]:
                timestamp=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                modified_timestamp = timestamp.replace('T', ' ').replace('Z', '')
                data_comment=dict(comment_id=item["snippet"]["topLevelComment"]["id"],
                                video_id=item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                                comment_text=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                                comment_author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                comment_published_date=modified_timestamp)
                comment_info.append(data_comment)
    except:
        pass
    return comment_info

# CODE - PLAYLIST
def PlayList_Data(channel_id):
    playlist_details=[]
    NextPage_Token=None

    while True:
        request=youtube.playlists().list(
                                part = "snippet,contentDetails",
                                channelId=channel_id,
                                maxResults=50,
                                pageToken=NextPage_Token
                                )
        response=request.execute()

        for item in response["items"]:
            playlist_data=dict(PlayList_ID=item["id"],
                            Channel_ID=item["snippet"]["channelId"],
                            PlayList_Name=item["snippet"]["title"])
            playlist_details.append(playlist_data)
        NextPage_Token=response.get("nextPageToken")
        if NextPage_Token is None:
            break
    return playlist_details

# MongoDB DATA MIGRATION

def YT_channel_Datas(channel_id):
    channel=channels_data(channel_id)
    video_id=video_data(channel_id)
    playlist=PlayList_Data(channel_id)
    video_details=get_video_details(video_id)
    comment=comment_details(video_id)
    
    collecting_data=mydb["channel_info"]
    collecting_data.insert_one({"Channel_infor":channel,
                               "Playlist_info":playlist,
                               "Video_info":video_details,
                               "Comment_info":comment})
    return "MongoDB Data Migration Successfully Completed"


# data migration from MongoDB to MySQL --> creating table formats
# CHANNEL sql table creation
def channels_info_table():
    myconnection=pymysql.connect(host='127.0.0.1',user='root',password='admin',database="youtubedata",port=3306)
    cur=myconnection.cursor()

    drop_table="drop table if exists channels"
    cur.execute(drop_table)
    myconnection.commit()


    create_query = '''create table if not exists channels(Channel_Name varchar(100),
                                                        Channel_Id varchar(80) primary key,
                                                        Subscribers bigint,
                                                        Views bigint,
                                                        Total_Videos int,
                                                        Channel_Description text,
                                                        Playlist_Id varchar(100))'''
                                                        
    cur.execute(create_query)
    myconnection.commit()

    # print("Table Created Successfully")
            
    Channel_list=[]
    mydb=client["YouTube_Data"]
    collecting_data=mydb["channel_info"]
    for yt_details in collecting_data.find({},{"_id":0,"Channel_infor":1}):
        Channel_list.append(yt_details["Channel_infor"])
    ch_df=pd.DataFrame(Channel_list)

    myconnection=pymysql.connect(host='127.0.0.1',user='root',password='admin',database="youtubedata",port=3306)
    cur=myconnection.cursor()


    for index,row in ch_df.iterrows():
        insert_data = '''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscribers,
                                            Views,
                                            Channel_Description,
                                            Playlist_Id,
                                            Total_Videos)
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_name'],
                row['channel_id'],
                row['subscription_Count'],
                row['channel_views'],
                row['channel_description'],
                row['playlist_id'],
                row['total_videos'])
        # return "Migration Sucessfully Completed"
        

        cur.execute(insert_data,values)
    myconnection.commit()
# channels_info_table()

# PLAYLIST SQL TABLE CREATION 
def playlist_info_table():
    myconnection=pymysql.connect(host='127.0.0.1',user='root',password='admin',database="youtubedata",port=3306)
    cur=myconnection.cursor()

    drop_table='''drop table if exists playlists'''
    cur.execute(drop_table)
    myconnection.commit()


    create_query = '''create table playlists(PlayList_ID varchar(100) primary key,
                                            Channel_ID varchar(100),
                                            PlayList_Name varchar(100))'''
                                            
                                                        
    cur.execute(create_query)
    myconnection.commit()

    playlists_list=[]
    mydb=client["YouTube_Data"]
    collecting_data=mydb["channel_info"]
    for playlists_details in collecting_data.find({},{"_id":0,"Playlist_info":1}):
        for i in range(len(playlists_details["Playlist_info"])):
            playlists_list.append(playlists_details["Playlist_info"][i])
    pl_df=pd.DataFrame(playlists_list)

    myconnection=pymysql.connect(host='127.0.0.1',user='root',password='admin',database="youtubedata",port=3306)
    cur=myconnection.cursor()

    for index,row in pl_df.iterrows():
        insert_data = '''insert into playlists(PlayList_ID,
                                            Channel_ID,
                                            PlayList_Name)
                                            values(%s,%s,%s)'''
        values=(row['PlayList_ID'],
                row['Channel_ID'],
                row['PlayList_Name'])
        

        cur.execute(insert_data,values)
    myconnection.commit()
# playlist_info_table()          


# VIDEOS sql table creation
def videos_info_table():
    myconnection=pymysql.connect(host='127.0.0.1',user='root',password='admin',database="youtubedata",port=3306)
    cur=myconnection.cursor()

    drop_table='''drop table if exists videos'''
    cur.execute(drop_table)
    myconnection.commit()


    create_query = '''create table if not exists videos(Channel_Name varchar(150),
                                                        Channel_Id varchar(300),
                                                        Video_Id varchar(300) primary key,
                                                        Title varchar(300),
                                                        Tags text,
                                                        Thumbnail varchar(300),
                                                        Description text,
                                                        PublishedAt timestamp,
                                                        Duration varchar(300),
                                                        View_Count bigint,
                                                        Like_Count bigint,
                                                        Comment_count int,
                                                        Dislike_Count int,
                                                        Favorite_Count int,
                                                        Caption_Status varchar(500))'''
                                            
                                                    
    cur.execute(create_query)
    myconnection.commit()

    vid_list=[]
    mydb=client["YouTube_Data"]
    collecting_data=mydb["channel_info"]
    for videos_details in collecting_data.find({},{"_id":0,"Video_info":1}):
        for i in range(len(videos_details["Video_info"])):
            vid_list.append(videos_details["Video_info"][i])
    vid_df=pd.DataFrame(vid_list)
    
    myconnection=pymysql.connect(host='127.0.0.1',user='root',password='admin',database="youtubedata",port=3306)
    cur=myconnection.cursor()
    
    for index,row in vid_df.iterrows():
        # duration_str=row['Duration']
        # iso_duration = pd.to_timedelta(duration_str).isoformat()
        insert_data = '''insert into videos(Channel_Name,
                                        Channel_Id,
                                        Video_Id,
                                        Title,
                                        Tags,
                                        Thumbnail,
                                        Description,
                                        PublishedAt,
                                        Duration,
                                        View_Count,
                                        Like_Count,
                                        Comment_count,
                                        Dislike_Count,
                                        Favorite_Count,
                                        Caption_Status)
                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                row['Tags'],
                row['Thumbnail'],
                row['Description'],
                row['PublishedAt'],
                row['Duration'],
                row['View_Count'],
                row['Like_Count'],
                row['Comment_count'],
                row['Dislike_Count'],
                row['Favorite_Count'],
                row['Caption_Status'])
        cur.execute(insert_data,values)
        myconnection.commit()
# return "table created"
# videos_info_table()


# COMMENT SQL table creation
def comments_info_table():
    myconnection=pymysql.connect(host='127.0.0.1',user='root',password='admin',database="youtubedata",port=3306)
    cur=myconnection.cursor()


    drop_table='''drop table if exists Comments'''
    cur.execute(drop_table)
    myconnection.commit()


    create_query = '''create table Comments(comment_id varchar(100) primary key,
                                            video_id varchar(100),
                                            comment_text text,
                                            comment_author varchar(150),
                                            comment_published_date varchar(50)
                                            )'''
                                            
                                                        
    cur.execute(create_query)
    myconnection.commit()

    comm_list=[]
    mydb=client["YouTube_Data"]
    collecting_data=mydb["channel_info"]
    for comm_details in collecting_data.find({},{"_id":0,"Comment_info":1}):
        for i in range(len(comm_details["Comment_info"])):
            comm_list.append(comm_details["Comment_info"][i])
    comm_df=pd.DataFrame(comm_list)

    myconnection=pymysql.connect(host='127.0.0.1',user='root',password='admin',database="youtubedata",port=3306)
    cur=myconnection.cursor()

    for index,row in comm_df.iterrows():
        insert_data = '''insert into comments(comment_id,
                                            video_id,
                                            comment_text,
                                            comment_author,
                                            comment_published_date)
                                            values(%s,%s,%s,%s,%s)'''
        values=(row['comment_id'],
                row['video_id'],
                row['comment_text'],
                row['comment_author'],
                row['comment_published_date'])
        

        cur.execute(insert_data,values)
    myconnection.commit()
# comments_info_table()

# OVERALL YouTube data Tables
def YT_tables():
    channels_info_table()
    playlist_info_table()
    videos_info_table()
    comments_info_table()
    
    return "MySQL Data Migration Successfully Completed"

# STREAMLIT data VISUALIZATION
def Channel_Table():
    Channel_list=[]
    mydb=client["YouTube_Data"]
    collecting_data=mydb["channel_info"]
    for yt_details in collecting_data.find({},{"_id":0,"Channel_infor":1}):
        Channel_list.append(yt_details["Channel_infor"])
    ch_df=st.dataframe(Channel_list)
    return ch_df

def PlayLists_Table():
    playlists_list=[]
    mydb=client["YouTube_Data"]
    collecting_data=mydb["channel_info"]
    for playlists_details in collecting_data.find({},{"_id":0,"Playlist_info":1}):
        for i in range(len(playlists_details["Playlist_info"])):
            playlists_list.append(playlists_details["Playlist_info"][i])
    pl_df=st.dataframe(playlists_list)
    return pl_df

def Videos_Table():
    vid_list=[]
    mydb=client["YouTube_Data"]
    collecting_data=mydb["channel_info"]
    for videos_details in collecting_data.find({},{"_id":0,"Video_info":1}):
        for i in range(len(videos_details["Video_info"])):
            vid_list.append(videos_details["Video_info"][i])
    vid_df=st.dataframe(vid_list)
    return vid_df

def Comment_Table():
    comm_list=[]
    mydb=client["YouTube_Data"]
    collecting_data=mydb["channel_info"]
    for comm_details in collecting_data.find({},{"_id":0,"Comment_info":1}):
        for i in range(len(comm_details["Comment_info"])):
            comm_list.append(comm_details["Comment_info"][i])
    comm_df=st.dataframe(comm_list)
    return comm_df

# Streamlit

with st.sidebar:
    st.title(":red[Welcome to YouTube Data Harvesting and Warehousing]")
    st.link_button("Go to YouTube","https://www.youtube.com/")
    st.header("Overview of this site")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("Storing in MongoDB")
    st.caption("Data Analyzing using MongoDb and MySQL")
    
    
channel_id=st.text_input("Enter the CHANNEL ID")

if st.button("Collect and Store data"):
    channel_IDs=[]
    mydb=client["YouTube_Data"]
    collection_data=mydb["channel_info"]
    for CHANNELS_data in collection_data.find({},{"_id":0,"Channel_infor":1}):
        channel_IDs.append(CHANNELS_data['Channel_infor']['channel_id'])
    if channel_id in channel_IDs:
        st.success("Channel ID Already Exists")
    else:
        insert=YT_channel_Datas(channel_id)
        st.success(insert)
    
if st.button("Migrate to MySQL"):
    TABLE=YT_tables()
    st.success(TABLE)
    # Table2=channels_info_table()
    # A=playlist_info_table()
    # B=comments_info_table()
    # L=videos_info_table()
    # st.success(T)
    # st.success(A)
    # st.success(Table2)
    
    
    
option=st.selectbox(
            "Select the Table you want to view?",
            ("Channels","Playlists","Videos","Comments"),
            index=None,
            placeholder="Choose type of Table....")
st.write('Details of' ,option )

if option=="Channels":
    Channel_Table()
    
elif option=="Playlists":
    PlayLists_Table()

elif option=="Videos":
    Videos_Table()

elif option=="Comments":
    Comment_Table()
    
 
# MySQL connection to Streamlit

myconnection=pymysql.connect(host='127.0.0.1',user='root',password='admin',database="youtubedata",port=3306)
cur=myconnection.cursor()

Queries=st.selectbox("Select Your Query",
                     ("1. Names of all the videos and their corresponding channels?",
                      "2. Which channels have the most number of videos, and how many videos do they have?",
                      "3. What are the top 10 most viewed videos and their respective channels?",
                      "4. How many comments were made on each video, and what are their corresponding video names?",
                      "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                      "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                      "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                      "8. What are the names of all the channels that have published videos in the year 2022?",
                      "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                      "10. Which videos have the highest number of comments, and what are their corresponding channel names?"),
                     index=None,
                     placeholder="Choose your Query....")

if Queries=="1. Names of all the videos and their corresponding channels?":
    Query_1='''select title as VIDEO_NAME,Channel_Name as CHANNEL_NAME from videos'''
    cur.execute(Query_1)
    myconnection.commit()
    Q1=cur.fetchall()
    df_q1=pd.DataFrame(Q1,columns=["VIDEO NAME","CHANNEL NAME"])
    st.write(df_q1)
    
elif Queries=="2. Which channels have the most number of videos, and how many videos do they have?":
    Query_2='''select Channel_Name as CHANNEL_NAME,Total_Videos as num_videos from  channels 
                order by Total_Videos desc'''
    cur.execute(Query_2)
    myconnection.commit()
    Q2=cur.fetchall()
    df_q2=pd.DataFrame(Q2,columns=["CHANNEL NAME","NUMBER of VIDEOS"])
    st.write(df_q2)
      
    
elif Queries=="3. What are the top 10 most viewed videos and their respective channels?":
    Query_3='''select View_Count as views,Channel_name as CHANNEL_NAME,Title as VIDEO_TITLE from videos 
                where View_Count is not null 
                order by View_Count desc limit 10'''
    cur.execute(Query_3)
    myconnection.commit()
    Q3=cur.fetchall()
    df_q3=pd.DataFrame(Q3,columns=["VIEWS","CHANNEl NAME","VIDEO TITLE"])
    st.write(df_q3)

elif Queries=="4. How many comments were made on each video, and what are their corresponding video names?":
    Query_4='''select Comment_Count as num_COMMENTS,title as VIDEO_TITLE from videos
                where Comment_Count is not null'''
    cur.execute(Query_4)
    myconnection.commit()
    Q4=cur.fetchall()
    df_q4=pd.DataFrame(Q4,columns=["COMMENTS","VIDEO NAME"])
    st.write(df_q4)
    
elif Queries=="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    Query_5='''select Title as VIDEO_TITLE,Channel_Name as CHANNEL_NAME,Like_Count as LIKES from videos
                where Like_Count is not null order by like_count desc'''
    cur.execute(Query_5)
    myconnection.commit()
    Q5=cur.fetchall()
    df_q5=pd.DataFrame(Q5,columns=["VIDEO TITLE","CHANNEL NAME","LIKES COUNT"])
    st.write(df_q5)
    
elif Queries=="6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    Query_6='''select Title as VIDEO_TITLE,Like_Count as LIKES,Dislike_Count as DISLIKES from videos'''
    cur.execute(Query_6)
    myconnection.commit()
    Q6=cur.fetchall()
    df_q6=pd.DataFrame(Q6,columns=["VIDEO NAME","LIKES COUNT","DISLIKE COUNT"])
    st.write(df_q6)
    
elif Queries=="7. What is the total number of views for each channel, and what are their corresponding channel names?":
    Query_7='''select Channel_Name as CHANNEL_NAME,Views as VIEWS from channels'''
    cur.execute(Query_7)
    myconnection.commit()
    Q7=cur.fetchall()
    df_q7=pd.DataFrame(Q7,columns=["CHANNEL NAME","VIEWS"])
    st.write(df_q7)
    
elif Queries=="8. What are the names of all the channels that have published videos in the year 2022?":
    Query_8='''select Channel_Name as CHANNEL_NAME,PublishedAt as PUBLISHED_DATE,Title as TITLE from videos
                where year(PublishedAt) = 2022'''
    cur.execute(Query_8)
    myconnection.commit()
    Q8=cur.fetchall()
    df_q8=pd.DataFrame(Q8,columns=["CHANNEL NAME","PUBLISHED DETAIL","TITLE"])
    st.write(df_q8)


elif Queries=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    Query_9='''select Channel_Name as CHANNEL_NAME,avg(Duration) as AVERAGE_DURATION from videos
                group by Channel_Name'''
    cur.execute(Query_9)
    myconnection.commit()
    Q9=cur.fetchall()
    df_q9=pd.DataFrame(Q9,columns=["CHANNEL NAME","AVERAGE DURATION"])
    new_q9=[]
    for index,row in df_q9.iterrows():
        TITLE=row["CHANNEL NAME"]
        AVG_1=row["AVERAGE DURATION"]
        AVERAGE_DUR_str=str(AVG_1)
        new_q9.append({"CHANNEL NAME":TITLE,"AVERAGE DURATION":AVERAGE_DUR_str})
    df_new_q9=pd.DataFrame(new_q9)    
    st.write(df_new_q9)
 
    
elif Queries=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    Query_10='''select Title as VIDEO_TITLE,Channel_Name as CHANNEL_NAME,Comment_count as COMMENTS from videos
                where Comment_count is not null order by Comment_count desc'''
    cur.execute(Query_10)
    myconnection.commit()
    Q10=cur.fetchall()
    df_q10=pd.DataFrame(Q10,columns=["VIDEO TITLE","CHANNEL NAME","COMMENTS COUNT"])    
    st.write(df_q10)
   


