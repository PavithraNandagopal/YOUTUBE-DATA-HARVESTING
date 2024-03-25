from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

def api_connect():
    api_id='AIzaSyAx4iihyg9JpRI1hWMFJZozN3aFfQuFTPM'
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey= api_id)
    return youtube
youtube=api_connect()

# get channels information
def get_channel_info(c_id):

    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=c_id
        )
    response = request.execute()
    for i in response['items']:
        data=dict(channel_Name=i[ 'snippet']['title'],
                channel_id=i["id"],
                subsribers=i['statistics']['subscriberCount'],
                views=i['statistics']['viewCount'],
                Total_Videos=i['statistics']['videoCount'],
                Channel_Description=i['snippet']['description'],
                Playlist_ID=i['contentDetails']['relatedPlaylists']['uploads']
        )
    return data


# get video ids
def get_video_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_ID=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None
    while True:
        response1=youtube.playlistItems().list(
            part='snippet',
            playlistId=Playlist_ID,
            maxResults=50,
            pageToken=next_page_token
            ).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

# to get video information
def get_video_details(v_i):
    video_data=[]
    for video_id in v_i:
        request=youtube.videos().list(
            part='snippet,ContentDetails,statistics',
            id=video_id
        )
        response=request.execute()
        for item in response['items']:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favourite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Details=item['contentDetails']['caption']
                    )
            video_data.append(data)
    return video_data
            
#get comment information
def get_command_info(v_i):
    comment_data=[]
    try:
        for video_id in v_i:
            request = youtube.commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    maxResults=50
                )
            response=request.execute()
            for item in response['items']:
                data=dict(comment_id=item['snippet']['topLevelComment']['id'],
                        Video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt']
                        )
                comment_data.append(data)
    except:
        pass
    return comment_data

#get playlist details   
def get_playlist_details(channel_id):
    next_page_token=None
    all_data=[]
    while True:
        request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response=request.execute()

        for item in response['items']:
            data=dict(Playlist_ID=item['id'],
                    Title=item['snippet']['title'],
                    Channel_Id=item['snippet']['channelId'],
                    Channel_Name=item['snippet']['channelTitle'],
                    Published_At=item['snippet']['publishedAt'],
                    Video_count=item['contentDetails']['itemCount']

            )
            all_data.append(data)
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return all_data

#upload to mongodb
client=pymongo.MongoClient( "mongodb+srv://Pavithra:Tamilarasan@pavithra.fmljuh2.mongodb.net/?retryWrites=true&w=majority&appName=Pavithra"
)
db=client['youtube_data']

def channel_details(channel_id):
   ch_details=get_channel_info(channel_id)
   pl_details=get_playlist_details(channel_id)
   v_ids=get_video_ids(channel_id)
   vi_details=get_video_details(v_ids)
   com_details=get_command_info(v_ids)

   collec1=db['channel_details']
   collec1.insert_one({'channel_information':ch_details,'playlist_information':pl_details,
                       'video_information':vi_details,'command_information':com_details})
    
   return "upload successfully"

#table creation for channel playlist videos comments
def get_channel_tables():
    mydb=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='1527',
                        database='youtube_data',
                        port='5432')
    cursor=mydb.cursor()
    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channels(channel_Name varchar(100),
                                                            channel_id varchar(80) primary key,
                                                            subsribers bigint,
                                                            views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_ID varchar (80))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("channels already created")


    ch_list=[]
    db=client['youtube_data']
    collec1=db['channel_details']
    for ch_data in collec1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])
    df=pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        insert_query='''insert into channels(channel_Name ,
                                            channel_id,
                                            subsribers,
                                            views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_ID)
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_Name'],
                row['channel_id'],
                row['subsribers'],
                row['views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_ID'])

        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("channel values are already inserted")
            
            

def get_playlist_table():
    mydb=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='1527',
                        database='youtube_data',
                        port='5432')
    cursor=mydb.cursor()
    drop_query='''drop table if exists playlist'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists playlist(Playlist_ID varchar(100) primary key,
                                                        Title varchar(100),
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        Published_At timestamp,
                                                        Video_count int)'''

    cursor.execute(create_query)
    mydb.commit()

    pl_list=[]
    db=client['youtube_data']
    collec1=db['channel_details']
    for pl_data in collec1.find({},{'_id':0,'playlist_information':1}):
        for i in range (len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    df1=pd.DataFrame(pl_list)

        
    for index,row in df1.iterrows():
        insert_query='''insert into playlist(Playlist_ID ,
                                        Title, 
                                        Channel_Id ,
                                        Channel_Name, 
                                        Published_At ,
                                        Video_count )
                                        values(%s,%s,%s,%s,%s,%s)'''
        values=(row['Playlist_ID'],
            row['Title'],
            row['Channel_Id'],
            row['Channel_Name'],
            row['Published_At'],
            row['Video_count'],
            )

        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            print("playlist values are already inserted")

def get_video_table():
    mydb=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='1527',
                        database='youtube_data',
                        port='5432')
    cursor=mydb.cursor()
    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                    Channel_Id varchar (100),
                                                    Video_Id varchar(30),
                                                    Title varchar(100),
                                                    Tags text,
                                                    Thumbnail varchar(200),
                                                    Description text,
                                                    Published_Date timestamp,
                                                    Duration interval,
                                                    Views bigint,
                                                    Likes bigint,
                                                    Comments int,
                                                    Favourite_Count int,
                                                    Definition varchar(20),
                                                    Caption_Details varchar(20))'''

    cursor.execute(create_query)
    mydb.commit()

    vi_list=[]
    db=client['youtube_data']
    collec1=db['channel_details']
    for vi_data in collec1.find({},{'_id':0,'video_information':1}):
        for i in range (len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df2=pd.DataFrame(vi_list)

        
    for index,row in df2.iterrows():
        insert_query='''insert into videos(Channel_Name,
                                            Channel_Id,
                                            Video_Id,
                                            Title,
                                            Tags,
                                            Thumbnail,
                                            Description,
                                            Published_Date,
                                            Duration,
                                            Views,
                                            Likes,
                                            Comments,
                                            Favourite_Count,
                                            Definition,
                                            Caption_Details)
                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
            row['Channel_Id'],
            row['Video_Id'],
            row['Title'],
            row['Tags'],
            row['Thumbnail'],
            row['Description'],
            row['Published_Date'],
            row['Duration'],
            row['Views'],
            row['Likes'],
            row['Comments'],
            row['Favourite_Count'],
            row['Definition'],
            row['Caption_Details']

            )

        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            print("playlist values are already inserted")


def get_comment_table():
    mydb=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='1527',
                        database='youtube_data',
                        port='5432')
    cursor=mydb.cursor()
    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists comments(comment_id varchar(100) primary key,
                                                        Video_id varchar(50),
                                                        Comment_text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp)'''

    cursor.execute(create_query)
    mydb.commit()

    comm_list=[]
    db=client['youtube_data']
    collec1=db['channel_details']
    for comm_data in collec1.find({},{'_id':0,'command_information':1}):
        for i in range (len(comm_data['command_information'])):
            comm_list.append(comm_data['command_information'][i])
    df3=pd.DataFrame(comm_list)

    for index,row in df3.iterrows():
        insert_query='''insert into comments(comment_id,
                                            Video_id,
                                            Comment_text, 
                                            Comment_Author,
                                            Comment_Published)
                                        values(%s,%s,%s,%s,%s)'''
        values=(row['comment_id'],
            row['Video_id'],
            row['Comment_text'],
            row['Comment_Author'],
            row['Comment_Published']
            )
        cursor.execute(insert_query,values)
        mydb.commit()

    

def tables():
    get_channel_tables()
    get_playlist_table()
    get_video_table()
    get_comment_table()

    return "tables are created successfully"

def show_channel_tables():
    ch_list=[]
    db=client['youtube_data']
    collec1=db['channel_details']
    for ch_data in collec1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])
    df=st.dataframe(ch_list)

    return df

def show_playlist_table():
    pl_list=[]
    db=client['youtube_data']
    collec1=db['channel_details']
    for pl_data in collec1.find({},{'_id':0,'playlist_information':1}):
        for i in range (len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    df1=st.dataframe(pl_list)

    return df1

def show_video_table():
    vi_list=[]
    db=client['youtube_data']
    collec1=db['channel_details']
    for vi_data in collec1.find({},{'_id':0,'video_information':1}):
        for i in range (len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df2=st.dataframe(vi_list)
    
    return df2

def show_comment_table():
    comm_list=[]
    db=client['youtube_data']
    collec1=db['channel_details']
    for comm_data in collec1.find({},{'_id':0,'command_information':1}):
        for i in range (len(comm_data['command_information'])):
            comm_list.append(comm_data['command_information'][i])
    df3=st.dataframe(comm_list)
    
    return df3

# streamlit part

with st.sidebar:
    st.title(":purple[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("SKILLS ")
    st.caption("python scripting")
    st.caption("data collection")
    st.caption("MongoDB")
    st.caption("API integration")
    st.caption("Data management using MongoDB and SQL")
channel_id=st.text_input("Enter the channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["youtube_data"]
    collec1=db["channel_details"]
    for ch_data in collec1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["channel_id"])
    if channel_id in ch_ids:
        st.success("Already exists")
    else:
        insert= channel_details(channel_id)
        st.success(insert)
if st.button("MIGRATE TO SQL"):
    Table=tables()
    st.success(Table)

show_table=st.radio("SELECT THE VIEW FOR BELOW TABLES",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channel_tables()
elif show_table=="PLAYLISTS":
    show_playlist_table()
elif show_table=="VIDEOS":
    show_video_table()
elif show_table=="COMMENTS":
    show_comment_table()


#SQL Connection
mydb=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='1527',
                        database='youtube_data',
                        port='5432')
cursor=mydb.cursor()

question=st.selectbox("select your question",
                      ("1.What are the names of all the videos and their corresponding channels?",
                        "2.Which channels have the most number of videos, and how many videos do they have?",
                        "3. What are the top 10 most viewed videos and their respective channels?",
                        "4. How many comments were made on each video, and what are their corresponding video names?",
                        "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                        "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                        "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                        "8. What are the names of all the channels that have published videos in the year 2022?",
                        "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                        "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

if question=="1.What are the names of all the videos and their corresponding channels?":
    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["VIDEO NAMES","CHANNEL NAMES"])
    st.write(df)
elif question=="2.Which channels have the most number of videos, and how many videos do they have?":
    query2='''select channel_name as channelnames, total_videos as no_videos from channels
                order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["CHANNEL NAMES","NO OF VIDEOS"])
    st.write(df2)

elif question=="3. What are the top 10 most viewed videos and their respective channels?":
    query3='''select views as views,channel_name as channelname, title as videotitle from videos
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["VIEWS","CHANNEL NAME","VIDEO TITLE"])
    st.write(df3)

elif question=="4. How many comments were made on each video, and what are their corresponding video names?":
   
    query4='''select comments as no_commands,title  as videotitle from videos
                where comments is not null '''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["NO OF COMMANDS","VIDEO TITLE"])
    st.write(df4)

elif question=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5='''select title  as videotitle, channel_name as channelnames, likes as likecount from videos
                where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["VIDEO TITLE","CHANNEL NAME","LIKE COUNT"])
    st.write(df5)

elif question=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
   
    query6='''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["LIKE COUNT","VIDEO TITLE"])
    st.write(df6)

elif question=="7. What is the total number of views for each channel, and what are their corresponding channel names?":
   
    query7='''select channel_name as channelnames,views as viewcount from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["CHANNEL NAME","TOTAL VIEWS"])
    st.write(df7)

elif question=="8. What are the names of all the channels that have published videos in the year 2022?":
    query8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["VIDEO TITLE","PUBLISHED DATE","CHANNEL NAME"])
    st.write(df8)

elif question=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9='''select channel_name as channelname,AVG(duration)as averageduration from videos group by channelname'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["CHANNEL NAME", "AVERAGE DURATION"])
    
    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["CHANNEL NAME"]
        averageduration = row["AVERAGE DURATION"]
        averageduration_str=str(averageduration)
        T9.append(dict(CHANNELNAME=channel_title,AVERAGEDURATION=averageduration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)
elif question=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":

    query10='''select title as videotitle,channel_name as channelname,comments as comments from videos
                where comments is not null order by comments desc '''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["VIDEO TITLE","CHANNEL NAME","COMMENTS"])
    st.write(df10)