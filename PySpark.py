# initializing spark
from pyspark import SparkConf, SparkContext

spark_conf = SparkConf()\
        .setAppName("lab08-xcui5145-Xinyi-Cui")
sc=SparkContext.getOrCreate(spark_conf) 

import csv

# extract dislikes based on video_id and country,
# key: (video_id, country)
# value: dislikes 
def extractDislikes(record):
    try:  
        video_id,trending_date,category,views,likes,dislikes,country = record.split(",")
        dislikes = int(dislikes) 
        return ((video_id,country),dislikes)
    except:
        return ()
    
# extract likes based on video_id and country,
# key: (video_id, country)
# value: likes
def extractLikes(record):
    try:
        video_id,trending_date,category,views,likes,dislikes,country = record.split(",")
        likes = int(likes)
        return ((video_id,country),likes)
    except:
        return ()

# get the first and last trending appearances in the same country
def diff(values):
    d_values = list(values)
    return (d_values[-1]-d_values[0])

# read the input as line and convert into RDD of String
AllVideosData = sc.textFile("AllVideos.csv")

# workload 1
# get dislikes value of each video
video_dislikes = AllVideosData.map(extractDislikes)
# group by key (video_id and country)
groupedByID_Dislike = video_dislikes.groupByKey()

# get likes value of each video
video_likes = AllVideosData.map(extractLikes)
# group by key (video_id, country)
groupedByID_Like = video_likes.groupByKey()

# join dislike RDD and like RDD based on key(video_id, country)
joined1 = groupedByID_Dislike.join(groupedByID_Like)
#joined1.take(10)

# get the growth of dislikes number 
# by the difference of dislikes increase and likes increase 
# between the first and last trending appearances in the same country
getDiff = joined1.mapValues(lambda d: (diff(d[0])-diff(d[1]))) 
DiffWork = getDiff.sortBy(lambda r: r[1], ascending=False)
# change to the correct output format
Workload1 = DiffWork.map(lambda re: (re[0][0],re[1],re[0][1])).collect()

# workload 1 output
# top 10 videos with fastest growth of dislikes number
for i in range(10):
    print(Workload1[i])

# workload 2 
# get video_id as key and country as value
def extractID_C(record):
    try:  
        video_id,trending_date,category,views,likes,dislikes,country = record.split(",")
        return (video_id, country)
    except:
        return ()
    
# get video_id as key and category as value  
def extractCategory_ID(record):
    try:
        video_id,trending_date,category,views,likes,dislikes,country = record.split(",")
        return (video_id, category)
    except:
        return()

# once get the count of unique country per video_id
# use this function to calculate average value of each category 
def average_country_number(value):
    n_unique_country_list = list(value)
    return sum(n_unique_country_list)/len(n_unique_country_list)

# get (video_id, country) 
id_country = AllVideosData.map(extractID_C)
# group by video_id
groupedC_ID = id_country.groupByKey()
# count the number of unique countries for each video_id 
getCountOfC = groupedC_ID.mapValues(lambda vals: len(set(vals)))

# get (video_id, category)
id_category = AllVideosData.map(extractCategory_ID)

# join (video_id, country) and (video_id, category) by video_id
joined2 = id_category.join(getCountOfC).distinct()
#joined2.take(3)
# map it as the correct format => category, (video_id, #unique country)
join2_change = joined2.map(lambda rec: (rec[1][0],(rec[0],rec[1][1])))
join2_change.take(10)

# map values as (category, #unique countries)
join2_category_n = join2_change.map(lambda r: (r[0],(r[1][1])))
# group #unique countries by category
join2_groupbyC = join2_category_n.groupByKey()
#join2_groupbyC.take(3)

# apply average_country_number function
getAverage = join2_groupbyC.mapValues(average_country_number)
# sort it as ascending order
workload2 = getAverage.sortBy(lambda r: r[1], ascending=True).collect()

# workload 2 output
# final result sorted by the average country number
workload2