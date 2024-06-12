# Importing package
from pyspark.sql.functions import desc,col,monotonically_increasing_id,row_number
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Implementing JSON File in PySpark
spark = SparkSession.builder \
    .master("local[10]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

# Reading JSON file into dataframe
df=spark.read.json("C:/Users/laxmi/PycharmProjects/Samplecode/shazam-tag-data.json")
print("READ JSON")

#Command Line to accept user input
type,track_limit=input(str("Enter either chart or state_chart AND Number of tracks \n")).split(' ')

#convert number of tracks to integer datatype
track_limit=int(track_limit)

#Based on user input "chart" or "state_chart" condition
if(type=='chart'):
    #Fetch the tracktile and artistname dataframe and group them
    df_withgroupby = df.select('match.track.metadata.tracktitle', 'match.track.metadata.artistname'). \
        groupBy('tracktitle', 'artistname').count()
    #sort df_withgroupby in descending order based on count with respect to most tagged tracktile
    df_sort = df_withgroupby.sort(col('count').desc()).limit(track_limit)
    #Adding row number
    df_chartposition = df_sort.withColumn("CHART POSITION", monotonically_increasing_id())
    w = Window().orderBy("CHART POSITION")
    df_all = df_chartposition.withColumn("CHART POSITION", (row_number().over(w)))

    #Final outcome for "chart number of tracks" input
    df_all.select(col('CHART POSITION'), col('tracktitle').alias('TRACK TITLE'),
               col('artistname').alias('ARTIST NAME')).show(truncate=False)

elif(type=='state_chart'):
    #Fetch the timezone key-values and apply filter to find the states belonging to either America or US country
    df_states = df.select((F.split(F.col('timezone'), '/')[0].alias('COUNTRY')),
                    (F.split(F.col('timezone'), '/')[1].alias('STATES')),
                    col('match.track.metadata.tracktitle').alias('TRACK TITLE'),
                    col('match.track.metadata.artistname').alias('ARTIST NAME')).filter((F.col('COUNTRY') == 'America') | (F.col('COUNTRY') == 'US'))

    #Selecting and grouping values based on states,tracktile and artistname to get the count
    df1 = df_states.select('TRACK TITLE', 'ARTIST NAME', 'STATES').groupBy('STATES', 'TRACK TITLE', 'ARTIST NAME').count()
    #Partitioning based on states and ordering it in descending order
    window = Window.partitionBy('STATES').orderBy(col('count').desc())

    #Output the top 3 tracks in each and every "America" or "US" state by default shows 20 rows
    df_top3 = df1.withColumn("row", row_number().over(window)).filter(col("row") <= 3).drop('count')
    df_top3.show(truncate=False)

    #Writing the dataframe into a csv file to compute all the records in states mentioned
    df_top3.write.option("header", True).csv('C:/Users/llnu002/PycharmProjects/Samplecode/state_chart.csv')
    #Reading csv data
    df = spark.read.csv('C:/Users/laxmi/PycharmProjects/Samplecode/state_chart.csv')

else:
    print("Wrong Input,Check again")
