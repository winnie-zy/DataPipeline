import urllib
import boto3
import os
import snowflake.connector
# from snowflake.sqlalchemy import URL
# from sqlalchemy import create_engine
from datetime import datetime
import logging
import os
import pandas as pd
from io import StringIO # python3; python2: BytesIO 


# s3 = boto3.client('s3')
s3 = boto3.resource('s3')
charset='utf8'
logger = logging.getLogger()
logger.setLevel(logging.INFO)
downloaded_file = ""
date = datetime.now().strftime("%Y-%m-%d")

##used for docker and barch job
snowAccountArg=''
snowUserArg= ''
snowPassArg= ''
snowDBArg= ''
snowWHArg= ''
snowSchemaArg= ''
snowRoleArg= ''
LOCAL_FILE=''


def run():

        bucketName=''
        key=''
        rawData_pre_process(bucketName, key, downloaded_file)
        print("rawData_pre_process func completed")
        initiateSnowTables()
        loadProcessedFileToSnow('factDel_table')
        # deleteDownloadedFile()#Docker运行完Container后自动释放系统资源，无需删除下载文件

###delete the downloaded file on lambda   
def deleteDownloadedFile():
    if os.path.exists(downloaded_file):
        os.remove(downloaded_file)
        print("Delete the file successfully")
    else:
        print("Can not delete the file as it doesn't exists")

def rawData_pre_process(bucket, key, file):
    s3get(bucket, key, file)##download file from s3 to lambda
    try:
      col_names = ["date_time", "title", "events"]
      rawCSV = pd.read_csv(file,names=col_names,skiprows=range(0,1),encoding='utf8')
      print(rawCSV.head())
      print(len(rawCSV))
      ####load raw data framework to rawData_table
      ## Determined from the “events” column containing “206”
      rawCSV=rawCSV[rawCSV['events'].str.contains("206")]
      print(rawCSV.head())
      print(len(rawCSV))
      ## title.split(‘|’).count = 1, discard the row
      rawCSV=rawCSV[rawCSV['title'].str.contains("|")]
      print(len(rawCSV))
      print(rawCSV.head())
      ### processing Datetime format
      rawCSV['date_time'] = rawCSV['date_time'].apply(getCorrectDatetime)
      rawCSV['date_time'] = pd.to_datetime(rawCSV['date_time'])##convert string to datetime
      print(rawCSV['date_time'].head())
      ###create new coloumns for DIM TABLE
      rawCSV['platform']="unk"
      rawCSV['site_name']="unk"
      rawCSV['video']="unk"
      print (rawCSV.head())
      ##display the unique value of the platform coloumn
      rawCSV['platform'] = rawCSV['title'].apply(lambda x: x.split('|')[0])
      print(rawCSV['platform'].unique())
      ###get platform data
      rawCSV['platform'] = rawCSV['title'].apply(getPlatform)
      print(rawCSV['platform'].unique())
      ###site data
      rawCSV['site_name'] = rawCSV['title'].apply(getSite)
      print(rawCSV['site_name'].unique())
      ###video title data
      rawCSV['video'] = rawCSV['title'].apply(lambda x: x.split('|')[-1])
      rawCSV['video'] = rawCSV['video'].apply(lambda x: x.strip())
      ####去掉含‘Rie’的行
      #rawCSV=rawCSV[ ~ rawCSV['video'].str.contains('Rie') ]
      rawCSV.drop(['title'],axis=1,inplace=True)##axis=1（按列方向操作）,默认axis=0为行
      print(rawCSV.columns)
      print(rawCSV.head())
      ##write pre-processed csv file to local file
      rawCSV.to_csv(LOCAL_FILE,index=False, header=False)

    except Exception as e:
      print(e)

def getSite(x):
    s = x.split('|')
    if (s[0]=='news' or s[0]=='Web'):##'news' 'Web'
        x =s[0]
    else:
        x='unk'
    # print(x)
    return x

###platform data
def getPlatform(x):
    s = x.split('|')
    if (s[0]=="iPhone" or s[0]=="Android" or s[0]=="iPad"):##'Android' 'iPhone' 'iPad'
        x =s[0]
    else:
        x= "windows"
    # print(x)
    return x

### processing Datetime format
def getCorrectDatetime(x):
      s = x.split('T')
      x=s[0]+' '+s[1].split(".")[0]
      return x

###snowflake data warehouse建表
def initiateSnowTables():   
        try:
          #### connect to snowflake - set ocsp response cache to /tmp, the only place we can write on lambda
          ctx = snowflake.connector.connect(
             user=snowUserArg,
             password=snowPassArg,
             account=snowAccountArg,
             ocsp_response_cache_filename="/tmp/ocsp_response_cache"
          )
          cs = ctx.cursor()
          cs.execute("USE ROLE "+snowRoleArg)
          ##### create database 'de_assignment' warehouse 'de_assignment_WH'#######
          cs.execute("CREATE DATABASE IF NOT EXISTS "+snowDBArg)
          cs.execute("CREATE WAREHOUSE IF NOT EXISTS "+snowWHArg)
          cs.execute("USE DATABASE "+snowDBArg)
          cs.execute("USE WAREHOUSE "+snowWHArg)
          logger.info("SUCCESS: Create database ... succeessfully")
          print("Create database  "+snowDBArg+" succeessfully")
          logger.info("SUCCESS: Create warehouse ... succeessfully")
          print("Create warehouse "+snowWHArg+" succeessfully")
          #########factDel_table########
          cs.execute("""CREATE TABLE factDel_table IF NOT EXISTS(
                            date_time datetime,
                            events varchar(255),
                            platform varchar(255),
                            site_name varchar(255),
                            video varchar(255)   
                        )""")
          print("Create table  factDel_table succeessfully") 
          #########dimDateDel_table########
          cs.execute("""CREATE TABLE dimDateDel_table IF NOT EXISTS(
                            date_time datetime
                        )""")
          print("Create table  dimDateDel_table succeessfully")
          #########dimPlatformDel_table########
          cs.execute("""CREATE TABLE dimPlatformDel_table  IF NOT EXISTS(
                            platform varchar(255)
                        )""")
          print("Create table  dimPlatformDel_table succeessfully")
          #########dimSiteDel_table########
          cs.execute("""CREATE TABLE dimSiteDel_table IF NOT EXISTS(
                            site_name varchar(255)
                        )""")  
          print("Create table  dimSiteDel_table succeessfully")
          #########dimVideoDel_table########
          cs.execute("""CREATE TABLE dimVideoDel_table IF NOT EXISTS(
                            video varchar(255)
                        )""") 
          print("Create table  dimVideoDel_table succeessfully")
          #########dimDate_table########
          cs.execute("""CREATE TABLE dimDate_table IF NOT EXISTS(
                            date_time datetime primary key NOT NULL
                        )""")
          print("Create table  dimDate_table succeessfully")
          #########dimPlatform_table########
          cs.execute("""CREATE TABLE dimPlatform_table IF NOT EXISTS(
                            platform_id int AUTOINCREMENT primary key NOT NULL,
                            platform varchar(255)
                        )""")
          print("Create table  dimPlatform_table succeessfully")
          #########dimSite_table########
          cs.execute("""CREATE TABLE dimSite_table IF NOT EXISTS(
                            site_id int AUTOINCREMENT primary key NOT NULL,
                            site_name varchar(255)
                        )""")
          print("Create table  dimSite_table succeessfully")
          #########dimVideo_table########
          cs.execute("""CREATE TABLE dimVideo_table IF NOT EXISTS(
                            video_id int AUTOINCREMENT primary key NOT NULL,
                            video varchar(255)
                        )""")
          print("Create table  dimVideo_table succeessfully")
          #########fact_table########
          cs.execute("""CREATE TABLE fact_table IF NOT EXISTS(
                            date_time datetime,  
                            platform_id int,
                            site_id int,
                            video_id int  
                        )""")
          print("Create table  fact_table succeessfully")

        except Exception as e:
          print(e)
          raise
        finally:
          cs.close()

def s3get(bucket, key, file):##file is the file's format which was defined before
    #download file的format可以传进来的
    s3.Object(bucket, key).download_file(file)
    print("The rawdata file has been downloaded  sucessfully!")

###load FACT_DEL TABLE(to snowflake)
def loadProcessedFileToSnow(tableName):
    try:
        ## snowflake python connector
        ctx = snowflake.connector.connect(
          user=snowUserArg,
          password=snowPassArg,
          account=snowAccountArg,
          database=snowDBArg,
          warehouse=snowWHArg,
          schema=snowSchemaArg,
          ocsp_response_cache_filename="/tmp/ocsp_response_cache"
        )
        cs = ctx.cursor()
        cs.execute("USE ROLE "+snowRoleArg)
        cs.execute("use warehouse "+snowWHArg)
        ### truncate factDel_table  to ensure the information of factDel_table is the lastest before each load.
        cs.execute("truncate table "+tableName)
        print("truncate table "+tableName+" succeessfully!")
        #put local file to snowflake table stage
        sql="put file://"+LOCAL_FILE+" @%"+tableName
        print(sql)
        cs.execute(sql)
        #copy data from table stage to table in snowflake
        FILE_FORMAT = """(
                              type = csv 
                              field_delimiter = ','  
                              FIELD_OPTIONALLY_ENCLOSED_BY= '"' 
                              )"""
        cs.execute("copy into "+tableName+" file_format="+FILE_FORMAT)
        print("Load processed data successfully") 

    except Exception as e:
        print(e)
        raise
    finally:
        cs.close()
        ctx.close()
        # delete local file
        os.remove("./"+LOCAL_FILE)


run()