import json
import boto3
import csv
import uuid
from datetime import datetime
import pandas as pd
import re
# import pymysql
# from sqlalchemy import create_engine
import logging
import os
import snowflake.connector

##used for docker and barch job
snowAccountArg=''
snowUserArg= ''
snowPassArg= ''
snowDBArg= ''
snowWHArg= ''
snowSchemaArg= ''
snowRoleArg= ''

# def run(event,context):
def run():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    try:
    #    print(event)
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
       ####truncate dimPlatformDel_table TABLE to ensure the information of dimPlatformDel_table is the lastest.
       cs.execute("USE ROLE "+snowRoleArg)
       cs.execute("use warehouse "+snowWHArg)
       cs.execute("""truncate table dimPlatformDel_table""")
       print("truncate table dimPlatformDel_table succeessfully!")

       ####initiate dimPlatformDel_table
       cs.execute("""insert into dimPlatformDel_table select platform from factDel_table group by platform""")
       print("Initiate dimPlatformDel_table successfully!")

       ####initiate and update dimPlatform_table
       cs.execute("""insert into dimPlatform_table(platform) 
            select dimPlatformDel_table.platform from dimPlatformDel_table 
            left join dimPlatform_table 
            on dimPlatformDel_table.platform=dimPlatform_table.platform
            where dimPlatform_table.platform is null""")
       print("Upsert into dimPlatform_table successfully!") 
    #    response = sns.publish(
    #        TopicArn='',   
    #        Message='Upsert into dimPlatform_table successfully!', 
    #        Subject='dimPlatform_table' 
    #     ) 
    except Exception as e:
            print(e)
            raise
    finally:
        cs.close()
        ctx.close()
    
# print(run())
run()