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
       ### truncate DIMVEDIO_DEL TABLE to ensure the information of dimVideoDel_table is the lastest.
       cs.execute("USE ROLE "+snowRoleArg)
       cs.execute("use warehouse "+snowWHArg)
       cs.execute("""truncate table dimVideoDel_table""")
       print("truncate table dimVideoDel_table succeessfully!")

       ####initiate dimVideoDel_table
       cs.execute("""insert into dimVideoDel_table select video from factDel_table group by video""")
       print("Initiate dimVideoDel_table successfully!")

       ####upset dimVideo_table
       cs.execute("""insert into dimVideo_table(video) 
            select dimVideoDel_table.video from dimVideoDel_table 
            left join dimVideo_table 
            on dimVideoDel_table.video=dimVideo_table.video
            where dimVideo_table.video is null""")
       print("Upset dimVideo_table successfully!")

    except Exception as e:
            print(e)
            raise
    finally:
        cs.close()
        ctx.close()

# print(run())
run()
