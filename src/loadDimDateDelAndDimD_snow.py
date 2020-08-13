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

charset='utf8'
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
       ####truncate DIMDATE_DEL TABLE to ensure the information of dimDateDel_table is the lastest.
       cs.execute("USE ROLE "+snowRoleArg)
       cs.execute("use warehouse "+snowWHArg)
       cs.execute("""truncate table dimDateDel_table""")
       print("truncate table dimDateDel_table succeessfully!")

       ####initiate dimDateDel_table
       cs.execute("""insert into dimDateDel_table select date_time from factDel_table group by date_time""")
       print("Initiate dimDateDel_table successfully!")

       ####initiate and update dimDate_table
       cs.execute("""insert into dimDate_table(date_time) 
                               select dimDateDel_table.date_time from dimDateDel_table 
                               left join dimDate_table 
                               on dimDateDel_table.date_time=dimDate_table.date_time
                               where dimDate_table.date_time is null
                            """)
       print("Upsert into dimDate_table successfully!") 
    except Exception as e:
            print(e)
            raise
    finally:
        cs.close()
        ctx.close()
    
# print(run())
run()