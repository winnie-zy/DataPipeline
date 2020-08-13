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
       ### truncate DIMSITE_DEL TABLE to ensure the information of dimSiteDel_table is the lastest.
       cs.execute("USE ROLE "+snowRoleArg)
       cs.execute("use warehouse "+snowWHArg)
       cs.execute("""truncate table dimSiteDel_table""")
       print("truncate table dimSiteDel_table succeessfully!")

       ####initiate dimSiteDel_table
       cs.execute("""insert into dimSiteDel_table select site_name from factDel_table group by site_name""")
       print("Initiate into dimSiteDel_table successfully!")

       ####upsert dimSite_table
       cs.execute("""insert into dimSite_table(site_name) 
                     select dimSiteDel_table.site_name 
                     from dimSiteDel_table 
                     left join dimSite_table 
                     on dimSiteDel_table.site_name=dimSite_table.site_name 
                     where dimSite_table.site_name is null""")
       print("Upsert into dimSite_table successfully!")

    except Exception as e:
            print(e)
            raise
    finally:
        cs.close()
        ctx.close()

# print(run())
run()
    
