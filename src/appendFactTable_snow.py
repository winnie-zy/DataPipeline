import json
import boto3
import csv
import uuid
from datetime import datetime
import re
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


def run():  
    try:
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
       cs.execute("""insert into fact_table(video_id,date_time,platform_id,site_id)
                               select A.video_id,B.date_time,C.platform_id,D.site_id 
                               from  factDel_table
                               left join dimVideo_table A
                               on  A.video=factDel_table.video
                               left join dimDate_table B
                               on B.date_time=factDel_table.date_time
                               left join dimPlatform_table C
                               on C.platform=factDel_table.platform
                               left join dimSite_table D
                               on D.site_name=factDel_table.site_name""")

       print("Append fact_table data successfully!")
    except Exception as e:
            print(e)
            raise
    finally:
        cs.close()
        ctx.close()

run()