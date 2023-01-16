import os
import sys
# from datetime import date, timedelta
# import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, udf, floor
from pyspark.sql.types import StringType, IntegerType, ArrayType
from pytoolkit import TDWSQLProvider, TDWUtil
from sklearn import preprocessing
from pyspark.sql.types import *

os.environ['GROUP_ID'] = 'g_omg_qqv_commerical'  # 从左侧文件树根目录pools.html中选取
os.environ['GAIA_ID'] = '3124'  # 从左侧文件树根目录pools.html中选取
spark = SparkSession.builder.getOrCreate()
spark

def read_tdw_df(db, table_name, columns, sql, date_start, date_end, title):    
    provider = TDWSQLProvider(spark, db=db)
    provider.table(table_name).createOrReplaceTempView(table_name)
    answer = spark.sql(sql.format(table_name, date_start, date_end, title)).collect()
    df = pd.DataFrame(data=answer, columns=columns)
    return df
    
db='pcg_video_commerical'
table_name='dwd_app_spu_rank_collection_d_zl'
columns = ['imp_date', 'cid_title', 'spu','r1','r2','r3'] #r3为基于r1的相对排序
date_start=20211101
date_end=20211130
title='斗罗大陆'
sql = '''
       select tt.imp_date, tt.cid_title, tt.spu,
             row_number() over(partition by tt.imp_date, tt.cid_title order by tt.r1) as r1,
             tt.r2,
             tt.r3
       from  
       (
           select t.imp_date, t.cid_title, t.spu,
                  if(t.r1_operation is null, t.length/t.null_num, t.r1_operation) as r1,
                  if(t.r2_product_num is null, t.length, t.r2_product_num) as r2,
                  if(t.r3_recommendation is null, t.length, row_number() over(partition by t.imp_date, t.cid_title order by t.r3_recommendation)) as r3
           from 
           (
               select b.imp_date, b.cid_title, b.spu, b.r1_operation, b.r2_product_num, b.r3_recommendation, a.length, a.null_num
               from 
               (
                   select imp_date, cid_title, count(*) as length, count(*)-count(r1_operation) as null_num
                   from {}
                   where imp_date between {} and {} and cid_title = '{}' and (r1_operation is not null or r2_product_num is not null)
                   group by imp_date, cid_title
               ) a
               left join
               (
                   select imp_date, cid_title, spu, r1_operation, r2_product_num, r3_recommendation
                   from {}
                   where imp_date between {} and {} and cid_title = '{}' and (r1_operation is not null or r2_product_num is not null)
               ) b
              on a.imp_date = b.imp_date and a.cid_title = b.cid_title
           ) t
       ) tt

      '''.format(table_name,date_start,date_end,title,table_name,date_start,date_end,title)


df = read_tdw_df(db, table_name, columns, sql, date_start, date_end, title)

#pd.set_option('display.max_rows', 500)

import numpy
from numpy import asarray

np.sort(df['imp_date'].unique())

df_table = pd.DataFrame(columns=['r1-r2', 'r2-r3', 'r1-r3'])
def spearman_footrule_distance(df_table): #计算s,t之间的距离
    
    for i in np.sort(df['imp_date'].unique()):
       
        r1 = df.loc[df['imp_date']==i,'r1']
        r2 = df.loc[df['imp_date']==i,'r2']
        r3 = df.loc[df['imp_date']==i,'r3']
        r1 = np.array(r1)
        r2 = np.array(r2)
        r3 = np.array(r3)

        assert len(r1) == len(r2)
        sdist1 = sum(abs(asarray(r1) - asarray(r2)))
        c1= len(r1) % 2
        normalizer1 = 0.5*(len(r1)**2 - c1)

        assert len(r2) == len(r3)
        sdist2 = sum(abs(asarray(r2)- asarray(r3)))
        c2= len(r2)% 2
        normalizer2 = 0.5*(len(r2)**2 - c2)

        assert len(r1) == len(r3)
        sdist3= sum(abs(asarray(r1)- asarray(r3)))
        c3= len(r1)% 2
        normalizer3 = 0.5*(len(r1)**2 - c3)
        
        s = pd.Series({'r1-r2':sdist1/normalizer1, 'r2-r3':sdist2/normalizer2, 'r1-r3':sdist3/normalizer3}, name = i)
        df_table = df_table.append(s)
    return df_table
 distance = spearman_footrule_distance(df_table)  
