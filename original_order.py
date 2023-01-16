#Author: Xinghan Guo
#Date: 2021.11.18
# ******************************************************************************
time = __import__('time')
datetime = __import__('datetime')
string = __import__('string')

def create_table(tdw, db_name, table_name):
    sql = '''
CREATE TABLE IF NOT EXISTS %s(
            imp_date BIGINT COMMENT '分区日期，格式：yyyyMMdd',
            cid_title STRING COMMENT '剧目名称',
            spu STRING COMMENT '商品SPU名称',
            r1_operation BIGINT COMMENT '运营自配置的选品排序',
            r2_product_num BIGINT COMMENT '根据支付商品数的选品排序',
            r3_recommendation BIGINT COMMENT '算法工具SPU选品排序'
         )
        COMMENT 'SPU选品排序合表'
        PARTITION BY LIST(imp_date)
        (
            PARTITION default
        )
        STORED AS RCFILE
    ''' % (table_name)
    tdw.WriteLog(("execute sql: %s ") % (sql))
    tdw.execute(sql)

# 插入表函数，修改sql字符串内的SQL插入数据代码
def insert_table(tdw, datehour, table_name):
    sql = '''
    INSERT OVERWRITE TABLE %(table_name)s PARTITION(imp_date=%(datehour)s)

    select  coalesce(a.imp_date, b.imp_date, c.imp_date) as imp_date,
            coalesce(a.cid_title, b.cid_title, c.prefer_name) as cid_title,
            coalesce(a.spu, b.spu, c.spu) as spu,
            a.rank_num as r1_operatoin, 
            b.rank_num as r2_product_num, 
            c.rank_num as r3_recommendation
    from  pcg_video_commerical::dwd_app_spu_rank_operation_d_zl a
    full join  pcg_video_commerical::dwd_app_spu_rank_product_num_d_zl b
    on  a.imp_date = b.imp_date and 
        a.cid_title = b.cid_title and 
        a.spu = b.spu
    full join  pcg_video_commerical::dwd_app_spu_rank_recommendation_d_zl c
    on  a.imp_date = c.imp_date and
        a.cid_title = c.prefer_name and
        a.spu = c.spu
    where coalesce(a.imp_date, b.imp_date, c.imp_date) = %(datehour)s 
    order by 1,2,4

    ''' % {'datehour': datehour,    
          'table_name': table_name }
    tdw.WriteLog(('execute sql:\\N%s') % (sql))
    tdw.execute(sql)


# ”主“函数，调用上面两个函数实现建表和修改表的功能
def TDW_PL(tdw, argv):
    datehour = argv[0]
    table_name = "dwd_app_spu_rank_collection_d_zl"
    db_name = "pcg_video_commerical;"
    tdw.WriteLog('Start processing data %s' % datehour)
    tdw.execute("use " + db_name)
    create_table(tdw, db_name, table_name)
    insert_table(tdw, datehour, table_name)
    tdw.WriteLog("all over")


    
