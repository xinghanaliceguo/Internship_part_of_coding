# Author: Xinghan Guo
# Date: 2021.11.18
# ******************************************************************************
time = __import__('time')
datetime = __import__('datetime')
string = __import__('string')

def create_table(tdw, db_name, table_name):
    sql = '''
CREATE TABLE IF NOT EXISTS %s(
            imp_date BIGINT COMMENT '分区日期，格式：yyyyMMdd',
            prefer_type STRING COMMENT '类型（IP或剧目）',
            prefer_name STRING COMMENT 'IP或剧目名称',
            spu STRING COMMENT '商品SPU名称',
            grade DOUBLE COMMENT '商品SPU偏好度',
            rank_num BIGINT COMMENT '算法工具SPU选品排序'
         )
        COMMENT '算法工具SPU选品排序'
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

    select distinct t.imp_date, 
                    t.prefer_type, 
                    t.prefer_name, 
                    t.spu, 
                    t.grade, 
                    t.rank_num
    from 
        (
        select  imp_date,
                prefer_type,
                prefer_name,
                spu,
                grade,
                row_number() over(partition by imp_date, prefer_type, prefer_name order by grade desc) as rank_num
        from  pcg_video_commerical::dwd_app_profile_prefer_zl
        where imp_date = %(datehour)s 
          and prefer_type in ('video_interest', 'ip_interest')
        ) t
    order by t.imp_date, 
             t.prefer_type, 
             t.prefer_name, 
             t.rank_num

    ''' % {'datehour': datehour,    
          'table_name': table_name }
    tdw.WriteLog(('execute sql:\\N%s') % (sql))
    tdw.execute(sql)


# ”主“函数，调用上面两个函数实现建表和修改表的功能
def TDW_PL(tdw, argv):
    datehour = argv[0]
    table_name = "dwd_app_spu_rank_recommendation_d_zl"
    db_name = "pcg_video_commerical;"
    tdw.WriteLog('Start processing data %s' % datehour)
    tdw.execute("use " + db_name)
    create_table(tdw, db_name, table_name)
    insert_table(tdw, datehour, table_name)
    tdw.WriteLog("all over")
 


