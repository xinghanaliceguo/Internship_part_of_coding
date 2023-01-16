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
            cid_title STRING COMMENT '剧目名称',
            spu STRING COMMENT '商品SPU名称',
            tot_product_num BIGINT COMMENT '当日支付商品数（销量）',
            rank_num BIGINT COMMENT '根据支付商品数的选品排序'
         )
        COMMENT '根据支付商品数的SPU选品排序'
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
                    t.cid_title, 
                    t.SPU, 
                    t.tot_product_num,
                    t.rank_num
    from 
        (
        select  tt.imp_date, 
                tt.cid_title, 
                tt.SPU, 
                tt.tot_product_num,
                row_number() over(partition by tt.imp_date, tt.cid_title order by tt.tot_product_num desc) as rank_num
        from 
            (
            select  imp_date, 
                    b_title as cid_title,
                    coalesce(spu_title, a.product_title) as SPU,
                    sum(product_num) as tot_product_num
            from 
                (
                select  imp_date, 
                        product_title, 
                        product_num, 
                        cid
                from  pcg_video_dwd::dwd_app_personal_live_ecom_trade_order_d_zl  
                where imp_date = %(datehour)s 
                  and from_unixtime(unix_timestamp(order_pay_time,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') = '%(datehour)s' 
                  and is_order_pay_today = 1
                ) a
            left join 
                (
                select  cid, 
                        b_title
                from  pcg_video_dim::dim_all_content_cid_h_ql
                where imp_hour = %(datehour)s23
                ) b
            on a.cid = b.cid
            left join 
                (    
                select distinct spu_title, 
                                product_title
                from  pcg_video_commerical::dwd_product_spu 
                ) c
            on a.product_title = c.product_title
            where coalesce(spu_title, a.product_title) is not null
            group by imp_date, 
                     b_title, 
                     coalesce(spu_title, a.product_title)
            ) tt
        ) t
    order by t.imp_date, 
             t.cid_title, 
             t.rank_num


    ''' % {'datehour': datehour,    
          'table_name': table_name }
    tdw.WriteLog(('execute sql:\\N%s') % (sql))
    tdw.execute(sql)


# ”主“函数，调用上面两个函数实现建表和修改表的功能
def TDW_PL(tdw, argv):
    datehour = argv[0]
    table_name = "dwd_app_spu_rank_product_num_d_zl"
    db_name = "pcg_video_commerical;"
    tdw.WriteLog('Start processing data %s' % datehour)
    tdw.execute("use " + db_name)
    create_table(tdw, db_name, table_name)
    insert_table(tdw, datehour, table_name)
    tdw.WriteLog("all over")
 


