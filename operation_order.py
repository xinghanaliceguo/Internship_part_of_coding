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
            new_item_idx DOUBLE COMMENT '旧排序加权',
            rank_num BIGINT COMMENT '运营选品排序'
         )
        COMMENT '运营自配置的SPU选品排序'
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

    select distinct imp_date,
                    cid_title, 
                    spu, 
                    new_item_idx, 
                    rank_num
    from 
        (
        select  imp_date,
                cid_title,
                spu, 
                new_item_idx,
                row_number() over(partition by imp_date, cid_title order by new_item_idx) as rank_num
        from
            (  
            select  t1.imp_date as imp_date, 
                    t1.cid_title as cid_title, 
                    t1.SPU as spu, 
                    sum(t1.num/t2.tot*t1.item_idx) as new_item_idx
            from
                (
                select  substr(cast(imp_hour as string),1,8) as imp_date,
                        b_title as cid_title,
                        if(spu_title is null, title, spu_title) as SPU,
                        cast(item_idx as int) + 1 as item_idx,
                        count(a.item_idx) as num
                from 
                    (
                    select  imp_hour, 
                            goods_id, 
                            item_idx, 
                            get_json_object(udf_kv, '$.cur_pg.cid') as cid
                    from  pcg_video_dwd::dwd_app_multiaction_ecom_h_zl  
                    where substr(cast(imp_hour as string),1,8) = '%(datehour)s'
                      and ei  = 'imp' 
                      and pgid = 'page_goods_list_h5'
                      and eid = 'goods_card' 
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
                    select  goods_id, 
                            title
                    from  pcg_video_dim::dim_app_personal_live_ecom_goods_id_info_d_ql
                    where imp_date = %(datehour)s
                      and title is not null
                    ) c
                on a.goods_id = c.goods_id
                left join 
                    (
                    select  spu_title, 
                            product_title
                    from  pcg_video_commerical::dwd_product_spu
                    ) d
                on c.title = d.product_title
                where if(spu_title is null, title, spu_title) is not null
                group by substr(cast(imp_hour as string),1,8),
                         b_title,
                         if(spu_title is null, title, spu_title),
                         cast(item_idx as int) + 1
                ) t1
            left join
                (             
                select  substr(cast(imp_hour as string),1,8) as imp_date_2,
                        b_title as cid_title,
                        if(spu_title is null, title, spu_title) as SPU_2,
                        count(a.item_idx) as tot
                from 
                    (
                    select  imp_hour, 
                            goods_id, 
                            item_idx, 
                            get_json_object(udf_kv, '$.cur_pg.cid') as cid
                    from  pcg_video_dwd::dwd_app_multiaction_ecom_h_zl  
                    where substr(cast(imp_hour as string),1,8) = '%(datehour)s'
                      and ei  = 'imp'
                      and pgid = 'page_goods_list_h5'
                      and eid = 'goods_card' 
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
                    select  goods_id, 
                            title
                    from  pcg_video_dim::dim_app_personal_live_ecom_goods_id_info_d_ql
                    where imp_date = %(datehour)s
                      and title is not null
                    ) c
                on a.goods_id = c.goods_id
                left join 
                    (
                    select  spu_title, 
                            product_title
                    from  pcg_video_commerical::dwd_product_spu
                    ) d
                on c.title = d.product_title
                where if(spu_title is null, title, spu_title) is not null
                group by substr(cast(imp_hour as string),1,8),
                         b_title,
                         if(spu_title is null, title, spu_title)
                ) t2
            on  t1.imp_date = t2.imp_date_2 
            and t1.cid_title = t2.cid_title 
            and t1.SPU = t2.SPU_2
            group by t1.imp_date, 
                     t1.cid_title, 
                     t1.SPU
            ) tt
        )
        order by imp_date,
                 cid_title, 
                 rank_num

    ''' % {'datehour': datehour,    
          'table_name': table_name }
    tdw.WriteLog(('execute sql:\\N%s') % (sql))
    tdw.execute(sql)


# ”主“函数，调用上面两个函数实现建表和修改表的功能
def TDW_PL(tdw, argv):
    datehour = argv[0]
    table_name = "dwd_app_spu_rank_operation_d_zl"
    db_name = "pcg_video_commerical;"
    tdw.WriteLog('Start processing data %s' % datehour)
    tdw.execute("use " + db_name)
    create_table(tdw, db_name, table_name)
    insert_table(tdw, datehour, table_name)
    tdw.WriteLog("all over")
 


