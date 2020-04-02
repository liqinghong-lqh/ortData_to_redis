#!usr/bin/python
#-*-coding:UTF-8 -*-
import datetime
import cx_Oracle
import redis

from oracle_pool import OraclePool
# 连接数据库
from send_email import Messages, Email

# # 数据库连接池
oracle = OraclePool('Oracle')
# # 原生连接共pandas使用
conn = OraclePool.conn('Oracle')

#描述：统计每周各个项目ORT抽样确认数据信息，存储至Redis
#作者：liqinghong
#时间：2020-04-01

def excute_project():
    try:
        now_time = datetime.date.today()
        one_day=datetime.timedelta(days=1)
        yesterday=now_time-one_day
        yesterday_time=yesterday.strftime("%Y-%m-%d")
        year=yesterday.strftime("%Y")
        begin_time=yesterday_time+" 00:00:00"
        print(begin_time)
        end_time=yesterday_time+" 23:59:59"
        print(end_time)
        ort_sql=''' SELECT      
                 PROJECT_NO,
                 scheme_code,
                 customer_pn,
                 operation_no,   
                 PRODUCT_WEEK,
                 PRODUCT_DATE,
                 THE_ANNUAL,
                 ITEM_NO,
                 WORK_CENTER_NO,
                 SAMPLE_NUMBER,
                count(SFC_NO) as TOTAL
                 from ( 
                   select i.sfc_no as sfc_no,
                         to_char( to_date( wh.create_date,'yyyy-MM-dd hh24:mi:ss'),'yyyy-MM-dd') as product_date,
                         FUN_GET_SWD_WEEKDAY(to_date( wh.create_date,'yyyy-MM-dd hh24:mi:ss')) AS PRODUCT_WEEK,
                         to_char(to_date( wh.create_date,'yyyy-MM-dd hh24:mi:ss'),'yyyy') as THE_ANNUAL,
                         wh.result as apmt_result ,
                         di.value as project_no,
                         wh.item_no as item_no,
                         rt.scheme_code as SCHEME_CODE ,
                         rt.customer_pn as CUSTOMER_PN,
                         rt.operation_no as OPERATION_NO,
                         rt.sample_number as SAMPLE_NUMBER,
                         wh.workcenter_no as work_center_no
                from sfc_info i,  sfc_wip_log_his wh,m_custom_data_ins di,m_bom b,XWD_ORT_SAMPLE_INFO rt
                where 
                substr(i.bom_no,0,3)=100
                and di.context_id=b.pid
                and i.item_no=b.bom_no
                and wh.item_no=rt.swd_item
                and di.custom_data_id='6659d145de584f08aab8f78e6e4c912b'
                and i.pid=wh.sfc_id
                and di.value=rt.project_no
                and substr(wh.workcenter_no,1,2)='SW'
                and wh.create_date between to_char(to_date('{begin_time}','yyyy-MM-dd hh24:mi:ss'),'yyyyMMddhh24miss')  
                and  to_char(to_date('{end_time}','yyyy-MM-dd hh24:mi:ss'),'yyyyMMddhh24miss')
                and wh.operation_no=rt.operation_no
                union  
               select  i.sfc_no  as sfc_no,
                       to_char(to_date( w.create_date,'yyyy-MM-dd hh24:mi:ss'),'yyyy-MM-dd') as product_date,
                       FUN_GET_SWD_WEEKDAY(to_date( w.create_date,'yyyy-MM-dd hh24:mi:ss')) AS PRODUCT_WEEK,
                       to_char(to_date( w.create_date,'yyyy-MM-dd hh24:mi:ss'),'yyyy') as THE_ANNUAL,
                       w.result as apmt_result,
                       di.value as project_no,
                       w.item_no as item_no,
                       rt.scheme_code as SCHEME_CODE ,
                       rt.customer_pn as CUSTOMER_PN,
                       rt.operation_no as OPERATION_NO,
                       rt.sample_number as SAMPLE_NUMBER,
                       w.workcenter_no as WORK_CENTER_NO 
                 from sfc_info i,  sfc_wip_log w,m_custom_data_ins di,m_bom b,XWD_ORT_SAMPLE_INFO rt
                 where
                 substr(i.bom_no,0,3)=100
                 and di.context_id=b.pid
                 and i.item_no=b.bom_no
                 and  i.item_no=rt.swd_item
                 and di.custom_data_id='6659d145de584f08aab8f78e6e4c912b'
                 and i.pid=w.sfc_id
                 and di.value=rt.project_no
                 and substr(w.workcenter_no,1,2)='SW'
                 and w.create_date  between to_char(to_date('{begin_time}','yyyy-MM-dd hh24:mi:ss'),'yyyyMMddhh24miss')  
                  and  to_char(to_date('{end_time}','yyyy-MM-dd hh24:mi:ss'),'yyyyMMddhh24miss')
                 and w.operation_no=rt.operation_no
                 )  
                 group by PROJECT_NO,SCHEME_CODE,CUSTOMER_PN,ITEM_NO,
                   WORK_CENTER_NO,PRODUCT_DATE,THE_ANNUAL,PRODUCT_WEEK,OPERATION_NO,SAMPLE_NUMBER order by PRODUCT_DATE  
        '''.format(begin_time=begin_time,end_time=end_time)

        dsn = cx_Oracle.makedsn('aph-scan.sunwoda.cn', 1521, service_name='aph')
        orcl = cx_Oracle.connect('aphmes', 'aph2018mes1314', dsn)
        curs = orcl.cursor()
        curs.execute(ort_sql)
        desc = [d[0] for d in curs.description]
        result = [dict(zip(desc, line)) for line in curs]
        curs.close()

        # 创建redis 连接
        r = redis.Redis(host='192.168.9.50', password='Aphredis168',port=6380,db=0)
        #设置key 值，并循环获取字典中的数据
        for value in result:
            value1=value.get("PRODUCT_WEEK")
            ort_key=year+'_'+value1
            value2=str(value)
            r.sadd(ort_key,value2)
        try:
                # 邮件内容
                message = """
                           <h3>'{yesterday_time}'ORT抽样确认数据已成功存储至redis</h3>""".format(yesterday_time=yesterday_time)

                # 邮件需发送的人的邮箱
                my_users=''' select u.email_address from mail_type t, mail_user u, mail_type_user s
                            where t.mail_type_id = s.mail_type_id
                             and s.user_id = u.user_id and t.mail_type_id = '6' '''
                list=oracle.get_all(my_users)
                my_user = []  #预警邮件收件人列表
                for i in list :
                    print(i[0])
                    my_user.append(i[0])
                    print(my_user)
                # my_user = ['lqh@sunwoda.com']
                # 发送邮件的邮箱
                my_sender = "mes_xwd_yxt@sunwoda.com"
                message = Messages(my_sender, my_user, "ORT抽样确认数据已成功存储至redis", message).generate_message()
                Email(my_sender, my_user, "sunwoda+2019", "mail.sunwoda.com").send_mail(message)
        except Exception as e:
             print(e)
             print('成功')
    except Exception as e:
        print(e)

if __name__=='__main__':
    excute_project()