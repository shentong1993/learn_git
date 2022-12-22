#coding=utf-8
import os,sys
sys.path.append('/home/yangningning/hcfc/utils')
import common_utils
from mysql_client import MysqlClient;
from hive_client import HiveClient;
from datetime import datetime, date, timedelta;
from typing import List;

def bdl_quick_check_config(target_table_name,target_table_threshold):

    table_name = target_table_name

    table_threshold = target_table_threshold
    #根据传入的值，进行判断是哪种监控类型
    strategy_type = -999
    if table_threshold >= 1:
        #行进行比较的类型
        strategy_type = 0
    else:
        #百分比进行比较的类型
        strategy_type = 1
    #以上的配置规则插入配置表，对监控表的配置初始化--生产环境
    #insert_sql = "insert into sharedb.bdl_quick_check_config(target_table_name,target_table_threshold,strategy_type) values ({0},{1},{2});".format(table_name,table_threshold,strategy_type)
    #以上的配置规则插入配置表，对监控表的配置初始化--测试环境
    insert_sql = "insert into test.bdl_quick_check_config(target_table_name,target_table_threshold,strategy_type) values ({0},{1},{2});".format(table_name,table_threshold,strategy_type)
    #insert_sql = "select * from test.bdl_quick_check_config;"
    mysql_client = MysqlClient()
    res=mysql_client.query(insert_sql)
    print(res)

if __name__ == '__main__':
    #配置表里的表名
    target_table_name = sys.argv[1]
    #告警监控策略-百分比/数值阈值：下一步进行判断，大于1则是行监控，小于1则是百分比监控
    target_table_threshold = float(sys.argv[2])
    #方法调用
    bdl_quick_check_config(target_table_name,target_table_threshold);