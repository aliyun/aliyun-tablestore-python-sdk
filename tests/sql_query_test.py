# -*- coding: utf8 -*-

import sys
import unittest
from tests.lib.api_test_base import APITestBase
from tablestore import *
import tests.lib.restriction as restriction
import copy
from tablestore.error import *
import math
import time

if sys.getdefaultencoding() != 'utf-8':
    reload(sys)
    sys.setdefaultencoding('utf-8')

def batch_write_row(client,table_name,tp):
    put_row_items = []
    for i in range(0, 10):
        if tp == "sql_test":
            primary_key = [('uid',str(i)),('pid',i)]
            attribute_columns = [('name','somebody'+str(i)), ('age',i%3),('grade',i+0.2)]
        else:
            primary_key = [('uid',str(i)),('pid',bytearray(i))]
            attribute_columns = [('name','somebody'+str(i)), ('age',i%3),('grade',i+0.2),('isMale',i%2==0),('picture',bytearray(i))]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        item = PutRowItem(row, condition)
        put_row_items.append(item)
    request = BatchWriteRowRequest()
    request.add(TableInBatchWriteRowItem(table_name, put_row_items))
    client.batch_write_row(request)
    
class SqlQueryTest(APITestBase):
    def test_sql_query(self):
        """对SQL执行结果进行测试"""
        table_name = 'SqlQuery' + self.get_python_version()
        table_meta = TableMeta(table_name, [('uid', 'STRING'),('pid', 'INTEGER')])
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MinReadWriteCapacityUnit,
            restriction.MinReadWriteCapacityUnit
        ))
        table_options = TableOptions()
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)
        sql_queries={
            "create_table" :'create table %s (uid VARCHAR(1024), pid BIGINT(20),name MEDIUMTEXT, age BIGINT(20), grade DOUBLE,PRIMARY KEY(uid,pid));' % table_name,
            "count":'select count(*) from %s' % table_name,
            "sum":'select sum(age) from %s' % table_name,
            "where":'select pid from %s where grade > 5.0' % table_name,
            "group_by":'select age,avg(grade) from %s group by age order by age' % table_name,
            "order_by":'select pid from %s order by grade limit 1' % table_name,
            "desc":'desc %s' % table_name,
            "drop_table" :'drop mapping table %s' % table_name
        }
        ground_truth={
            "count":10,
            "sum":9.0,
            "where":[5,6,7,8,9],
            "group_by":[(0,4.7),(1,4.2),(2,5.2)],
            "order_by":0,
            "desc":[[('Default', None), ('Extra', ''), ('Field', 'age'), ('Key', ''), ('Null', 'YES'), ('Type', 'bigint(20)')], 
            [('Default', None), ('Extra', ''), ('Field', 'grade'), ('Key', ''), ('Null', 'YES'), ('Type', 'double')], 
            [('Default', None), ('Extra', ''), ('Field', 'name'), ('Key', ''), ('Null', 'YES'), ('Type', 'mediumtext')], 
            [('Default', None), ('Extra', ''), ('Field', 'pid'), ('Key', 'PRI'), ('Null', 'NO'), ('Type', 'bigint(20)')], 
            [('Default', None), ('Extra', ''), ('Field', 'uid'), ('Key', 'PRI'), ('Null', 'NO'), ('Type', 'varchar(1024)')]]
        }

        def sort_result(input):
            ret = []
            for item in input:
                tmp = sorted(item,key=lambda x:x[0])
                ret.append(tmp)
            ret.sort()
            return ret

        def get_result(row_list,tp):
            ret = []
            for row in row_list:
                ret.append(row.attribute_columns)
            if tp == "count":return ret[0][0][1]
            elif tp == "sum":return ret[0][0][1]
            elif tp == "where":
                where_ret = []
                for item in ret:
                    where_ret.append(item[0][1])
                return where_ret
            elif tp == "group_by":
                group_by_ret = []
                for item in ret:
                    group_by_ret.append((item[0][1],item[1][1]))
                return group_by_ret
            elif tp == "order_by":return ret[0][0][1]
            elif tp == "desc":return sort_result(ret)
        
        def exe_sql_query(query):
            return self.client_test.exe_sql_query(query)

        def CHECK(tp):
            row_list,_,_ = exe_sql_query(sql_queries[tp])
            ret = get_result(row_list,tp)
            self.assert_equal(ret, ground_truth[tp])

        batch_write_row(self.client_test,table_name,"sql_test")
        exe_sql_query(sql_queries["create_table"])
        CHECK("count")
        CHECK("sum")
        CHECK("where")
        CHECK("group_by")
        CHECK("order_by")
        CHECK("desc")
        exe_sql_query(sql_queries["drop_table"])
        self.client_test.delete_table(table_name)
    
    def test_fbs_decoder_types(self):
        """对fbs decoder结果进行测试"""
        table_name = 'fbsDecoderTypes' + self.get_python_version()
        table_meta = TableMeta(table_name, [('uid', 'STRING'),('pid', 'BINARY')])
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MinReadWriteCapacityUnit,
            restriction.MinReadWriteCapacityUnit
        ))
        table_options = TableOptions()
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)
        sql_queries={
            "create_table" :'create table %s (uid VARCHAR(1024), pid VARBINARY(1024), name MEDIUMTEXT, age BIGINT(20), grade DOUBLE, isMale BOOLEAN, picture MEDIUMBLOB, PRIMARY KEY(uid,pid));' % table_name,
            "all":'select * from %s' % table_name,
            "drop_table" :'drop mapping table %s' % table_name
        }

        def get_result(row_list):
            ret = []
            for row in row_list:
                ret.append(row.attribute_columns)
            return ret

        def exe_sql_query(query):
            return self.client_test.exe_sql_query(query)

        def CHECK(ret):
            for i in range(0, 10):
                gt = [('uid',str(i)),('pid',bytearray(i)),('name','somebody'+str(i)), ('age',i%3),
                                ('grade',i+0.2),('isMale',i%2==0),('picture',bytearray(i))]
            self.assert_equal(ret[i].sort(), gt.sort())

        batch_write_row(self.client_test,table_name,"fbs_test")
        exe_sql_query(sql_queries["create_table"])
        row_list,_,_ = exe_sql_query(sql_queries["all"])
        CHECK(get_result(row_list))
        exe_sql_query(sql_queries["drop_table"])
        self.client_test.delete_table(table_name)

if __name__ == '__main__':
    unittest.main()