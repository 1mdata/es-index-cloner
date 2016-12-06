#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
1，首先查询 es 按照相关条件执行查询
2，通过 es 返回的结果，和 sql 的配置构建 sql 查询语句
3，从 mysql 中查询出相应的结果更新到 es 和 MongoDB 中
"""
import argparse
import yaml

# es
from progress.bar import ShadyBar
from elasticsearch import Elasticsearch,connection as es_connection
from elasticsearch.helpers import streaming_bulk,BulkIndexError

# MongoDB
from pymongo import MongoClient
# MySQL
import pymysql.cursors

class MyCloner(object):
    def __init__(self,config):
        self.config = config
        self.bulk_size = config['bulk_size']

        self.mongo_client = MongoClient(config['mongo']['uri'])
        self.mongo_db = self.mongo_client[config['mongo']['db']]
        
        self.mysql_uri = config['mysql']['uri']
        self.sql_query = config['mysql']['query']
        self.sql_bulk_size = config['mysql']['bulk_size']
        self.sql_in_field = config['mysql']['in_field']

        kw = {
            "sniff_on_start":True,
            "sniff_on_connection_fail": True,
            "sniffer_timeout": 60
        }
        self.update_type = config['elastic']['update_type']
        self.update_field = config['elastic']['update_field']
        self.source_field = config['elastic']['source_field']
        self.query_type = config['elastic']['query_type']
        self.query = config['elastic']['query']
        self.es_index = config['elastic']['index']
        self.bulk_size = config['elastic']['bulk_size']
        # es
        self.es = Elasticsearch(config['elastic']['host'],**kw)
    
    # 执行更新
    def update(self):
        self._update_with_es()

    def _search_es_and_sql_update():
        pass
    # 直接从 mysql 更新
    def _update_with_sql(self):
        pass
    # 从 es 反查询
    def _update_with_es(self):
        kw={
            'index': self.es_index,
            'scroll': '1m',
            'search_type': 'scan',
            'size': self.bulk_size
        }
        scroll = self.es.search(**kw)
        sid = scroll['_scroll_id']
        total_size = scroll['hits']['total']
        hits_size = total_size
        dealt_size = 0
        print("docs: " + str(total_size))
        suffix = '%(percent)d%% - %(index)d [%(elapsed_td)s / %(eta_td)s]'
        bar = ShadyBar("clone",suffix=suffix,max=total_size)
        while (hits_size>0):
            scroll = self.es.scroll(scroll_id=sid,scroll='1m')
            sid = scroll['_scroll_id']
            hits = scroll['hits']['hits']
            hits_size = len(hits)
            # todo
            res = self._bulk_es_mongo(hits)
            #
            # dealt size
            dealt_size += hits_size
            bar.goto(dealt_size)
        # done
        print('\nDone !')

    def _build_sql(self,hits):
        inIds = [ y['_source'][self.sql_in_field] for y in hits if y.get('_source')]
        sql = '%s AND %s IN (%s)' % (self.sql_query,self.sql_in_field,','.join(inIds))
        return [(sql,inIds)]

    def _query_sql(self,sql):
        connection = pymysql.connect(self.mysql_uri)
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                result = cursor.fetchall()
                return result
        finally:
            connection.close()
    # 处理 es 结果为需要更新的数据
    def _compile_es_hits(self,hits):
        def _find_hits(inid):
            #docs = list(filter(lambda x:x.get('_source') and x.get('_source')[self.sql_in_field] == inid, hits))
            docs = [ x for x in hits if y.get('_source') and y.get('_srouce')[self.sql_in_field] == inid ]
            return docs
        sqls = self._build_sql(hits)
        news = []
        for sql,ids in sqls:
            res = self._query_sql(sql)
            for re in res:
                inid = re[self.sql_in_field]
                docs = _find_hits(inid)
                for doc in docs:
                    for field in self.update_field:
                        doc[field] = re.get(field)
                    news.append(doc)

        return news

    # 根据从 es 中获取到的数据执行查询 mysql ，并作出更新操作
    def _bulk_es_mongo(self,hits):
        hits = self._compile_es_hits(hits)

        pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Update part field from mysql to es and mongodb")
    parser.add_argument('-f',action='store',dest='yaml_file',help='yaml config')

    arguments = parser.parse_args()
    
    config = yaml.load(open(arguments.yaml_file))
    MyCloner(config)
