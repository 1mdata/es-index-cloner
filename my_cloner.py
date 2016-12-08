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
from elasticsearch.helpers import bulk,streaming_bulk,BulkIndexError

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
        self.mongo_update_type = config['mongo']['update_type']
        self.mongo_where_field_in_es = config['mongo']['where_field_in_es']
        self.mongo_where_field = config['mongo']['where_field']
        
        self.mysql_uri = config['mysql']['uri']
        self.sql_query = config['mysql']['query']
        self.sql_bulk_size = config['mysql']['bulk_size']
        self.sql_where_field_in_es = config['mysql']['where_field_in_es']
        self.sql_where_field = config['mysql']['where_field']

        kw = {
            "sniff_on_start":True,
            "sniff_on_connection_fail": True,
            "sniffer_timeout": 60
        }
        self.update_type = config['elastic']['update_type']
        self.update_field = config['elastic']['update_field']
        self.update_id_in_source = config['elastic']['update_id_in_source']
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

    def _bulk_update_mongo(self,bulks):
        if (len(bulks)>0):
            collection = self.mongo_db[self.mongo_update_type]
            bulk = collection.initialize_ordered_bulk_op()
            for key,find,update in bulks:
                bulk.find(find).update(update)
            res = bulk.execute()
            return res
        return 0

    # 直接从 mysql 更新
    def _update_with_sql(self):
        pass
    # 从 es 反查询
    def _update_with_es(self):
        kw={
            'index': self.es_index,
            'doc_type': self.query_type,
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
            if (hits_size>0):
                res = self._bulk_es_mongo(hits)
            #
            # dealt size
            dealt_size += hits_size
            bar.goto(dealt_size)
        # done
        print('\nDone !')

    def _build_sql(self,hits):
        inIds = [ y['_source'][self.sql_where_field_in_es] for y in hits if y.get('_source')]
        if (len(inIds)>0):
            sql = '%s AND %s IN (%s)' % (self.sql_query,self.sql_where_field,','.join(str(i) for i in inIds if str(i).isnumeric()))
        else:
            sql = self.sql_query
        # print(sql)
        return [(sql,inIds)]

    def _query_sql(self,sql):
        connection = pymysql.connect(cursorclass=pymysql.cursors.DictCursor,**self.mysql_uri)
        res = []
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                res = cursor.fetchall()
        except:
            print(sql)
        finally:
            connection.close()
            return res
    # 处理 es 结果为需要更新的数据
    def _compile_es_hits(self,hits):
        def _find_hits(inid):
            #docs = list(filter(lambda x:x.get('_source') and x.get('_source')[self.sql_in_field] == inid, hits))
            docs = [ x for x in hits if x.get('_source') and x.get('_source')[self.sql_where_field_in_es] == str(inid) ]
            return docs
        sqls = self._build_sql(hits)
        news = []
        for sql,ids in sqls:
            res = self._query_sql(sql)
            for re in res:
                inid = re[self.sql_where_field_in_es]
                docs = _find_hits(inid)
                for doc in docs:
                    hits.remove(doc)
                    for field in self.update_field:
                        doc['_source'][field] = re.get(field)
                    news.append(doc)
        return news
    def _build_hits_update_bulk_es_and_mongo(self,hits):
        bulk_actions = []
        bulk_mongo = []
        for hit in hits:
            doc_id = hit.get('_id')
            doc_type = hit.get('_type')
            doc_version = hit.get('_version')
            doc_parent = hit.get('_parent')
            doc_routing = hit.get('_routing')

            doc = {}
            doc['_index'] = self.es_index
            doc['_type'] = doc_type
            doc['_id'] = doc_id

            if doc_parent:
                doc['_parent'] = doc_parent
            if doc_routing:
                doc['_routing'] = doc_routing
            
            source = {}
            for field in self.update_field:
                if hit.get('_source'):
                    value = hit.get('_source')[field]
                    source[field] = value
            
            action = doc.copy()
            if (hit.get('_source') and hit.get('_source').get(self.update_id_in_source)):
                action['_id'] = hit.get('_source')[self.update_id_in_source]
                action['_op_type'] = 'update'
                action['_type'] = self.update_type
                action['doc'] = source
                del action['_parent']
                mongo_filter = {}
                mongo_filter[self.mongo_where_field] = hit.get('_source')[self.mongo_where_field_in_es]
                mongo_set = {}
                mongo_set['$set'] = source
                bulk_mongo.append((self.mongo_update_type,mongo_filter,mongo_set))
                bulk_actions.append(action)
        return (bulk_actions,bulk_mongo)

    # 根据从 es 中获取到的数据执行查询 mysql ，并作出更新操作
    def _bulk_es_mongo(self,hits):
        hits = self._compile_es_hits(hits)
        (actions,mongo) = self._build_hits_update_bulk_es_and_mongo(hits)
        # 1, update es
        kw = {
                "raise_on_exception":False
        }
        #print(actions)
        #print(mongo)
        try:
            res = streaming_bulk(client=self.es,actions=actions,**kw)
            #print(res)
            okNum = 0
            for ok,re in res:
                if not ok:
                    print(re)
                else:
                    okNum+=1
            if (okNum>0):
                self.es.indices.refresh(index=self.es_index)
        except:
            print(actions)
        # 2, update mongo
        res = self._bulk_update_mongo(mongo)
        #print(res)
        return actions

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Update part field from mysql to es and mongodb")
    parser.add_argument('-f',action='store',dest='yaml_file',help='yaml config')

    arguments = parser.parse_args()
    
    config = yaml.load(open(arguments.yaml_file))
    MyCloner(config).update()
