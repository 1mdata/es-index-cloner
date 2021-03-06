#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import logging.config

LOG_FILE = "cloner.log"

from progress.bar import ShadyBar
from elasticsearch import Elasticsearch,connection as es_connection
from elasticsearch.helpers import streaming_bulk,BulkIndexError
from elasticsearch.exceptions import *

class IndexCloner(object):
    def __init__(self, source_index, target_index, source_es_ip_port,target_es_ip_port, shard_count, replica_count,bulk_size,source_sort):
        self.source_index = source_index
        self.source_sort = source_sort
        self.target_index = target_index
        self.shard_count = shard_count
        self.replica_count = replica_count
        self.bulk_size = bulk_size

        self.logger = logging.getLogger("IndexCloner")
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(message)s");
        handler = logging.handlers.RotatingFileHandler(LOG_FILE,maxBytes=4096,backupCount=1)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        if(target_es_ip_port.strip()==''):
                target_es_ip_port = source_es_ip_port

        kw = {
                "connection_class": es_connection.RequestsHttpConnection,
                "sniff_on_start": True,
                "sniff_on_connection_fail": True,
                "sniffer_timeout":60
        }
        source_kw = kw.copy()
        target_kw = kw.copy()
        self.source_es = Elasticsearch(source_es_ip_port.split(','),**source_kw)
        self.target_es = Elasticsearch(target_es_ip_port.split(','),**target_kw)

    def clone(self):
        self._copy_mappings()
        self._copy_data()

    def _copy_mappings(self):
        source_mappings = self._get_mappings()

        index_settings_mappings = {
                 "settings": {"index": {"number_of_shards": self.shard_count, "number_of_replicas": self.replica_count}}
                ,"mappings": source_mappings['mappings']}
        # Create index with settings and mappings
        exists = self.target_es.indices.exists(index=self.target_index,ignore=[404])
        self.target_index_is_old = exists
        if exists == False:
            self.target_es.indices.create(index=self.target_index,body=index_settings_mappings)
        

    def _get_mappings(self):
        r = self.source_es.indices.get_mapping(index=self.source_index) 
        source_mappings = r
        return source_mappings[self.source_index]
    
    # compile hits to bulk action and mget body
    def _compile_hits_bulk_and_mget(self,hits):
        mget_docs = []
        bulk_actions = []
        hit = None
        for hit in hits:
            doc_id = hit.get('_id')
            doc_type = hit.get('_type')
            doc_version = hit.get('_version')
            doc_parent = hit.get('_parent')
            doc_routing = hit.get('_routing')
            
            doc = {}
            doc['_index'] = self.target_index
            doc['_type'] = hit.get('_type')
            doc['_id'] = hit.get('_id')
            
            if doc_parent:
                doc['_parent'] = doc_parent
            if doc_routing:
                doc['_routing'] = doc_routing
            
            action = doc.copy()
            doc['_source'] = False
            action['_source'] = hit.get('_source')
            action['_version'] = doc_version
            mget_docs.append(doc)
            bulk_actions.append(action)
        return (bulk_actions,mget_docs)

    
    def _bulk_hits(self,hits):
        actions,docs = self._compile_hits_bulk_and_mget(hits)
        mget = self.target_es.mget(body={"docs":docs},index=self.target_index,_source=False,ignore=[404])
        docs = mget.get('docs')
        if docs:
            cleanly = []
            for action in actions:
                exclude = False
                for doc in docs:
                    if ( doc.get('found') == True and action.get('_type') == doc.get('_type') and action.get('_id') == doc.get('_id') and action.get('_version') <= doc.get('_version') ):
                        exclude = True
                        break
                if (exclude == False):
                    del action['_version']
                    cleanly.append(action)
            actions = cleanly
            #print(len(cleanly))
        return actions

    def _copy_data(self):
        ss_kw = {}
        # sort
        if self.source_sort:
            ss_kw['sort'] = self.source_sort
        scroll = self.source_es.search(index=self.source_index,scroll='1m',search_type='scan',size=self.bulk_size,version=True,timeout='60s',**ss_kw)
        sid = scroll['_scroll_id']
        total_size = scroll['hits']['total']
        hits_size = total_size
        dealt_size = 0
        print("docs: " + str(total_size))
        self.logger.info("docs: "+ str(total_size))
        suffix = '%(percent)d%% - %(index)d [%(elapsed_td)s / %(eta_td)s]'
        bar = ShadyBar("clone",suffix=suffix,max=total_size)
        while (hits_size > 0):
            scroll = self.source_es.scroll(scroll_id=sid,scroll='1m')
            sid = scroll['_scroll_id']
            hits = scroll['hits']['hits']
            hits_size = len(hits)
            actions = self._bulk_hits(hits)
            if (len(actions)>0):
                kw = {}
                kw['timeout'] = '60s'
                res = []
                try:
                    res = streaming_bulk(client=self.target_es,actions=actions,**kw)
                except BulkIndexError as err:
                    print(err)
                    pass
                okNum = 0 
                for ok,re in res:
                    if not ok:
                        print(re)
                    else:
                        okNum+=1
                # refresh index
                if (okNum>0):
                    self.target_es.indices.refresh(index=self.target_index)
            # dealt size
            dealt_size += hits_size
            bar.goto(dealt_size)
            self.logger.info("dealt: "+str(dealt_size) +" / "+ str(total_size))
        print('\nDone !')
        self.logger.info("Done ! \n\n")
        # clear scroll 
        # self.source_es.clear_scroll(scroll_id=sid,body=str(sid))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Clone elasticsearch index')
    parser.add_argument('-s', action="store", dest='source_index', help="Source index to copy from")
    parser.add_argument('-t', action="store", dest='target_index', help="Target index")
    parser.add_argument('-e', action="store", dest='source_es_server', default="127.0.0.1:9200", help="Elasticsearch source ip:port - default(127.0.0.1:9200)")
    parser.add_argument('-d', action="store", dest='target_es_server', default="", help="Elasticsearch target ip:port - default(-e)")
    parser.add_argument('-p', action="store", dest='primary_shards', default=3, help="primary shards in target index - default(3)")
    parser.add_argument('-r', action="store", dest='replica_shards', default=0, help="replica shards in target index - default(0)")
    parser.add_argument('-b', action="store", dest='bulk_size', default=100, help="bulk size - default(100)")
    parser.add_argument('-ss', action="store", dest='source_sort', default="", help="Search source index sort <field>:<direction>(asc|desc) - default()")

    arguments = parser.parse_args()
    try:
        IndexCloner(arguments.source_index, arguments.target_index, arguments.source_es_server,arguments.target_es_server, arguments.primary_shards,
                arguments.replica_shards,arguments.bulk_size,arguments.source_sort).clone()
    except Exception as err:
        print(err)
        pass
