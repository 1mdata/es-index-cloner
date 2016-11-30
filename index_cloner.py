import argparse
from progress.bar import ShadyBar
from elasticsearch import Elasticsearch,connection as es_connection
from elasticsearch.helpers import streaming_bulk


class IndexCloner(object):
    def __init__(self, source_index, target_index, es_ip_port, shard_count, replica_count,bulk_size):
        self.source_index = source_index
        self.target_index = target_index
        self.shard_count = shard_count
        self.replica_count = replica_count
        self.bulk_size = bulk_size
        self.es = Elasticsearch([es_ip_port],
                connection_class=es_connection.RequestsHttpConnection,
                sniff_on_start=True,
                sniff_on_connection_fail=True,
                sniffer_timeout=60)
        
        # print(self.es.info())

    def clone(self):
        self._copy_mappings()
        self._copy_data()

    def _copy_mappings(self):
        source_mappings = self._get_mappings()

        index_settings_mappings = {
                 "settings": {"index": {"number_of_shards": self.shard_count, "number_of_replicas": self.replica_count}}
                ,"mappings": source_mappings['mappings']}
        # Create index with settings and mappings
        self.es.indices.create(index=self.target_index,body=index_settings_mappings)
        

    def _get_mappings(self):
        r = self.es.indices.get_mapping(index=self.source_index) 
        source_mappings = r
        return source_mappings[self.source_index]
 
    def _bulk_hits(self,hits):
        hit = None
        for hit in hits:
            doc_id = hit['_id']
            doc_type = hit['_type']
            doc_parent = hit.get('_parent')
            doc_routing = hit.get('_routing')
            
            document_action = {
                "_index": self.target_index,
                "_type": doc_type,
                "_id": doc_id,
                "_source": hit['_source']
            }
            
            if doc_parent:
                document_action["_parent"] = doc_parent
            
            if doc_routing:
                document_action["_routing"] = doc_routing
            
            yield document_action


    def _copy_data(self):
        mappings_types = self._get_mappings()['mappings'].keys()
        sids = []
        #for doc_type in mappings_types:
        scroll = self.es.search(index=self.source_index,scroll='1m',search_type='scan',size=self.bulk_size,timeout='60s')
        sid = scroll['_scroll_id']
        sids += sid
        total_size = scroll['hits']['total']
        hits_size = total_size
        dealt_size = 0
        print("docs: " + str(total_size))
        suffix = '%(percent)d%% [%(elapsed_td)s / %(eta_td)s]'
        bar = ShadyBar("clone",suffix=suffix,max=total_size)
        while (hits_size > 0):
            scroll = self.es.scroll(scroll_id=sid,scroll='1m')
            sid = scroll['_scroll_id']
            hits = scroll['hits']['hits']
            hits_size = len(hits)
            actions = self._bulk_hits(hits)
            kw = {}
            kw['timeout'] = '60s'
            res = streaming_bulk(client=self.es,actions=actions,**kw)
            for ok,re in res:
                if not ok:
                    print(re)

            # refresh index
            self.es.indices.refresh(index=self.target_index)
            # dealt size
            dealt_size += hits_size
            #print("dealt size: " + str(dealt_size))
            bar.goto(dealt_size)

        # clear scroll 
        print(sids)
        self.es.clear_scroll(scroll_id=sids)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Clone elasticsearch index')
    parser.add_argument('-s', action="store", dest='source_index', help="Source index to copy from")
    parser.add_argument('-t', action="store", dest='target_index', help="Target index")
    parser.add_argument('-e', action="store", dest='es_server', default="127.0.0.1:9200", help="Elasticsearch ip:port - default(127.0.0.1:9200)")
    parser.add_argument('-p', action="store", dest='primary_shards', default=3, help="primary shards in target index - default(3)")
    parser.add_argument('-r', action="store", dest='replica_shards', default=0, help="replica shards in target index - default(0)")
    parser.add_argument('-b', action="store", dest='bulk_size', default=100, help="bulk size - default(100)")

    arguments = parser.parse_args()

    IndexCloner(arguments.source_index, arguments.target_index, arguments.es_server, arguments.primary_shards,
                arguments.replica_shards,arguments.bulk_size).clone()
