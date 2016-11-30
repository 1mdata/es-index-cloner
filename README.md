es-index-cloner
===============

Clone one elastic search index to another

- Clones documents along with mappings.
- Configure shard and replica count for the newly created index

Steps to execute:
----------------

1. Clone repository
2. run "pip install -r requirements.txt" to install dependencies
3. run "python index_cloner -h" for help


Sample
----------------

```
python index_cloner.py -e 192.168.9.1:9200,192.168.9.2:9200,192.168.9.3:9200 -s v1_2016118 -t v1_20161130 -b 1000
```
