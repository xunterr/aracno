

### aracno
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/xunterr/aracno) ![GitHub last commit](https://img.shields.io/github/last-commit/xunterr/aracno) ![GitHub License](https://img.shields.io/github/license/xunterr/aracno)


Aracno is a web-scale, distributed, polite web crawler. It utilizes a fully distributed peer-to-peer protocol to scale across millions of hosts and thousands of operations per second.

## Why?
There are many web crawlers, but most either don't support distributed mode or require some infrastructure to function (like Hadoop). Aracno is simple. There’s nothing extra you need to set up to enable distributed mode!

## Quickstart
1. Download binary from the [Releases tab](https://github.com/xunterr/aracno/releases) or [build](#build) it yourself
2. Run: `./aracno`

Crawled pages and relevant metadata are later saved in the data/warc folder as gzipped [warc](https://en.wikipedia.org/wiki/WARC_(file_format)) files.

## Build
To build aracno, you need to:
1. Install rocksdb v9.8.4 by following the [Rocksdb installation guide](https://github.com/facebook/rocksdb/blob/main/INSTALL.md) (just `make static_lib` and `sudo make install`)
2. Build: `go build`

## Monitoring with Prometheus
Aracno exposes a Prometheus scrape endpoint on port 8080. The provided metrics include the total number of crawled pages as well as the number of successfully crawled ones.

## Configuration
Place your configuration in the `config.yaml` file.
| Field | Description | Default value |
|--|--|--|
| scope | A regular expression pattern used to match crawled URLs. Only URLs that match this pattern will be processed and fetched. | (empty) 
| seed | File with seed URLs | (empty)
|	politeness.max_active_queues | Defines max number of queues (hosts) to process at a time | 256
| politeness.multiplier | A multiplier used to calculate the next crawl time based on the response time (_response time × multiplier_) | 10
| politeness.session_budget | The budget for a single queue, determining how long the queue will remain active | 20
| politeness.timeout | HTTP request timeout | 0
| distributed.addr | The address the node listens on. Distributed mode is disabled if left empty | (empty)
| distributed.bootstrap_node | The address of a node in the network to join. Leave empty if this node is the first | (empty)
| distributed.batch_period	| The interval (in milliseconds) for sending URL batches to another node | 40000
| distributed.checkkeys_period	| The interval (in milliseconds) for checking whether keys (hosts) still belong to this node | 30000
| distributed.dht.vnode_num |	The number of virtual nodes per physical node. A higher value improves key distribution but increases network usage | 16
| distributed.dht.succlist_length |	The number of successors a node should be aware of in the ring. This affects fault tolerance. It is recommended that this parameter be set to _log(n)_, where _n_ is the number of nodes in the network | 2
|	distributed.dht.stabilize_interval | The interval (in milliseconds) for checking predecessor and successor nodes | 15000
| distributed.dht.fixfingers_interval |	The interval (in milliseconds) for fixing fingers in the Chord ring. Faster fixes keep fingers up to date, reducing the number of hops per request | 15000



## Contribution
Contributions are highly appreciated. Feel free to open a new issue or submit a pull request.
