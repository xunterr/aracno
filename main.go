package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/linxGnu/grocksdb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	boom "github.com/tylertreat/BoomFilters"
	"github.com/xunterr/aracno/internal/dht"
	"github.com/xunterr/aracno/internal/fetcher"
	"github.com/xunterr/aracno/internal/frontier"
	p2p "github.com/xunterr/aracno/internal/net"
	"github.com/xunterr/aracno/internal/storage"
	"github.com/xunterr/aracno/internal/storage/inmem"
	"github.com/xunterr/aracno/internal/storage/rocksdb"
	"github.com/xunterr/aracno/internal/warc"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	total = promauto.NewCounter(prometheus.CounterOpts{
		Name: "crawler_processed_total",
		Help: "The total number of processed pages.",
	})

	totalGood = promauto.NewCounter(prometheus.CounterOpts{
		Name: "crawler_processed_total_good",
		Help: "The total number of 200 OK processed pages.",
	})
)

type persistentQp struct {
	db              *grocksdb.DB
	queueStorage    *rocksdb.RocksdbStorage[frontier.Url]
	metadataStorage *rocksdb.RocksdbStorage[string]
}

func newPersistentQp(path string) (*persistentQp, error) {
	db, cfs, err := createDefaultDBWithCF(path, []string{"metadata", "data"})
	if err != nil {
		return nil, err
	}

	metadataCF := cfs[0]
	dataCF := cfs[1]

	metadataStorage := rocksdb.NewRocksdbStorage[string](db, rocksdb.WithCF(metadataCF))
	queueStorage := rocksdb.NewRocksdbStorage[frontier.Url](db, rocksdb.WithCF(dataCF))
	return &persistentQp{
		db:              db,
		queueStorage:    queueStorage,
		metadataStorage: metadataStorage,
	}, nil
}

func createDefaultDBWithCF(path string, cfs []string) (*grocksdb.DB, grocksdb.ColumnFamilyHandles, error) {
	cfs = append(cfs, "default")
	var opts []*grocksdb.Options
	for _ = range cfs {
		opts = append(opts, grocksdb.NewDefaultOptions())
	}
	return grocksdb.OpenDbColumnFamilies(getDbOpts(), path, cfs, opts)
}

func (qp *persistentQp) Get(id string) (storage.Queue[frontier.Url], error) {
	err := qp.metadataStorage.Put(id, "")
	if err != nil {
		return nil, err
	}
	return rocksdb.NewRocksdbQueue(qp.queueStorage, []byte(id)), nil
}

func (qp *persistentQp) GetAll() (map[string]storage.Queue[frontier.Url], error) {
	queueMap := make(map[string]storage.Queue[frontier.Url])
	metadata, err := qp.metadataStorage.GetAll()
	if err != nil {
		return nil, err
	}

	for k, _ := range metadata {
		queue := rocksdb.NewRocksdbQueue(qp.queueStorage, []byte(k))
		queueMap[k] = queue
	}
	return queueMap, nil
}

func initLogger(level zapcore.Level) *zap.Logger {
	conf := zap.NewProductionEncoderConfig()
	conf.EncodeTime = zapcore.ISO8601TimeEncoder
	encoder := zapcore.NewConsoleEncoder(conf)
	core := zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), level)
	l := zap.New(core)
	return l
}

func readSeed(path string) ([]*url.URL, error) {
	dat, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(dat)
	urls := []*url.URL{}

	for scanner.Scan() {
		url, err := url.Parse(scanner.Text())
		if err != nil {
			return urls, err
		}

		urls = append(urls, url)
	}
	return urls, nil
}

func main() {
	var wg sync.WaitGroup
	wg.Add(1)

	defaultLogger := initLogger(zapcore.InfoLevel)
	defer defaultLogger.Sync()
	logger := defaultLogger.Sugar()

	conf, err := ReadConf()
	if err != nil {
		logger.Fatalln(err)
	}

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		logger.Fatalln(http.ListenAndServe(":8080", nil))
	}()

	var frontier frontier.Frontier
	if conf.Distributed.Addr != "" {
		frontier = makeDistributedFrontier(logger, makeFrontier(conf.Frontier), conf.Distributed)
	} else {
		frontier = makeFrontier(conf.Frontier)
	}

	urls, err := readSeed(conf.Frontier.Seed)
	if err != nil {
		logger.Errorf("Can't parse URL: %s", err.Error())
	}

	for _, u := range urls {
		err = frontier.Put(u)
		if err != nil {
			logger.Errorln(err)
		}
	}

	fetcher := fetcher.NewDefaultFetcher(time.Duration(conf.Fetcher.TimeoutMs) * time.Millisecond)
	loop(logger, frontier, fetcher)

	wg.Wait()
}

func makeDistributedFrontier(logger *zap.SugaredLogger, bfFrontier *frontier.BfFrontier, conf DistributedConf) frontier.Frontier {
	peer := p2p.NewPeer(logger.Desugar(), conf.Addr)

	go peer.Listen(context.Background())

	dht, err := makeDHT(logger.Desugar(), peer, conf.Dht)
	if err != nil {
		logger.Fatalln(err)
		return nil
	}

	var opts []frontier.DistributedOption
	if conf.CheckKeysPeriodMs > 0 {
		opts = append(opts, frontier.WithCheckKeysPeriod(conf.CheckKeysPeriodMs))
	}
	if conf.BatchPeriodMs > 0 {
		opts = append(opts, frontier.WithBatchPeriod(conf.BatchPeriodMs))
	}

	distributedFrontier, err := frontier.NewDistributed(logger.Desugar(), peer, bfFrontier, dht, opts...)
	if err != nil {
		logger.Fatalln("Failed to init dispatcher: %s", err.Error())
		return nil
	}

	bootstrap(logger, conf.Bootstrap, distributedFrontier)

	return distributedFrontier
}

func makeDHT(logger *zap.Logger, peer *p2p.Peer, conf DhtConf) (*dht.DHT, error) {
	options := []struct {
		condition bool
		option    dht.DhtOption
	}{
		{conf.FixFingersInterval > 0, dht.WithFixFingersIntervaal(conf.FixFingersInterval)},
		{conf.StabilizeInterval > 0, dht.WithStabilizeInterval(conf.StabilizeInterval)},
		{conf.SuccListLength > 0, dht.WithSuccListLength(conf.SuccListLength)},
		{conf.VnodeNum > 0, dht.WithVnodeNum(conf.VnodeNum)},
	}

	var opts []dht.DhtOption
	for _, opt := range options {
		if opt.condition {
			opts = append(opts, opt.option)
		}
	}

	table, err := dht.NewDHT(logger, peer, opts...)
	if err != nil {
		return nil, err
	}

	return table, nil
}

func bootstrap(logger *zap.SugaredLogger, addr string, distributedFrontier *frontier.DistributedFrontier) {
	fmt.Printf("Node to bootstrap from: %s\n", addr)
	if len(addr) > 0 {
		err := distributedFrontier.Bootstrap(addr)
		if err != nil {
			logger.Infof("Failed to bootstrap from node %s: %s", addr, err.Error())
		}
	}
}

func makeFrontier(conf FrontierConf) *frontier.BfFrontier {
	qp, err := newPersistentQp("data/queues/")
	if err != nil {
		panic(err.Error())
	}

	bloomDb, err := grocksdb.OpenDb(getDbOpts(), "data/bloom/")
	if err != nil {
		panic(err.Error())
	}

	persistentStorage := rocksdb.NewRocksdbStorageWithEncoderDecoder[*boom.ScalableBloomFilter](bloomDb, encode, decode)
	storage := inmem.NewSlidingStorage(persistentStorage, 1024)
	//storage := inmem.NewInMemoryStorage[*boom.ScalableBloomFilter]()
	queues, err := qp.GetAll()
	if err != nil {
		panic(err)
	}

	opts := []frontier.BfFrontierOption{}
	if conf.DefaultSessionBudget > 0 {
		opts = append(opts, frontier.WithSessionBudget(conf.DefaultSessionBudget))
	}
	if conf.Politeness > 0 {
		opts = append(opts, frontier.WithPolitenessMultiplier(conf.Politeness))
	}
	if conf.MaxActiveQueues > 0 {
		opts = append(opts, frontier.WithMaxActiveQueues(conf.MaxActiveQueues))
	}

	frontier := frontier.NewBfFrontier(qp, storage, opts...)
	frontier.LoadQueues(queues)
	return frontier
}

func encode(bloom *boom.ScalableBloomFilter) ([]byte, error) {
	var bytes bytes.Buffer
	writer := io.Writer(&bytes)
	bloom.WriteTo(writer)
	return bytes.Bytes(), nil
}

func decode(data []byte) (*boom.ScalableBloomFilter, error) {
	bloom := boom.NewDefaultScalableBloomFilter(0.01)
	buf := bytes.NewReader(data)
	_, err := bloom.ReadFrom(buf)

	if err != nil {
		return nil, err
	}
	return bloom, err
}

func getDbOpts() *grocksdb.Options {
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(grocksdb.NewLRUCache(5 << 30))
	opts := grocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	return opts
}

func openRocksDB(path string) (*grocksdb.DB, error) {
	return grocksdb.OpenDb(getDbOpts(), path)
}

func loop(logger *zap.SugaredLogger, frontier frontier.Frontier, fet fetcher.Fetcher) {
	var wg sync.WaitGroup

	urls := make(chan resource, 128)
	processed := make(chan result, 16)

	go func() {
		for r := range processed {
			total.Inc()
			if r.err != nil {
				if r.err != ErrCrawlForbidden {
					logger.Errorf("Error processing url: %s - %s", r.url, r.err)
				}
				if _, isReqErr := r.err.(*RequestError); isReqErr {
					frontier.MarkFailed(r.url)
				} else {
					frontier.MarkProcessed(r.url)
				}
				continue
			}

			totalGood.Inc()
			for _, u := range r.links {
				err := frontier.Put(u)
				if err != nil {
					logger.Errorln(err.Error())
				}
			}
			frontier.MarkSuccessful(r.url, r.ttr)
		}
	}()

	warcWriter := warc.NewWarcWriter("data/warc/")

	worker := &Worker{
		fetcher:     fet,
		in:          urls,
		out:         processed,
		warcWriter:  warcWriter,
		maxPageSize: 100 * 1024 * 1024,
	}
	worker.runN(context.Background(), &wg, 512)

	for {
		url, accessAt, err := frontier.Get()
		if err != nil {
			continue
		}

		urls <- resource{
			u:  url,
			at: accessAt,
		}
	}
}
