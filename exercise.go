package main

import (
	"os"
	"sync"
	"sync/Mutex"
	"sync/atomic"
	"time"
	"crypto/md5"
	"log"
)

type Dirlock struct {
	dir	string
	f *os.File
}

type context struct {
	nsqd *NSQD
}

type Message struct {
	ID MessageID
	Body	[]byte
	TimeStamp int64
	Attempts	uint16

	deliveryTS	time.Time
	clientID 	int64
	pri	int64
	index	int
	deferred	time.Duration
}

type Quantile struct {
	sync.Mutex
	streams [2]quantile.Stream
	currentIndex	uint8
	lastMoveWindow	time.Time
	currentStream	*quantile.Stream

}

type BackendQueue interface {
	Put([]byte) error
	ReadChan() chan []byte
	Close() error
	Delete() error
	Depth() error
	Empty() error
}

type Consumer interface {
	Unpause()
	Pause()
	Close() error
	TimeOutMessage()
	Stats() ClientStats
	Empty()
}

type Channel struct {
	requeueCount unint64
	messageCount unint64
	timeoutCount unint64

	sync.RWMutex

	topicName string
	name string
	ctx	*context

	backend BackendQueue

	memoryMsgChan	chan *Message
	exitFlag	int32
	exitMutex	sync.RWMutex

	clients 	map[int64]Consumer
	paused	int32
	ephemeral	bool
	deleteCallback	func(*Channel)
	deleter	sync.Once

	e2eProcessingLatencyStream *quantile.Quantile

	deferredMessages map[MessageID]*pqueue.Item
	deferredPQ	pqueue.PriorityQueue
	deferredMutex	sync.Mutex
	inFlightMessages	map[MessageID]*Message
	inFlightPQ	inFlightPqueue
	inFlightMutex	sync.Mutex
}

type Topic struct {
	messageCount	unint64

	sync.RWMutex

	name	string
	channelMap	map[string]*Channel
	backend		BackendQueue
	memoryMsgChan	chan *Message
	startChan	chan int
	exitChan	chan int
	channelUpdateChan	chan int
	waitGroup	util.WaitGroupWrapper
	exitFlag	int32
	idFactory	*guidFactory

	ephemeral	bool
	deleteCallback	func(*Topic)
	deleter	sync.Once

	pause 	int32
	pauseChan	chan int
	ctx *context

}

type NSQD struct {
	clientIDSequence int64

	sync.RWMutex

	opts atomic.Value

	dl	*Dirlock
	isloading	int32
	errValue	atomic.Value
	startTime	time.Time

	topicMap map[string]*Topic

	clientLock sync.RWMutex
	clients 	mao[int64]Client

	lookupPeers	atomic.Value

	tcpListener	net.Listener
	httpListener	net.Listener
	httpsListener	net.Listener
	tlsConfig	*tls.Config

	poolSize	int

	notifyChan	chan interface{}
	optsNotificationChan	chan struct{}
	exitChan	chan int
	waitGroup 	util.WaitGroupWrapper

	ci *clusterinfo.ClusterInfo
}

type program struct {
	nsqd *nsqd.NSQD
}

func NewOptions() *Options {
	// 创建options结构，作为nsqd启动的参数来源
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	h := md5.New()
	io.WriteString(h, hostname)
	defauldID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	return &Options{
		ID:	defauldID
		LogPrefix:	"[nsqd]"
		LogLevel:	"info"

		TCPAddress:       "0.0.0.0:4150",
		HTTPAddress:      "0.0.0.0:4151",
		HTTPSAddress:     "0.0.0.0:4152",
		BroadcastAddress: hostname,

		MemQueueSize:	10000,
		MaxBytesPerFile:	100 * 1024 * 1024,
		SyncEvery:	2500,
		SyncTimeout: 2 * time.Second,

		QueueScanInterval:	100 * time.Millisecond,
		QueueScanRefreshInterval:	5 * time.Second,
		QueueScanSelectionCount:	20,
		QueueScanWorkerPoolMax: 4,
		QueueScanDirtyPercent:	0.25,
	}
}

func nsqdFlagSet(opts *nsqd.Options) *flag.FlagSet {
	flagSet := flag.NewFlagSet("nsqd", flag.ExitOnError)

	// basic options
	flagSet.Bool("version", false, "print version string")
	flagSet.String("config", "", "path to config file")

	flagSet.String("log-level", "info", "set log verbosity: debug, info, warn, error, or fatal")
	flagSet.String("log-prefix", "[nsqd] ", "log message prefix")
	flagSet.Bool("verbose", false, "deprecated in favor of log-level")

	flagSet.Int64("node-id", opts.ID, "unique part for message IDs, (int) in range [0,1024) (default is hash of hostname)")
	flagSet.Bool("worker-id", false, "do NOT use this, use --node-id")

	flagSet.String("https-address", opts.HTTPSAddress, "<addr>:<port> to listen on for HTTPS clients")
	flagSet.String("http-address", opts.HTTPAddress, "<addr>:<port> to listen on for HTTP clients")
	flagSet.String("tcp-address", opts.TCPAddress, "<addr>:<port> to listen on for TCP clients")
	authHTTPAddresses := app.StringArray{}
	flagSet.Var(&authHTTPAddresses, "auth-http-address", "<addr>:<port> to query auth server (may be given multiple times)")
	flagSet.String("broadcast-address", opts.BroadcastAddress, "address that will be registered with lookupd (defaults to the OS hostname)")
	lookupdTCPAddrs := app.StringArray{}
	flagSet.Var(&lookupdTCPAddrs, "lookupd-tcp-address", "lookupd TCP address (may be given multiple times)")
	flagSet.Duration("http-client-connect-timeout", opts.HTTPClientConnectTimeout, "timeout for HTTP connect")
	flagSet.Duration("http-client-request-timeout", opts.HTTPClientRequestTimeout, "timeout for HTTP request")

	// diskqueue options
	flagSet.String("data-path", opts.DataPath, "path to store disk-backed messages")
	flagSet.Int64("mem-queue-size", opts.MemQueueSize, "number of messages to keep in memory (per topic/channel)")
	flagSet.Int64("max-bytes-per-file", opts.MaxBytesPerFile, "number of bytes per diskqueue file before rolling")
	flagSet.Int64("sync-every", opts.SyncEvery, "number of messages per diskqueue fsync")
	flagSet.Duration("sync-timeout", opts.SyncTimeout, "duration of time per diskqueue fsync")

	// msg and command options
	flagSet.Duration("msg-timeout", opts.MsgTimeout, "default duration to wait before auto-requeing a message")
	flagSet.Duration("max-msg-timeout", opts.MaxMsgTimeout, "maximum duration before a message will timeout")
	flagSet.Int64("max-msg-size", opts.MaxMsgSize, "maximum size of a single message in bytes")
	flagSet.Duration("max-req-timeout", opts.MaxReqTimeout, "maximum requeuing timeout for a message")
	flagSet.Int64("max-body-size", opts.MaxBodySize, "maximum size of a single command body")

	// client overridable configuration options
	flagSet.Duration("max-heartbeat-interval", opts.MaxHeartbeatInterval, "maximum client configurable duration of time between client heartbeats")
	flagSet.Int64("max-rdy-count", opts.MaxRdyCount, "maximum RDY count for a client")
	flagSet.Int64("max-output-buffer-size", opts.MaxOutputBufferSize, "maximum client configurable size (in bytes) for a client output buffer")
	flagSet.Duration("max-output-buffer-timeout", opts.MaxOutputBufferTimeout, "maximum client configurable duration of time between flushing to a client")

	// statsd integration options
	flagSet.String("statsd-address", opts.StatsdAddress, "UDP <addr>:<port> of a statsd daemon for pushing stats")
	flagSet.Duration("statsd-interval", opts.StatsdInterval, "duration between pushing to statsd")
	flagSet.Bool("statsd-mem-stats", opts.StatsdMemStats, "toggle sending memory and GC stats to statsd")
	flagSet.String("statsd-prefix", opts.StatsdPrefix, "prefix used for keys sent to statsd (%s for host replacement)")
	flagSet.Int("statsd-udp-packet-size", opts.StatsdUDPPacketSize, "the size in bytes of statsd UDP packets")

	// End to end percentile flags
	e2eProcessingLatencyPercentiles := app.FloatArray{}
	flagSet.Var(&e2eProcessingLatencyPercentiles, "e2e-processing-latency-percentile", "message processing time percentiles (as float (0, 1.0]) to track (can be specified multiple times or comma separated '1.0,0.99,0.95', default none)")
	flagSet.Duration("e2e-processing-latency-window-time", opts.E2EProcessingLatencyWindowTime, "calculate end to end latency quantiles for this duration of time (ie: 60s would only show quantile calculations from the past 60 seconds)")

	// TLS config
	flagSet.String("tls-cert", opts.TLSCert, "path to certificate file")
	flagSet.String("tls-key", opts.TLSKey, "path to key file")
	flagSet.String("tls-client-auth-policy", opts.TLSClientAuthPolicy, "client certificate auth policy ('require' or 'require-verify')")
	flagSet.String("tls-root-ca-file", opts.TLSRootCAFile, "path to certificate authority file")
	tlsRequired := tlsRequiredOption(opts.TLSRequired)
	tlsMinVersion := tlsMinVersionOption(opts.TLSMinVersion)
	flagSet.Var(&tlsRequired, "tls-required", "require TLS for client connections (true, false, tcp-https)")
	flagSet.Var(&tlsMinVersion, "tls-min-version", "minimum SSL/TLS version acceptable ('ssl3.0', 'tls1.0', 'tls1.1', or 'tls1.2')")

	// compression
	flagSet.Bool("deflate", opts.DeflateEnabled, "enable deflate feature negotiation (client compression)")
	flagSet.Int("max-deflate-level", opts.MaxDeflateLevel, "max deflate compression level a client can negotiate (> values == > nsqd CPU usage)")
	flagSet.Bool("snappy", opts.SnappyEnabled, "enable snappy feature negotiation (client compression)")

	return flagSet
}

func (p *program) Start() error {
	// 获取
	opts := nsqd.NewOptions(opts)

	flagSet := nsqdFlagSet(opts)
	flagSet.Parse(os.Args[1:])

	rand.Seed(time.Now().UTC().UnixNano())

	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) {
		fmt.Println(version.String("nsqd"))
		os.Exit(0)
	}

	var cfg config
	configFile := flagSet.Lookup("config").Value.String()
	if configFile != "" {
		_, err := toml.DecodeFi;e(configFile, &cfg)
		if err != nil {
			log.Fatalf("ERROR: failed to load config file %s - %s", configFile, err.Error())
		}
	}
	cfg.Validate()

	options.Resolve(opts, flagSet, cfg)
	nsqd := nsqd.New(opts)

	err := nsqd.LoadMetadata()
	if err != nil {
		log.Fatalf("ERROR: %s", err.Error())
	}

	err = nsqd.PersistMetadata()
	if err != nil {
		log.Fatalf("ERROR: fail to persist metadata - %s", err.Error())
	}
	nsqd.Main()

	p.nsqd = nsqd
	return nil

}