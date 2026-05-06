package main

import (
	"flag"
	_ "net/http/pprof"
	"os"

	"big-market-kratos/internal/conf"
	"big-market-kratos/internal/dcc"
	"big-market-kratos/pkg/logger"

	"big-market-kratos/internal/server"

	"github.com/go-kratos/kratos/contrib/config/etcd/v2"
	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/tracing"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	"github.com/go-kratos/kratos/v2/transport/http"
	clientv3 "go.etcd.io/etcd/client/v3"

	_ "go.uber.org/automaxprocs"
)

// go build -ldflags "-X main.Version=x.y.z"
var (
	// Name is the name of the compiled software.
	Name string
	// Version is the version of the compiled software.
	Version string
	// flagconf is the config flag.
	flagconf string

	id, _ = os.Hostname()
)

func init() {
	flag.StringVar(&flagconf, "conf", "../../configs", "config path, eg: -conf config.yaml")
}

func newApp(
	logger log.Logger,
	gs *grpc.Server,
	hs *http.Server,
	asynqSrv *server.AsynqServer,
	rmqSrv *server.RabbitMQServer,
) *kratos.App {
	return kratos.New(
		kratos.ID(id),
		kratos.Name(Name),
		kratos.Version(Version),
		kratos.Metadata(map[string]string{}),
		kratos.Logger(logger),
		kratos.Server(
			gs,
			hs,
			asynqSrv,
			rmqSrv,
		),
	)
}

func main() {
	flag.Parse()
	// go func() {
	// 	httpprof.ListenAndServe("0.0.0.0:6060", nil)
	// }()

	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"127.0.0.1:2379"},
	})
	if err != nil {
		panic(err)
	}

	source, err := etcd.New(client, etcd.WithPath("configs/config.yaml"))
	if err != nil {
		panic(err)
	}

	c := config.New(
		config.WithSource(
			file.NewSource(flagconf),
			source,
		),
	)
	defer c.Close()

	if err := c.Load(); err != nil {
		panic(err)
	}

	var bc conf.Bootstrap

	if err := c.Scan(&bc); err != nil {
		panic(err)
	}

	logger.Init(logger.Config{
		Filename:   bc.Log.Filename,
		MaxSize:    int(bc.Log.MaxSize),
		MaxBackups: int(bc.Log.MaxBackups),
		MaxAge:     int(bc.Log.MaxAge),
		Level:      bc.Log.Level,
		Compress:   bc.Log.Compress,
	})

	logInstance := log.With(logger.NewKratosLogger(logger.Log),
		"ts", log.DefaultTimestamp,
		"caller", log.DefaultCaller,
		"service.id", id,
		"service.name", Name,
		"service.version", Version,
		"trace.id", tracing.TraceID(),
		"span.id", tracing.SpanID(),
	)
	log.SetLogger(logInstance)

	fallback := dcc.NewFallback("./data/fallback_snapshot.json", logInstance)
	dccManager := dcc.NewManager(c, "dcc", logInstance, fallback)

	if err := dccManager.Init(); err != nil {
		panic("DCC 初始化失败，应用无法启动: " + err.Error())
	}

	app, cleanup, err := wireApp(bc.Server, bc.Data, bc.Rabbitmq, bc.Asynq, logInstance, dccManager)
	if err != nil {
		panic(err)
	}
	defer cleanup()

	// start and wait for stop signal
	if err := app.Run(); err != nil {
		panic(err)
	}
}
