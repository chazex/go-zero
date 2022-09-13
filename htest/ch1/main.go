package main

import (
	"context"
	"flag"
	"github.com/zeromicro/go-queue/kq"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
)

var qlogConfig = `
remote.log-name=android.build.apk.kq
remote.host=file01.white.bjyt.qihoo.net:8888
remote.log-queue-size=2048000
remote.block-queue=false
remote.thread-num=1
remote.compress=true
remote.log-level=ALL
remote.stat-log-delay=5
remote.recv-lowwater=0
remote.qlog-debug=false
`

var configFile = flag.String("f", "etc/order.yaml", "Specify the config file")

func main() {
	flag.Parse()

	logx.Infof("start...")

	var c config.Config
	conf.MustLoad(*configFile, &c)
	//conf.MustLoad("E:\\codespace\\develop\\godevelop\\u\\update.android.qihoo.net\\app\\kq\\etc\\update.yaml", &c)

	// log、prometheus、trace、metricsUrl.
	if err := c.SetUp(); err != nil {
		panic(err)
	}

	svcContext := svc.NewServiceContext(c)

	serviceGroup := service.NewServiceGroup()
	defer serviceGroup.Stop()

	serviceGroup.Add(kq.MustNewQueue(c.ApkUpdateConf, logic.NewApkUpdateMq(context.Background(), svcContext, c)))

	writer, err := zlog.NewKLogWriter([]byte(qlogConfig))
	logx.Must(err)
	logx.SetWriter(writer)
	logx.DisableStat()

	serviceGroup.Start()
}
