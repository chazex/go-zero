package prometheus

// 专门为Prometheus指标的收集，开启一个HTTP服务。
// Host， PORT： HTTP服务 地址
// PATH： 指标收集的http路由

// A Config is a prometheus config.
type Config struct {
	Host string `json:",optional"`
	Port int    `json:",default=9101"`
	Path string `json:",default=/metrics"`
}
