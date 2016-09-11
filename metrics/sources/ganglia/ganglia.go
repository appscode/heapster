package ganglia

import (
	"fmt"
	"strconv"
	"time"

	"github.com/golang/glog"
	. "k8s.io/heapster/metrics/core"
	"k8s.io/heapster/metrics/store"
	"k8s.io/heapster/third_party/ganglia/gmon"
	"k8s.io/kubernetes/pkg/client/cache"
)

type Host struct {
	IP       string
	Port     int
	Protocol string
}

type gangliaMetricsSource struct {
	host Host
}

type gangliaProvider struct {
}

var PodLister *cache.StoreToPodLister
var NodeLister *cache.StoreToNodeLister

func newGangliaMetricsSource(host Host) (MetricsSource, error) {
	return &gangliaMetricsSource{
		host: host,
	}, nil
}

func (this *gangliaMetricsSource) Name() string {
	return this.String()
}

func (this *gangliaMetricsSource) String() string {
	return fmt.Sprintf("ganglia:%s:%d", this.host.IP, this.host.Port)
}

func (this *gangliaMetricsSource) ScrapeMetrics(start, end time.Time) *DataBatch {
	result := &DataBatch{
		Timestamp:  end,
		MetricSets: map[string]*MetricSet{},
	}

	gangliaList := PodLister.Indexer.(*store.BiCache).GetGangliaPodIP()
	for _, gangliaIP := range gangliaList {
		gmond, err := gmon.RemoteRead(this.host.Protocol, fmt.Sprintf("%s:%d", gangliaIP, this.host.Port))
		if err != nil {
			glog.V(2).Infof("failed to get gmond data from user-ganglie: ", gangliaIP)
			continue
		}

		for _, cluster := range gmond.Clusters {
			for _, host := range cluster.Hosts {
				hostIp := host.IP
				if hostIp == "127.0.0.1" {
					hostIp = gangliaIP
				}
				name, metrics := this.decodeMetrics(gangliaIP, hostIp, host.Reported, cluster.Localtime, host.Metrics)
				if name == "" || metrics == nil {
					continue
				}
				result.MetricSets[name] = metrics
			}
		}
	}
	return result
}

func IpInNodeList(ip string) bool {
	_, exists, err := NodeLister.Store.(*store.BiCache).GetByIP(ip)
	if err != nil {
		return false
	}
	if exists {
		return true
	}
	return false
}

func IpInPodList(ip string) bool {
	_, exists, err := PodLister.Indexer.(*store.BiCache).GetByIP(ip)
	if err != nil {
		return false
	}
	if exists {
		return true
	}
	return false
}

func (this *gangliaMetricsSource) decodeMetrics(gangliaIp, hostIp string, createTime, scrapeTime int, metrics []gmon.Metric) (string, *MetricSet) {
	cMetrics := &MetricSet{
		CreateTime:   time.Unix(int64(createTime), 0).UTC(),
		ScrapeTime:   time.Unix(int64(scrapeTime), 0).UTC(),
		MetricValues: map[string]MetricValue{},
		Labels: map[string]string{
			LabelHostIp.Key:        hostIp,
			LabelMetricSetType.Key: MetricSetTypeAppscodeGanglia,
		},
		LabeledMetrics: []LabeledMetric{},
	}

	if found := IpInNodeList(hostIp); found {
		cMetrics.Labels[LabelGangliaSource.Key] = MetricNodeSource
		for _, metric := range metrics {
			mv := MetricValue{
				MetricType: MetricGauge,
			}
			switch metric.Type {
			case "uint32", "uint16":
				mv.ValueType = ValueInt64
				i, err := strconv.ParseInt(metric.Value, 10, 64)
				if err != nil {
					continue
				}
				mv.IntValue = i
			case "float", "double":
				mv.ValueType = ValueFloat
				f, err := strconv.ParseFloat(metric.Value, 32)
				if err != nil {
					continue
				}
				mv.FloatValue = float32(f)
			case "string":
				mv.ValueType = ValueString
				mv.StringValue = metric.Value
			default:
				glog.V(3).Info("Ignore Type: ", metric.Type)
			}
			cMetrics.MetricValues["test-node/"+metric.Name] = mv
		}
	} else if found := IpInPodList(hostIp); found {
		cMetrics.Labels[LabelGangliaSource.Key] = MetricPodSource
		for _, metric := range metrics {
			metricName := metric.Name
			labelList := make(map[string]string)
			for _, val := range metric.ExtraData.ExtraElements {
				if val.Name == "GROUP" {
					metricName = val.Val + "/" + metricName
				} else if val.Name == "DESC" || val.Name == "TITLE" {
					continue
				} else {
					labelList[val.Name] = val.Val
				}
			}
			labelMetric := LabeledMetric{
				Name:   metricName,
				Labels: labelList,
			}

			labelMetric.MetricType = MetricGauge
			switch metric.Type {
			case "uint32", "uint16":
				labelMetric.ValueType = ValueInt64
				i, err := strconv.ParseInt(metric.Value, 10, 64)
				if err != nil {
					continue
				}
				labelMetric.IntValue = i
			case "float", "double":
				labelMetric.ValueType = ValueFloat
				f, err := strconv.ParseFloat(metric.Value, 32)
				if err != nil {
					continue
				}
				labelMetric.FloatValue = float32(f)
			case "string":
				labelMetric.ValueType = ValueString
				labelMetric.StringValue = metric.Value
			default:
				glog.V(3).Info("Ignore Type: ", metric.Type)
			}

			cMetrics.LabeledMetrics = append(cMetrics.LabeledMetrics, labelMetric)
		}
	}
	key := AppscodeGangliaKey(gangliaIp, hostIp)
	return key, cMetrics
}

func NewGangliaProvider() MetricsSourceProvider {
	return &gangliaProvider{}
}

func (this *gangliaProvider) GetMetricsSources() []MetricsSource {
	sources := []MetricsSource{}
	acGangliaHost := Host{
		Port:     8649,
		Protocol: "tcp",
	}
	acGanglia, err := newGangliaMetricsSource(acGangliaHost)
	if err != nil {
		return nil
	}
	sources = append(sources, acGanglia)
	return sources
}
