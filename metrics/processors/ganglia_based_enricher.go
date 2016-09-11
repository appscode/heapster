package processors

import (
	"fmt"

	"github.com/golang/glog"
	"k8s.io/heapster/metrics/core"
	"k8s.io/heapster/metrics/store"
	"k8s.io/heapster/metrics/util"
	kube_api "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/client/cache"
)

type GangliaBasedEnricher struct {
	podLister  *cache.StoreToPodLister
	nodeLister *cache.StoreToNodeLister
}

func (this *GangliaBasedEnricher) Name() string {
	return "ganglia_based_enricher"
}

func (this *GangliaBasedEnricher) Process(batch *core.DataBatch) (*core.DataBatch, error) {
	newMs := make(map[string]*core.MetricSet, len(batch.MetricSets))
	for k, v := range batch.MetricSets {
		switch v.Labels[core.LabelMetricSetType.Key] {
		case core.MetricSetTypeAppscodeGanglia:
			hostIp := v.Labels[core.LabelHostIp.Key]
			switch v.Labels[core.LabelGangliaSource.Key] {
			case core.MetricNodeSource:
				node, err := this.getNode(hostIp)
				if err != nil {
					glog.V(3).Infof("Failed to get node Info from hostIP %s", hostIp, err)
					continue
				}
				ms := addNodeInfoInGanglia(v, node)
				newMs[k] = ms
			case core.MetricPodSource:
				pod, err := this.getPod(hostIp)
				if err != nil {
					glog.V(3).Infof("Failed to get pod Info from hostIP %s", hostIp, err)
					continue
				}
				nodeName := pod.Spec.NodeName
				node, err := this.getNodeByname(nodeName)
				if err != nil {
					glog.V(3).Infof("Failed to get node info from nodeName %s", nodeName, err)
					continue
				}
				ms := addNodeInfoInGanglia(v, node)
				ms = addPodInfoInGanglia(ms, pod)
				newMs[k] = ms
			}
		}
	}

	for k, v := range newMs {
		batch.MetricSets[k] = v
	}

	return batch, nil
}
func (this *GangliaBasedEnricher) getPod(podIP string) (*kube_api.Pod, error) {
	o, exists, err := this.podLister.Indexer.(*store.BiCache).GetByIP(podIP)
	if err != nil {
		return nil, err
	}
	if !exists || o == nil {
		return nil, fmt.Errorf("cannot find pod definition")
	}
	pod, ok := o.(*kube_api.Pod)
	if !ok {
		return nil, fmt.Errorf("cache contains wrong type")
	}
	return pod, nil
}

func (this *GangliaBasedEnricher) getNode(nodeIP string) (*kube_api.Node, error) {
	o, exists, err := this.podLister.Indexer.(*store.BiCache).GetByIP(nodeIP)
	if err != nil {
		return nil, err
	}
	if !exists || o == nil {
		return nil, fmt.Errorf("cannot find node definition")
	}
	pod, ok := o.(*kube_api.Node)
	if !ok {
		return nil, fmt.Errorf("cache contains wrong type")
	}
	return pod, nil
}

func (this *GangliaBasedEnricher) getNodeByname(nodeName string) (*kube_api.Node, error) {
	o, exists, err := this.nodeLister.Store.(*store.BiCache).GetByKey(nodeName)
	if err != nil {
		return nil, err
	}
	if !exists || o == nil {
		return nil, fmt.Errorf("cannot find node definition")
	}
	node, ok := o.(*kube_api.Node)
	if !ok {
		return nil, fmt.Errorf("cache contains wrong type")
	}
	return node, nil
}

func addPodInfoInGanglia(podMs *core.MetricSet, pod *kube_api.Pod) *core.MetricSet {
	labels := map[string]string{
		core.LabelMetricSetType.Key: core.MetricSetTypePod,
		core.LabelNamespaceName.Key: pod.Namespace,
		core.LabelPodNamespace.Key:  pod.Namespace,
		core.LabelPodName.Key:       pod.Name,
		core.LabelPodId.Key:         string(pod.UID),
		core.LabelLabels.Key:        util.LabelsToString(pod.Labels, ","),
	}
	for key, val := range labels {
		podMs.Labels[key] = val
	}
	return podMs
}

func addNodeInfoInGanglia(nodeMs *core.MetricSet, node *kube_api.Node) *core.MetricSet {
	ms := &core.MetricSet{
		CreateTime:   nodeMs.CreateTime,
		ScrapeTime:   nodeMs.ScrapeTime,
		MetricValues: make(map[string]core.MetricValue),
		Labels: map[string]string{
			core.LabelMetricSetType.Key: core.MetricSetTypeNode,
			core.LabelHostID.Key:        node.Spec.ExternalID,
			core.LabelNodename.Key:      node.Name,
			core.LabelHostname.Key:      getNodeHostname(node),
		},
		LabeledMetrics: make([]core.LabeledMetric, 0),
	}
	ms.MetricValues = nodeMs.MetricValues
	ms.LabeledMetrics = nodeMs.LabeledMetrics
	return ms
}

func getNodeHostname(node *kube_api.Node) string {
	for _, addr := range node.Status.Addresses {
		if addr.Type == kube_api.NodeHostName && addr.Address != "" {
			return addr.Address
		}
	}
	return node.Name
}

func NewGangliaBasedEnricher(podLister *cache.StoreToPodLister, nodeLister *cache.StoreToNodeLister) (*GangliaBasedEnricher, error) {
	return &GangliaBasedEnricher{
		podLister:  podLister,
		nodeLister: nodeLister,
	}, nil
}
