// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package processors

import (
	"github.com/stretchr/testify/assert"
	"k8s.io/heapster/metrics/core"
	"k8s.io/heapster/metrics/store"
	kube_api "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/client/cache"
	"testing"
	"time"
)

var gangliaBatch = &core.DataBatch{
	Timestamp: time.Now(),
	MetricSets: map[string]*core.MetricSet{
		core.AppscodeGangliaKey("10.24.31.2", "127.0.0.1"): {
			Labels: map[string]string{
				core.LabelMetricSetType.Key: core.MetricSetTypeAppscodeGanglia,
				core.LabelGangliaSource.Key: core.MetricPodSource,
				core.LabelHostIp.Key:        "10.24.31.2",
			},
			MetricValues: map[string]core.MetricValue{},
			LabeledMetrics: []core.LabeledMetric{
				{
					Name: "metric1",
					Labels: map[string]string{
						"key11": "val11",
						"key12": "val12",
					},
					MetricValue: core.MetricValue{
						MetricType: core.MetricGauge,
						ValueType:  core.ValueInt64,
						IntValue:   10,
					},
				},
				{
					Name: "metric2",
					Labels: map[string]string{
						"key21": "val21",
						"key22": "val22",
					},
					MetricValue: core.MetricValue{
						MetricType: core.MetricGauge,
						ValueType:  core.ValueFloat,
						FloatValue: 20.5,
					},
				},
			},
		},
		core.AppscodeGangliaKey("10.26.3.12", "127.0.0.1"): {
			Labels: map[string]string{
				core.LabelMetricSetType.Key: core.MetricSetTypeAppscodeGanglia,
				core.LabelGangliaSource.Key: core.MetricNodeSource,
				core.LabelHostIp.Key:        "10.26.3.12",
			},
			MetricValues: map[string]core.MetricValue{
				"metric1": core.MetricValue{
					IntValue: 10,
				},
				"metric2": core.MetricValue{
					FloatValue: 20.5,
				},
			},
		},
	},
}

func TestGangliaEnricher(t *testing.T) {
	pod := kube_api.Pod{
		ObjectMeta: kube_api.ObjectMeta{
			Name:      "pod1",
			Namespace: "ns1",
			UID:       "10.10.10.10",
		},
		Status: kube_api.PodStatus{
			PodIP: "10.24.31.2",
		},
		Spec: kube_api.PodSpec{
			NodeName: "node1",
		},
	}

	node := kube_api.Node{
		ObjectMeta: kube_api.ObjectMeta{
			Name: "node1",
		},
		Status: kube_api.NodeStatus{
			Addresses: []kube_api.NodeAddress{
				{
					Type:    kube_api.NodeExternalIP,
					Address: "10.26.3.12",
				},
				{
					Type:    kube_api.NodeHostName,
					Address: "cluster-node",
				},
			},
		},
		Spec: kube_api.NodeSpec{
			ExternalID: "10.25.12.2",
		},
	}

	podLister := &cache.StoreToPodLister{Indexer: store.NewBiCache(cache.MetaNamespaceKeyFunc, store.PodIPFunc, store.GangliaPodIPFunc)}
	podLister.Add(&pod)

	nodeLister := &cache.StoreToNodeLister{Store: store.NewBiCache(cache.MetaNamespaceKeyFunc, store.NodeIPFunc, store.GangliaPodIPFunc)}
	nodeLister.Add(&node)

	gangliaBasedEnricher, err := NewGangliaBasedEnricher(podLister, nodeLister)

	batch, err := gangliaBasedEnricher.Process(gangliaBatch)
	assert.NoError(t, err)

	// Test for pod ganglia
	gangliaMS, found := batch.MetricSets[core.AppscodeGangliaKey("10.24.31.2", "127.0.0.1")]
	assert.True(t, found)

	assert.EqualValues(t, core.MetricSetTypePod, gangliaMS.Labels[core.LabelMetricSetType.Key])
	assert.EqualValues(t, "10.25.12.2", gangliaMS.Labels[core.LabelHostID.Key])
	assert.EqualValues(t, "node1", gangliaMS.Labels[core.LabelNodename.Key])
	assert.EqualValues(t, "cluster-node", gangliaMS.Labels[core.LabelHostname.Key])

	assert.EqualValues(t, "ns1", gangliaMS.Labels[core.LabelNamespaceName.Key])
	assert.EqualValues(t, "ns1", gangliaMS.Labels[core.LabelPodNamespace.Key])
	assert.EqualValues(t, "pod1", gangliaMS.Labels[core.LabelPodName.Key])
	assert.EqualValues(t, "10.10.10.10", gangliaMS.Labels[core.LabelPodId.Key])

	found, metric := gangliaLabeledMetrics(gangliaMS, "metric1")
	assert.True(t, found)

	assert.EqualValues(t, "val11", metric.Labels["key11"])
	assert.EqualValues(t, "val12", metric.Labels["key12"])
	assert.EqualValues(t, 10, metric.IntValue)

	found, metric = gangliaLabeledMetrics(gangliaMS, "metric2")
	assert.True(t, found)

	assert.EqualValues(t, "val21", metric.Labels["key21"])
	assert.EqualValues(t, "val22", metric.Labels["key22"])
	assert.EqualValues(t, 20.5, metric.FloatValue)

	found, metric = gangliaLabeledMetrics(gangliaMS, "metric3")
	assert.False(t, found)

	// Test for unknown Metric
	gangliaMS, found = batch.MetricSets[core.AppscodeGangliaKey("10.24.31.3", "127.0.0.1")]
	assert.False(t, found)

	// Test for node ganglia
	gangliaMS, found = batch.MetricSets[core.AppscodeGangliaKey("10.26.3.12", "127.0.0.1")]
	assert.True(t, found)

	assert.EqualValues(t, core.MetricSetTypeNode, gangliaMS.Labels[core.LabelMetricSetType.Key])
	assert.EqualValues(t, "10.25.12.2", gangliaMS.Labels[core.LabelHostID.Key])
	assert.EqualValues(t, "node1", gangliaMS.Labels[core.LabelNodename.Key])
	assert.EqualValues(t, "cluster-node", gangliaMS.Labels[core.LabelHostname.Key])

	found, metricValue := gangliaMetricValues(gangliaMS, "metric1")
	assert.True(t, found)
	assert.EqualValues(t, 10, metricValue.IntValue)

	found, metricValue = gangliaMetricValues(gangliaMS, "metric2")
	assert.True(t, found)
	assert.EqualValues(t, 20.5, metricValue.FloatValue)

	found, metricValue = gangliaMetricValues(gangliaMS, "metric3")
	assert.False(t, found)
}

func gangliaLabeledMetrics(ms *core.MetricSet, name string) (bool, *core.LabeledMetric) {
	for _, metric := range ms.LabeledMetrics {
		if metric.Name == name {
			return true, &metric
		}
	}
	return false, nil
}

func gangliaMetricValues(ms *core.MetricSet, name string) (bool, *core.MetricValue) {
	if val, found := ms.MetricValues[name]; found {
		return true, &val
	}
	return false, nil
}
