package processors

import (
	"errors"
	"github.com/golang/glog"
	kube_config "k8s.io/heapster/common/kubernetes"
	"k8s.io/heapster/metrics/core"
	kube_api "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/client/cache"
	kube_client "k8s.io/kubernetes/pkg/client/unversioned"
	"k8s.io/kubernetes/pkg/fields"
	"net/url"
	"time"
)

type ServiceBasedEnricher struct {
	serviceLister *cache.StoreToServiceLister
	podLister     *cache.StoreToPodLister
	reflector     *cache.Reflector
}

func (this *ServiceBasedEnricher) Name() string {
	return "service_based_enricher"
}

func (this *ServiceBasedEnricher) Process(batch *core.DataBatch) (*core.DataBatch, error) {
	newMs := make(map[string]*core.MetricSet, len(batch.MetricSets))
	for k, v := range batch.MetricSets {
		switch v.Labels[core.LabelMetricSetType.Key] {
		case core.MetricSetTypePod, core.MetricSetTypePodContainer:
			namespace := v.Labels[core.LabelNamespaceName.Key]
			podName := v.Labels[core.LabelPodName.Key]
			pod, err := this.getPod(namespace, podName)
			if err != nil {
				glog.V(3).Infof("Failed to get pod %s from cache: %v", core.PodKey(namespace, podName), err)
				continue
			}
			this.addServiceInfo(k, v, pod, newMs)
		}
	}

	for k, v := range newMs {
		batch.MetricSets[k] = v
	}
	return batch, nil
}

func (this *ServiceBasedEnricher) getPod(namespace, name string) (*kube_api.Pod, error) {
	o, exists, err := this.podLister.Get(
		&kube_api.Pod{
			ObjectMeta: kube_api.ObjectMeta{
				Namespace: namespace,
				Name:      name,
			},
		},
	)
	if err != nil {
		return nil, err
	}
	if !exists || o == nil {
		return nil, errors.New("cannot find pod definition")
	}
	pod, ok := o.(*kube_api.Pod)
	if !ok {
		return nil, errors.New("cache contains wrong type")
	}
	return pod, nil
}

func (this *ServiceBasedEnricher) addServiceInfo(key string, podMs *core.MetricSet, pod *kube_api.Pod, newMs map[string]*core.MetricSet) {
	services, err := this.serviceLister.GetPodServices(pod)
	if err != nil || len(services) == 0 {
		return
	}
	podMs.Labels[core.LabelServiceName.Key] = services[0].Name
	newMs[key] = podMs
}

func NewServiceBasedEnricher(url *url.URL, podLister *cache.StoreToPodLister) (*ServiceBasedEnricher, error) {
	kubeConfig, err := kube_config.GetKubeClientConfig(url)
	if err != nil {
		return nil, err
	}
	kubeClient := kube_client.NewOrDie(kubeConfig)

	// watch service
	lw := cache.NewListWatchFromClient(kubeClient, "services", kube_api.NamespaceAll, fields.Everything())
	serviceLister := &cache.StoreToServiceLister{Store: cache.NewStore(cache.MetaNamespaceKeyFunc)}
	reflector := cache.NewReflector(lw, &kube_api.Service{}, serviceLister.Store, time.Hour)
	reflector.Run()
	return &ServiceBasedEnricher{
		podLister:     podLister,
		serviceLister: serviceLister,
		reflector:     reflector,
	}, nil
}
