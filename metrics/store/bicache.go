package store

import (
	"fmt"

	kube_api "k8s.io/kubernetes/pkg/api"
	. "k8s.io/kubernetes/pkg/client/cache"
)

// biCache responsibilities are limited to:
// 1. Computing keys for objects via keyFunc
// 2. Invoking methods of a ThreadSafeStorage interface
type BiCache struct {
	// cacheStorage bears the burden of thread safety for the biCache
	cacheStorage ThreadSafeStore
	keyByIP      map[string]string
	// to store user ganglia IP
	gangliaContainer map[string]string
	// keyFunc is used to make the key for objects stored in and retrieved from items, and
	// should be deterministic.
	keyFunc KeyFunc
	// IPFunc knows how to get IP from an object. Implementations should be
	// resource (node, pod) specific.
	ipFunc IPFunc
	// GangliaFunc checks if a pod contains a custom ganglia container
	gangliaIpFunc GangliaFunc
}

// IPFunc knows how to get IP from an object. Implementations should be resource (node, pod) specific.
type IPFunc func(obj interface{}) ([]string, error)

// GangliaFunc checks if a pod contains a custom ganglia container
type GangliaFunc func(obj interface{}) (bool, string)

// IPError will be returned any time a IPFunc gives an error; it includes the object
// at fault.
type IPError struct {
	Obj interface{}
	Err error
}

// Error gives a human-readable description of the error.
func (ip IPError) Error() string {
	return fmt.Sprintf("couldn't retrieve IP for object %+v: %v", ip.Obj, ip.Err)
}

var _ Store = &BiCache{}

// Add inserts an item into the biCache.
func (c *BiCache) Add(obj interface{}) error {
	key, err := c.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	ips, err := c.ipFunc(obj)
	if err != nil {
		return IPError{obj, err}
	}
	c.cacheStorage.Add(key, obj)
	for _, ip := range ips {
		c.keyByIP[ip] = key
	}
	found, gIp := c.gangliaIpFunc(obj)
	if found {
		c.gangliaContainer[gIp] = key
	}
	return nil
}

// Update sets an item in the biCache to its updated state.
func (c *BiCache) Update(obj interface{}) error {
	key, err := c.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	ips, err := c.ipFunc(obj)
	if err != nil {
		return IPError{obj, err}
	}
	c.cacheStorage.Update(key, obj)
	for _, ip := range ips {
		c.keyByIP[ip] = key
	}
	found, gIp := c.gangliaIpFunc(obj)
	if found {
		c.gangliaContainer[gIp] = key
	}
	return nil
}

// Delete removes an item from the biCache.
func (c *BiCache) Delete(obj interface{}) error {
	key, err := c.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	ips, err := c.ipFunc(obj)
	if err != nil {
		return IPError{obj, err}
	}
	c.cacheStorage.Delete(key)
	for _, ip := range ips {
		delete(c.keyByIP, ip)
	}
	found, gIp := c.gangliaIpFunc(obj)
	if found {
		delete(c.gangliaContainer, gIp)
	}
	return nil
}

// List returns a list of all the items.
// List is completely threadsafe as long as you treat all items as immutable.
func (c *BiCache) List() []interface{} {
	return c.cacheStorage.List()
}

// ListKeys returns a list of all the keys of the objects currently
// in the biCache.
func (c *BiCache) ListKeys() []string {
	return c.cacheStorage.ListKeys()
}

// GetIndexers returns the indexers of cache
func (c *BiCache) GetIndexers() Indexers {
	return c.cacheStorage.GetIndexers()
}

// Index returns a list of items that match on the index function
// Index is thread-safe so long as you treat all items as immutable
func (c *BiCache) Index(indexName string, obj interface{}) ([]interface{}, error) {
	return c.cacheStorage.Index(indexName, obj)
}

// ListIndexFuncValues returns the list of generated values of an Index func
func (c *BiCache) ListIndexFuncValues(indexName string) []string {
	return c.cacheStorage.ListIndexFuncValues(indexName)
}

func (c *BiCache) ByIndex(indexName, indexKey string) ([]interface{}, error) {
	return c.cacheStorage.ByIndex(indexName, indexKey)
}

// Get returns the requested item, or sets exists=false.
// Get is completely threadsafe as long as you treat all items as immutable.
func (c *BiCache) Get(obj interface{}) (item interface{}, exists bool, err error) {
	key, err := c.keyFunc(obj)
	if err != nil {
		return nil, false, KeyError{obj, err}
	}
	return c.GetByKey(key)
}

// GetByKey returns the request item, or exists=false.
// GetByKey is completely threadsafe as long as you treat all items as immutable.
func (c *BiCache) GetByKey(key string) (item interface{}, exists bool, err error) {
	item, exists = c.cacheStorage.Get(key)
	return item, exists, nil
}

// GetByIP returns the request item, or exists=false.
// GetByIP is completely threadsafe as long as you treat all items as immutable.
func (c *BiCache) GetByIP(ip string) (item interface{}, exists bool, err error) {
	if key, ok := c.keyByIP[ip]; ok {
		item, exists = c.cacheStorage.Get(key)
	}
	return item, exists, nil
}

func (c *BiCache) GetGangliaPodIP() []string {
	list := make([]string, 0)
	for ip := range c.gangliaContainer {
		list = append(list, ip)
	}
	return list
}

// Replace will delete the contents of 'c', using instead the given list.
// 'c' takes ownership of the list, you should not reference the list again
// after calling this function.
func (c *BiCache) Replace(list []interface{}, resourceVersion string) error {
	items := map[string]interface{}{}
	gangliaLists := make(map[string]string)
	newKeyByIP := make(map[string]string)
	for _, item := range list {
		key, err := c.keyFunc(item)
		if err != nil {
			return KeyError{item, err}
		}
		ips, err := c.ipFunc(item)
		if err != nil {
			return IPError{item, err}
		}
		items[key] = item
		for _, ip := range ips {
			newKeyByIP[ip] = key
		}
		found, gIp := c.gangliaIpFunc(item)
		if found {
			gangliaLists[gIp] = key
		}

	}
	c.keyByIP = newKeyByIP
	c.gangliaContainer = gangliaLists
	c.cacheStorage.Replace(items, resourceVersion)
	return nil
}

// NewStore returns a Store implemented simply with a map and a lock.
func NewBiCache(keyFunc KeyFunc, ipFunc IPFunc, gangliaIpFunc GangliaFunc) Indexer {
	return &BiCache{
		cacheStorage:  NewThreadSafeStore(Indexers{}, Indices{}),
		keyFunc:       keyFunc,
		ipFunc:        ipFunc,
		gangliaIpFunc: gangliaIpFunc,
	}
}

// NodeIPFunc is an IPFunc which knows how to retrieve IP from Node API object.
func NodeIPFunc(obj interface{}) ([]string, error) {
	if key, ok := obj.(ExplicitKey); ok {
		return nil, fmt.Errorf("object %v has no ip", key)
	}

	node, ok := obj.(*kube_api.Node)
	if !ok {
		return nil, fmt.Errorf("cache contains wrong type")
	}
	ips := make([]string, 0)
	for _, addr := range node.Status.Addresses {
		if addr.Type == kube_api.NodeInternalIP && addr.Address != "" {
			ips = append(ips, addr.Address)
		} else if addr.Type == kube_api.NodeExternalIP && addr.Address != "" {
			ips = append(ips, addr.Address)
		} else if addr.Type == kube_api.NodeLegacyHostIP && addr.Address != "" {
			ips = append(ips, addr.Address)
		}
	}
	return ips, nil
}

// PodIPFunc is an IPFunc which knows how to retrieve IP from Pod API object.
func PodIPFunc(obj interface{}) ([]string, error) {
	if key, ok := obj.(ExplicitKey); ok {
		return nil, fmt.Errorf("object %v has no ip", key)
	}

	pod, ok := obj.(*kube_api.Pod)
	if !ok {
		return nil, fmt.Errorf("cache contains wrong type")
	}
	return []string{pod.Status.PodIP}, nil
}

// This func calls for pod type object &
// checks whether obj has ganglia or not ... @MS
func GangliaPodIPFunc(obj interface{}) (bool, string) {
	pod, ok := obj.(*kube_api.Pod)
	if !ok {
		return false, ""
	}
	// ganglia pod must have following label with ganglia container name ... @MS
	val, ok := pod.Annotations["monitoring.appscode.com/container"]
	if !ok {
		return false, ""
	}
	for _, container := range pod.Spec.Containers {
		if container.Name == val {
			// returns pod private IP which will be used to get ganglia data ... @MS
			return true, pod.Status.PodIP
		}
	}
	return false, ""
}

//
func GangliaNodeIPFunc(obj interface{}) (bool, string) {
	return false, ""
}
