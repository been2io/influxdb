package tsi1

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/influxdata/influxdb/tsdb"
)

// TagValueSeriesIDCache is an LRU cache for series id sets associated with
// name -> key -> value mappings. The purpose of the cache is to provide
// efficient means to get sets of series ids that would otherwise involve merging
// many individual bitmaps at query time.
//
// When initialising a TagValueSeriesIDCache a capacity must be provided. When
// more than c items are added to the cache, the least recently used item is
// evicted from the cache.
//
// A TagValueSeriesIDCache comprises a linked list implementation to track the
// order by which items should be evicted from the cache, and a hashmap implementation
// to provide constant time retrievals of items from the cache.
type TagValueSeriesIDCache struct {
	sync.RWMutex
	cache    *measurementsMap
	evictor  *evictor
	capacity int
}
type evictor struct {
	l  *list.List
	rw sync.RWMutex
}

func (e *evictor) MoveToFront(ele *list.Element) {
	e.rw.Lock()
	defer e.rw.Unlock()
	e.l.MoveToFront(ele)
}
func (e *evictor) PushFront(ele interface{}) *list.Element {
	e.rw.Lock()
	defer e.rw.Unlock()
	return e.l.PushFront(ele)
}
func (e *evictor) Len() int {
	e.rw.RLock()
	defer e.rw.RUnlock()
	return e.l.Len()
}
func (e *evictor) Back() *list.Element {
	e.rw.RLock()
	defer e.rw.RUnlock()
	return e.l.Back()
}
func (e *evictor) Remove(ele *list.Element) {
	e.rw.Lock()
	defer e.rw.Unlock()
	e.l.Remove(ele)
}

type tagMap struct {
	m     sync.Map
	count int32
}

func (kv *tagMap) Get(name string) (*valueMap, bool) {
	m, ok := kv.m.Load(name)
	if ok {
		return m.(*valueMap), true
	} else {
		return nil, false
	}
}
func (kv *tagMap) Put(name string, v *valueMap) {
	kv.m.Store(name, v)
	atomic.AddInt32(&kv.count, 1)

}
func (kv *tagMap) Delete(name string) {
	atomic.AddInt32(&kv.count, -1)
	kv.m.Delete(name)
}

type valueMap struct {
	m     sync.Map
	count int32
}

func (kv *valueMap) Get(name string) (*list.Element, bool) {
	m, ok := kv.m.Load(name)
	if ok {
		return m.(*list.Element), true
	} else {
		return nil, false
	}
}
func (kv *valueMap) Put(name string, v *list.Element) {
	kv.m.Store(name, v)
	atomic.AddInt32(&kv.count, 1)
}
func (kv *valueMap) Delete(name string) {
	atomic.AddInt32(&kv.count, -1)
	kv.m.Delete(name)
}

type measurementsMap struct {
	m sync.Map
}

func (kv *measurementsMap) Get(name string, k string, v string) (*list.Element, bool) {
	m, ok := kv.m.Load(name)
	if !ok {
		return nil, false
	}
	km, ok := m.(*tagMap).Get(k)
	if !ok {
		return nil, false
	}
	return km.Get(v)
}

func (kv *measurementsMap) Put(name string, k string, v string, ele *list.Element) {
	m, ok := kv.m.Load(name)
	if !ok {
		m = &tagMap{}
		kv.m.Store(name, m)
	}
	km := m.(*tagMap)
	vm, ok := km.Get(k)
	if !ok {
		vm = &valueMap{}
		km.Put(k, vm)
	}
	vm.Put(v, ele)
}
func (kv *measurementsMap) Delete(name string, k string, v string) {
	m, ok := kv.m.Load(name)
	if !ok {
		return
	}
	km := m.(*tagMap)
	vm, ok := km.Get(k)
	if !ok {
		return
	}
	vm.Delete(v)
	if atomic.LoadInt32(&vm.count) == 0 {
		km.Delete(k)
	}
	if atomic.LoadInt32(&km.count) == 0 {
		kv.m.Delete(name)
	}
}
func (kv *measurementsMap) DeleteTagKey(name string, k string) {
	m, ok := kv.m.Load(name)
	if !ok {
		return
	}
	km := m.(*tagMap)
	km.Delete(k)
}
func (kv *measurementsMap) MeasurementExists(name string) bool {
	_, ok := kv.m.Load(name)
	return ok
}

// NewTagValueSeriesIDCache returns a TagValueSeriesIDCache with capacity c.
func NewTagValueSeriesIDCache(c int) *TagValueSeriesIDCache {
	return &TagValueSeriesIDCache{
		cache: &measurementsMap{},
		evictor: &evictor{
			l: list.New(),
		},
		capacity: c,
	}
}

// Get returns the SeriesIDSet associated with the {name, key, value} tuple if it
// exists.
func (c *TagValueSeriesIDCache) Get(name, key, value []byte) *tsdb.SeriesIDSet {
	return c.get(name, key, value)
}

func (c *TagValueSeriesIDCache) get(name, key, value []byte) *tsdb.SeriesIDSet {
	if ele, ok := c.cache.Get(string(name), string(key), string(value)); ok {
		c.Lock()
		c.evictor.MoveToFront(ele)
		c.Unlock()
		return ele.Value.(*seriesIDCacheElement).IDSet()
	}
	return nil
}

// exists returns true if the an item exists for the tuple {name, key, value}.
func (c *TagValueSeriesIDCache) exists(name, key, value []byte) bool {
	if _, ok := c.cache.Get(string(name), string(key), string(value)); ok {
		return ok
	}
	return false
}

// addToSet adds x to the SeriesIDSet associated with the tuple {name, key, value}
// if it exists. This method takes a lock on the underlying SeriesIDSet.
//
// NB this does not count as an access on the set—therefore the set is not promoted
// within the LRU cache.
func (c *TagValueSeriesIDCache) addToSet(name, key, value []byte, x uint64) {
	if ele, ok := c.cache.Get(string(name), string(key), string(value)); ok {
		ss := ele.Value.(*seriesIDCacheElement)
		ss.Add(x)
	}
}

// measurementContainsSets returns true if there are sets cached for the provided measurement.
func (c *TagValueSeriesIDCache) measurementContainsSets(name []byte) bool {
	ok := c.cache.MeasurementExists(string(name))
	return ok
}

// Put adds the SeriesIDSet to the cache under the tuple {name, key, value}. If
// the cache is at its limit, then the least recently used item is evicted.
func (c *TagValueSeriesIDCache) Put(name, key, value []byte, ss *tsdb.SeriesIDSet) {
	// Check under the write lock if the relevant item is now in the cache.
	if c.exists(name, key, value) {
		return
	}

	// Ensure our SeriesIDSet is go heap backed.
	if ss != nil {
		ss = ss.Clone()
	}
	// Create list item, and add to the front of the eviction list.
	listElement := c.evictor.PushFront(&seriesIDCacheElement{
		name:        string(name),
		key:         string(key),
		value:       string(value),
		seriesIDSet: ss,
	})
	c.cache.Put(string(name), string(key), string(value), listElement)
	c.checkEviction()
}

// Delete removes x from the tuple {name, key, value} if it exists.
// This method takes a lock on the underlying SeriesIDSet.
func (c *TagValueSeriesIDCache) Delete(name, key, value []byte, x uint64) {
	c.delete(name, key, value, x)
}

// delete removes x from the tuple {name, key, value} if it exists.
func (c *TagValueSeriesIDCache) delete(name, key, value []byte, x uint64) {
	ele, ok := c.cache.Get(string(name), string(key), string(value))
	if ok {
		if ss := ele.Value.(*seriesIDCacheElement); ss != nil {
			ele.Value.(*seriesIDCacheElement).Remove(x)
		}
	}
}

// checkEviction checks if the cache is too big, and evicts the least recently used
// item if it is.
func (c *TagValueSeriesIDCache) checkEviction() {
	if c.evictor.Len() <= c.capacity {
		return
	}
	e := c.evictor.Back()
	// Least recently used item.
	listElement := e.Value.(*seriesIDCacheElement)
	name := listElement.name
	key := listElement.key
	value := listElement.value

	c.evictor.Remove(e)
	// Remove from evictor
	c.cache.Delete(string(name), string(key), string(value))

}

// seriesIDCacheElement is an item stored within a cache.
type seriesIDCacheElement struct {
	name        string
	key         string
	value       string
	seriesIDSet *tsdb.SeriesIDSet
	rw          sync.RWMutex
}

func (s *seriesIDCacheElement) IDSet() *tsdb.SeriesIDSet {
	s.rw.RLock()
	defer s.rw.RUnlock()
	return s.seriesIDSet
}
func (s *seriesIDCacheElement) Add(i uint64) {
	s.rw.Lock()
	defer s.rw.Unlock()
	if s.seriesIDSet == nil {
		s.seriesIDSet = tsdb.NewSeriesIDSet(i)
	}
	s.seriesIDSet.Add(i)
}
func (s *seriesIDCacheElement) Remove(i uint64) {
	s.rw.Lock()
	defer s.rw.Unlock()
	if s.seriesIDSet == nil {
		return
	}
	s.seriesIDSet.Remove(i)
}
func (s *seriesIDCacheElement) Put(set *tsdb.SeriesIDSet) {
	s.rw.Lock()
	defer s.rw.Unlock()
	s.seriesIDSet = set
}
