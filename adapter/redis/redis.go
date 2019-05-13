/*
MIT License

Copyright (c) 2018 Victor Springer

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

package redis

import (
	cache "github.com/Columbus-internet/http-cache"
	"github.com/go-redis/redis"
)

// Adapter is the memory adapter data structure.
type Adapter struct {
	ring *redis.Ring
}

// RingOptions exports go-redis RingOptions type.
type RingOptions redis.RingOptions

// Get implements the cache Adapter interface Get method.
func (a *Adapter) Get(prefix, key string) ([]byte, bool) {
	if c, err := a.ring.HGet(prefix, key).Bytes(); err == nil {
		return c, true
	}
	return nil, false
}

// Set implements the cache Adapter interface Set method.
func (a *Adapter) Set(prefix, key string, response []byte) {
	a.ring.HSet(prefix, key, response)
}

// Release implements the cache Adapter interface Release method.
func (a *Adapter) Release(prefix, key string) {
	a.ring.HDel(prefix, key)
}

// ReleasePrefix ...
func (a *Adapter) ReleasePrefix(prefix string) {
	a.ring.Del(prefix)
}

// NewAdapter initializes Redis adapter.
func NewAdapter(opt *RingOptions) cache.Adapter {
	ropt := redis.RingOptions(*opt)
	return &Adapter{
		ring: redis.NewRing(&ropt),
	}
}
