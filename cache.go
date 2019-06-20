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

package cache

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Response is the cached response data structure.
type Response struct {
	// Value is the cached response value.
	Value []byte

	// Header is the cached response header.
	Header http.Header

	// Expiration is the cached response expiration date.
	Expiration time.Time

	// LastAccess is the last date a cached response was accessed.
	// Used by LRU and MRU algorithms.
	LastAccess time.Time

	// Frequency is the count of times a cached response is accessed.
	// Used for LFU and MFU algorithms.
	Frequency int
}

// Client data structure for HTTP cache middleware.
type Client struct {
	adapter            Adapter
	ttl                time.Duration
	refreshKey         string
	debugOutputEnabled bool
}

// ClientOption is used to set Client settings.
type ClientOption func(c *Client) error

// Adapter interface for HTTP cache middleware client.
type Adapter interface {
	// Get retrieves the cached response by a given key. It also
	// returns true or false, whether it exists or not.
	Get(prefix, key string) ([]byte, bool)

	Set(prefix, key string, response []byte)

	// Release frees cache for a given key.
	Release(prefix, key string)

	ReleasePrefix(prefix string)
	ReleaseIfStartsWith(key string)
}

// Middleware is the HTTP cache middleware handler.
func (c *Client) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" || r.Method == "" {
			prefix, key := c.GeneratePrefixAndKey(r)
			params := r.URL.Query()
			if _, ok := params[c.refreshKey]; ok {
				if c.debugOutputEnabled {
					log.Printf("refresh key found, releasing key %s:%s\n", prefix, key)
				}
				delete(params, c.refreshKey)

				r.URL.RawQuery = params.Encode()
				key = generateKey(r.URL.String())

				c.adapter.Release(prefix, key)
			} else {
				b, ok := c.adapter.Get(prefix, key)
				response := BytesToResponse(b)
				if ok {
					if response.Expiration.After(time.Now()) {
						if c.debugOutputEnabled {
							log.Printf("serving from cache %s:%s\n", prefix, key)
						}
						response.LastAccess = time.Now()
						response.Frequency++
						c.adapter.Set(prefix, key, response.Bytes())

						//w.WriteHeader(http.StatusNotModified)
						for k, v := range response.Header {
							w.Header().Set(k, strings.Join(v, ","))
						}
						w.Write(response.Value)
						return
					}
					if c.debugOutputEnabled {
						log.Printf("requested object is in cache, but expried - releasing %s:%s\n", prefix, key)
					}
					c.adapter.Release(prefix, key)
				}
			}
			if c.debugOutputEnabled {
				log.Printf("requested object is not in cache or expired - getting %s:%s from DB\n", prefix, key)
			}
			responce, value := c.PutItemToCache(next, r, prefix, key)
			for k, v := range responce.Header {
				w.Header().Set(k, strings.Join(v, ","))
			}
			w.WriteHeader(responce.StatusCode)
			w.Write(value)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// GeneratePrefixAndKey ...
func (c *Client) GeneratePrefixAndKey(r *http.Request) (prefix, key string) {
	sortURLParams(r.URL)
	prefix = r.URL.Path
	key = generateKey(r.URL.String())
	return
}

// PutItemToCache ...
func (c *Client) PutItemToCache(next http.Handler, r *http.Request, prefix, key string) (result *http.Response, value []byte) {
	rec := httptest.NewRecorder()
	next.ServeHTTP(rec, r)
	result = rec.Result()

	statusCode := result.StatusCode
	value = rec.Body.Bytes()
	if statusCode < 400 {
		now := time.Now()

		response := Response{
			Value:      value,
			Header:     result.Header,
			Expiration: now.Add(c.ttl),
			LastAccess: now,
			Frequency:  1,
		}
		c.adapter.Set(prefix, key, response.Bytes())
	}
	return
}

// ReleaseURI ...
func (c *Client) ReleaseURI(uri string) {
	c.adapter.ReleasePrefix(uri)
}

// ReleaseIfStartsWith ...
func (c *Client) ReleaseIfStartsWith(uri string) {
	c.adapter.ReleaseIfStartsWith(uri)
}

// Release ...
func (c *Client) Release(uri string) {
	url, _ := url.Parse(uri)
	sortURLParams(url)
	prefix := url.Path
	key := generateKey(url.String())
	c.adapter.Release(prefix, key)
}

// BytesToResponse converts bytes array into Response data structure.
func BytesToResponse(b []byte) Response {
	var r Response
	dec := gob.NewDecoder(bytes.NewReader(b))
	dec.Decode(&r)

	return r
}

// Bytes converts Response data structure into bytes array.
func (r Response) Bytes() []byte {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	enc.Encode(&r)

	return b.Bytes()
}

func sortURLParams(URL *url.URL) {
	params := URL.Query()
	for _, param := range params {
		sort.Slice(param, func(i, j int) bool {
			return param[i] < param[j]
		})
	}
	URL.RawQuery = params.Encode()
}

func generateKey(URL string) string {
	hash := fnv.New64a()
	hash.Write([]byte(URL))

	return strconv.FormatUint(hash.Sum64(), 10)
}

// NewClient initializes the cache HTTP middleware client with the given
// options.
func NewClient(opts ...ClientOption) (*Client, error) {
	c := &Client{}
	c.debugOutputEnabled = false

	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}

	if c.adapter == nil {
		return nil, errors.New("cache client adapter is not set")
	}
	if int64(c.ttl) < 1 {
		return nil, errors.New("cache client ttl is not set")
	}

	return c, nil
}

// ClientWithAdapter sets the adapter type for the HTTP cache
// middleware client.
func ClientWithAdapter(a Adapter) ClientOption {
	return func(c *Client) error {
		c.adapter = a
		return nil
	}
}

// ClientWithTTL sets how long each response is going to be cached.
func ClientWithTTL(ttl time.Duration) ClientOption {
	return func(c *Client) error {
		if int64(ttl) < 1 {
			return fmt.Errorf("cache client ttl %v is invalid", ttl)
		}

		c.ttl = ttl

		return nil
	}
}

// ClientWithRefreshKey sets the parameter key used to free a request
// cached response. Optional setting.
func ClientWithRefreshKey(refreshKey string) ClientOption {
	return func(c *Client) error {
		c.refreshKey = refreshKey
		return nil
	}
}

// ClientWithDebugOutput sets the parameter key used to switch client debug
// output. Optional setting.
func ClientWithDebugOutput(debugOutputEnabled bool) ClientOption {
	return func(c *Client) error {
		c.debugOutputEnabled = debugOutputEnabled
		return nil
	}
}
