package filter

import (
	"net/url"
	"regexp"

	"github.com/jimsmart/grobotstxt"
	"github.com/xunterr/aracno/internal/fetcher"
	"github.com/xunterr/aracno/internal/storage"
	"github.com/xunterr/aracno/internal/storage/inmem"
)

type FilterFunc func(url *url.URL) (bool, error)

type FilterChain struct {
	filters []FilterFunc
}

func NewFilterChain() *FilterChain {
	return &FilterChain{}
}

func (fc *FilterChain) Append(f ...FilterFunc) {
	fc.filters = append(fc.filters, f...)
}

func (fc *FilterChain) Test(url *url.URL) (bool, error) {
	for _, f := range fc.filters {
		ok, err := f(url)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func NewRegexFilter(regex string) FilterFunc {
	r := regexp.MustCompile(regex)
	return func(url *url.URL) (bool, error) {
		return r.MatchString(url.String()), nil
	}
}

type robotsFilter struct {
	fetcher fetcher.Fetcher
	cache   *inmem.LruCache[string]
}

func NewRobotsFilter(fetcher fetcher.Fetcher, cacheSize uint) FilterFunc {
	cache := inmem.NewLruCache[string](cacheSize)
	robotsFilter := robotsFilter{
		cache:   cache,
		fetcher: fetcher,
	}
	return robotsFilter.canCrawl
}

func (rf *robotsFilter) canCrawl(res *url.URL) (bool, error) {
	body, err := rf.getRobots(res)
	if err != nil {
		return false, err
	}

	ok := grobotstxt.AgentAllowed(body, "GoBot/1.0", res.String())
	return ok, nil
}

func (rf *robotsFilter) getRobots(url *url.URL) (string, error) {
	body, err := rf.cache.Get(url.Hostname())
	if err == nil {
		return body, nil
	}

	if err != storage.NoSuchKeyError {
		return "", err
	}

	details, err := rf.fetchRobots(url)
	if err != nil {
		return "", err
	}

	body = string(details.Body)

	err = rf.cache.Put(url.Hostname(), body)
	if err != nil {
		return "", err
	}
	return body, nil
}

func (rf *robotsFilter) fetchRobots(url *url.URL) (*fetcher.FetchDetails, error) {
	robotsUrl := *url
	robotsUrl.Path = "/robots.txt"
	robotsUrl.RawQuery = ""
	robotsUrl.Fragment = ""

	return rf.fetcher.Fetch(&robotsUrl)
}
