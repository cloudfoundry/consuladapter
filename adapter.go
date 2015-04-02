package consuladapter

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/cloudfoundry-incubator/cf_http"
	"github.com/hashicorp/consul/api"
)

const defaultWatchBlockDuration = 10 * time.Second

func Parse(urlArg string) (string, []string, error) {
	urlStrings := strings.Split(urlArg, ",")
	addresses := []string{}
	scheme := ""

	for _, urlString := range urlStrings {
		u, err := url.Parse(urlString)
		if err != nil {
			return "", nil, err
		}

		if scheme == "" {
			if u.Scheme != "http" && u.Scheme != "https" {
				return "", nil, errors.New("scheme must be http or https")
			}

			scheme = u.Scheme
		} else if scheme != u.Scheme {
			return "", nil, errors.New("inconsistent schemes")
		}

		if u.Host == "" {
			return "", nil, errors.New("missing address")
		}
		addresses = append(addresses, u.Host)
	}

	return scheme, addresses, nil
}

type Adapter interface {
	AcquireAndMaintainLock(key string, value []byte, ttl time.Duration, cancelChan <-chan struct{}) (lost <-chan struct{}, err error)
	ReleaseAndDeleteLock(key string) error

	GetValue(key string) ([]byte, error)
	ListPairsExtending(prefix string) (map[string][]byte, error)
	SetValue(key string, value []byte) error

	WatchForDisappearancesUnder(prefix string) (disappearance <-chan []string, cancel chan<- struct{}, err <-chan error)

	reset() error
}

func NewAdapter(addresses []string, scheme string) (Adapter, error) {
	if len(scheme) == 0 {
		return nil, errors.New("missing consul scheme")
	}

	if len(addresses) == 0 {
		return nil, errors.New("missing consul addresses")
	}

	clients := make([]*api.Client, len(addresses))

	for i, address := range addresses {
		client, err := api.NewClient(&api.Config{
			Address:    address,
			Scheme:     scheme,
			HttpClient: cf_http.NewStreamingClient(),
		})

		if err != nil {
			return nil, err
		}

		clients[i] = client
	}

	return &adapter{
		clientPool: &clientPool{clients: clients},
		locks:      map[string]*api.Lock{},
		lockLock:   &sync.Mutex{},
	}, nil
}

type adapter struct {
	*clientPool
	locks    map[string]*api.Lock
	lockLock *sync.Mutex
}

func (a *adapter) AcquireAndMaintainLock(key string, value []byte, ttl time.Duration, cancelChan <-chan struct{}) (<-chan struct{}, error) {
	lock, err := a.clientPool.lockOpts(&api.LockOptions{
		Key:        key,
		Value:      value,
		SessionTTL: ttl.String(),
	})
	if err != nil {
		return nil, err
	}

	lostLockChan, err := lock.Lock(cancelChan)
	if err != nil {
		return nil, err
	}

	// Consul doesn't document this behaviour, but if the given cancelChan is
	// closed or sent something, then Lock() returns nil, nil.
	if err == nil && lostLockChan == nil {
		return nil, NewCancelledLockAttemptError(key)
	}

	a.lockLock.Lock()
	a.locks[key] = lock
	a.lockLock.Unlock()
	return lostLockChan, nil
}

func (a *adapter) ReleaseAndDeleteLock(key string) error {
	a.lockLock.Lock()
	defer a.lockLock.Unlock()

	lock, found := a.locks[key]
	if !found {
		return NewUnknownLockError(key)
	}

	err := lock.Unlock()
	if err != nil && err != api.ErrLockNotHeld {
		return err
	}

	delete(a.locks, key)
	_ = lock.Destroy() // best effort cleanup

	return nil
}

func (a *adapter) GetValue(key string) ([]byte, error) {
	kvPair, err := a.clientPool.kvGet(key)
	if err != nil {
		return nil, err
	}

	return kvPair.Value, nil
}

func (a *adapter) ListPairsExtending(prefix string) (map[string][]byte, error) {
	kvPairs, err := a.clientPool.kvList(prefix)
	if err != nil {
		return nil, err
	}

	children := map[string][]byte{}
	for _, kvPair := range kvPairs {
		children[kvPair.Key] = kvPair.Value
	}
	return children, nil
}

func (a *adapter) SetValue(key string, value []byte) error {
	return a.clientPool.kvPut(key, value)
}

func (a *adapter) WatchForDisappearancesUnder(prefix string) (<-chan []string, chan<- struct{}, <-chan error) {
	disappearanceChan := make(chan []string)
	cancelChan := make(chan struct{})
	errorChan := make(chan error, 1)

	prefixNotFound := NewPrefixNotFoundError(prefix)
	go func() {
		defer close(disappearanceChan)
		defer close(errorChan)

		keys := keySet{}
		var index uint64 = 0

		for {
			select {
			case <-cancelChan:
				return
			default:
				newKeyStrings, newIndex, err := a.clientPool.kvKeysWithWait(prefix, index, defaultWatchBlockDuration)
				if err != nil {
					if err != prefixNotFound || len(keys) == 0 {
						errorChan <- err
						return
					}
				}

				newKeys := newKeySetFromStrings(newKeyStrings)
				if missing := difference(keys, newKeys); len(missing) > 0 {
					disappearanceChan <- missing
				}

				keys = newKeys
				index = newIndex
			}
		}
	}()

	return disappearanceChan, cancelChan, errorChan
}

func (a *adapter) reset() error {
	err := a.clientPool.kvDeleteTree("")
	err2 := a.clientPool.sessionDestroyAll()
	if err != nil {
		return err
	}

	return err2
}

func newKeySetFromStrings(keyStrings []string) keySet {
	newKeySet := keySet{}
	for _, key := range keyStrings {
		newKeySet[key] = struct{}{}
	}
	return newKeySet
}

type keySet map[string]struct{}

func difference(a, b keySet) []string {
	var missing []string
	for key, _ := range a {
		if _, ok := b[key]; !ok {
			missing = append(missing, key)
		}
	}

	return missing
}

func NewCancelledLockAttemptError(key string) error {
	return cancelledLockAttemptError{key: key}
}

type cancelledLockAttemptError struct {
	key string
}

func (e cancelledLockAttemptError) Error() string {
	return fmt.Sprintf("Cancelled attempt to acquire lock '%s'", e.key)
}

func NewUnknownLockError(key string) error {
	return unknownLockError{key: key}
}

type unknownLockError struct {
	key string
}

func (e unknownLockError) Error() string {
	return fmt.Sprintf("Unknown lock '%s'", e.key)
}
