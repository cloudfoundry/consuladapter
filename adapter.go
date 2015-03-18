package consuladapter

import (
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
)

type Adapter interface {
	AcquireAndMaintainLock(key string, value []byte, ttl time.Duration, cancelChan <-chan struct{}) (<-chan struct{}, error)
	ReleaseAndDeleteLock(key string) error

	GetValue(key string) ([]byte, error)
	ListPairsExtending(prefix string) (map[string][]byte, error)
	SetValue(key string, value []byte) error

	reset() error
}

func NewAdapter(addresses []string, scheme, datacenter string) (*adapter, error) {
	clients := make([]*api.Client, len(addresses))

	for i, address := range addresses {
		client, err := api.NewClient(&api.Config{
			Address:    address,
			Scheme:     scheme,
			Datacenter: datacenter,
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
	if err != nil {
		return err
	}

	err = lock.Destroy()
	if err != nil {
		return err
	}

	delete(a.locks, key)

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

func (a *adapter) reset() error {
	err := a.clientPool.kvDeleteTree("")
	if err != nil {
		return err
	}

	return a.clientPool.sessionDestroyAll()
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
