package consuladapter

import (
	"fmt"
	"time"

	"github.com/hashicorp/consul/api"
)

type clientPool struct {
	clients []*api.Client
}

func (cp *clientPool) lockOpts(opts *api.LockOptions) (*api.Lock, error) {
	var err error

	for _, client := range cp.clients {
		var lock *api.Lock
		lock, err = client.LockOpts(opts)
		if err == nil {
			return lock, nil
		}
	}

	return nil, poolError(err)
}

func (cp *clientPool) kvGet(key string) (*api.KVPair, error) {
	var err error

	for _, client := range cp.clients {
		var kvPair *api.KVPair
		kvPair, _, err = client.KV().Get(key, nil)

		// Checking `err == nil` as the success case is invalid, because it
		// sometimes returns nil for both the value and error when there
		// is no response internally in consul for the request, or the
		// response is empty.
		if kvPair != nil {
			return kvPair, nil
		}
	}

	if err != nil {
		return nil, poolError(err)
	} else {
		return nil, NewKeyNotFoundError(key)
	}
}

func (cp *clientPool) kvKeysWithWait(prefix string, waitIndex uint64, waitDuration time.Duration) ([]string, uint64, error) {
	var err error
	queryOpts := &api.QueryOptions{
		WaitIndex: waitIndex,
		WaitTime:  waitDuration,
	}

	for _, client := range cp.clients {
		keys, queryMeta, e := client.KV().Keys(prefix, "", queryOpts)

		if e != nil {
			err = e
			continue
		}

		if keys != nil && queryMeta != nil {
			return keys, queryMeta.LastIndex, nil
		}
	}

	if err != nil {
		return nil, 0, poolError(err)
	} else {
		return nil, 0, NewPrefixNotFoundError(prefix)
	}
}

func (cp *clientPool) kvList(prefix string) (api.KVPairs, error) {
	var err error

	for _, client := range cp.clients {
		var kvPairs api.KVPairs
		kvPairs, _, err = client.KV().List(prefix, nil)

		if err == nil {
			return kvPairs, nil
		}
	}

	return nil, poolError(err)
}

func (cp *clientPool) kvPut(key string, value []byte) error {
	var err error

	for _, client := range cp.clients {
		_, err = client.KV().Put(&api.KVPair{Key: key, Value: value}, nil)
		if err == nil {
			return nil
		}
	}

	return poolError(err)
}

func (cp *clientPool) kvDeleteTree(prefix string) error {
	var err error

	for _, client := range cp.clients {
		_, err = client.KV().DeleteTree(prefix, nil)
		if err == nil {
			return nil
		}
	}

	return poolError(err)
}

func (cp *clientPool) sessionDestroyAll() error {
	var err error

	for _, client := range cp.clients {
		var sessions []*api.SessionEntry
		sessions, _, err = client.Session().List(nil)
		if err != nil {
			continue
		}

		for _, session := range sessions {
			_, err = client.Session().Destroy(session.ID, nil)
			if err != nil {
				break
			}
		}

		if err == nil {
			return nil
		}
	}

	return poolError(err)

}

func poolError(err error) error {
	return fmt.Errorf("all client requests failed; last error message: %s", err.Error())
}

func NewKeyNotFoundError(key string) error {
	return KeyNotFoundError{key: key}
}

type KeyNotFoundError struct {
	key string
}

func (e KeyNotFoundError) Error() string {
	return fmt.Sprintf("key not found: '%s'", e.key)
}

func NewPrefixNotFoundError(prefix string) error {
	return PrefixNotFoundError{prefix: prefix}
}

type PrefixNotFoundError struct {
	prefix string
}

func (e PrefixNotFoundError) Error() string {
	return fmt.Sprintf("prefix not found: '%s'", e.prefix)
}
