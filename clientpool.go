package consuladapter

import (
	"fmt"

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
	return keyNotFoundError{key: key}
}

type keyNotFoundError struct {
	key string
}

func (e keyNotFoundError) Error() string {
	return fmt.Sprintf("key not found: '%s'", e.key)
}
