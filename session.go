package consuladapter

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/consul/api"
)

type LostLockError string

func (e LostLockError) Error() string {
	return fmt.Sprintf("Lost lock '%s'", e)
}

var ErrInvalidSession = errors.New("invalid session")
var ErrAlreadyRenewed = errors.New("already renewed")

type Session struct {
	kv *api.KV

	id         string
	name       string
	sessionMgr SessionManager
	ttl        time.Duration
	doneCh     chan struct{}
	errCh      chan error

	destroyOnce sync.Once
	lostLock    string

	renewed int32
}

func NewSession(sessionName string, ttl time.Duration, client *api.Client, sessionMgr SessionManager) (*Session, error) {
	return newSession(sessionName, ttl, client.KV(), sessionMgr)
}

func newSession(sessionName string, ttl time.Duration, kv *api.KV, sessionMgr SessionManager) (*Session, error) {
	se := &api.SessionEntry{
		Name:      sessionName,
		Behavior:  api.SessionBehaviorDelete,
		TTL:       ttl.String(),
		LockDelay: 1 * time.Nanosecond,
	}

	id, renewTTL, err := renewOrCreate(se, sessionMgr)
	if err != nil {
		return nil, err
	}

	doneCh := make(chan struct{}, 1)
	errCh := make(chan error, 1)

	s := &Session{
		kv:         kv,
		id:         id,
		name:       sessionName,
		sessionMgr: sessionMgr,
		ttl:        ttl,
		doneCh:     doneCh,
		errCh:      errCh,
	}

	go func() {
		err := sessionMgr.RenewPeriodic(renewTTL, id, nil, doneCh)
		if s.lostLock != "" {
			err = LostLockError(s.lostLock)
		} else {
			err = convertError(err)
		}

		errCh <- err
	}()

	return s, nil
}

func (s *Session) ID() string {
	return s.id
}

func (s *Session) Err() chan error {
	return s.errCh
}

func (s *Session) Destroy() {
	s.destroyOnce.Do(func() {
		close(s.doneCh)
		s.sessionMgr.Destroy(s.id, nil)
	})
}

func (s *Session) Renew() (*Session, error) {
	if atomic.CompareAndSwapInt32(&s.renewed, 0, 1) == true {
		s.Destroy()
		session, err := newSession(s.name, s.ttl, s.kv, s.sessionMgr)
		if err != nil {
			atomic.CompareAndSwapInt32(&s.renewed, 1, 0)
		}
		return session, err
	}

	return nil, ErrAlreadyRenewed
}

func (s *Session) AcquireLock(key string, value []byte) error {
	lock, err := s.sessionMgr.NewLock(s.id, key, value)
	if err != nil {
		return convertError(err)
	}

	lostLock, err := lock.Lock(s.doneCh)
	if err != nil {
		return convertError(err)
	}

	go func() {
		select {
		case <-lostLock:
			s.lostLock = key
			s.Destroy()
		case <-s.doneCh:
		}
	}()

	return nil
}

func (s *Session) SetPresence(key string, value []byte) (<-chan string, error) {
	lock, err := s.sessionMgr.NewLock(s.id, key, value)
	if err != nil {
		return nil, convertError(err)
	}

	lostLock, err := lock.Lock(s.doneCh)
	if err != nil {
		return nil, convertError(err)
	}

	presenceLost := make(chan string, 1)
	go func() {
		select {
		case <-lostLock:
			presenceLost <- key
		case <-s.doneCh:
		}
	}()

	return presenceLost, nil
}

func renewOrCreate(se *api.SessionEntry, sessionMgr SessionManager) (string, string, error) {
	nodeName, err := sessionMgr.NodeName()
	if err != nil {
		return "", "", err
	}

	nodeSessions, _, err := sessionMgr.Node(nodeName, nil)
	if err != nil {
		return "", "", err
	}

	id := ""
	ttl := se.TTL
	session := findSession(se.Name, nodeSessions)
	if session != nil {
		session, _, err = sessionMgr.Renew(session.ID, nil)
		if err == nil && session != nil {
			id = session.ID
			ttl = session.TTL
		}
	}

	if id == "" {
		id, _, err = sessionMgr.Create(se, nil)
		if err != nil {
			return "", "", err
		}
	}

	return id, ttl, nil
}

func findSession(name string, sessions []*api.SessionEntry) *api.SessionEntry {
	for _, session := range sessions {
		if session.Name == name {
			return session
		}
	}

	return nil
}

func convertError(err error) error {
	if err == nil {
		return err
	}

	if strings.Contains(err.Error(), "500 (Invalid session)") {
		return ErrInvalidSession
	}

	return err
}
