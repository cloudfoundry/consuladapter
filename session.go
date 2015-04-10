package consuladapter

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
)

type LostLockError string

func (e LostLockError) Error() string {
	return fmt.Sprintf("Lost lock '%s'", e)
}

var ErrInvalidSession = errors.New("invalid session")
var ErrDestroyed = errors.New("already destroyed")

type Session struct {
	kv *api.KV

	name       string
	sessionMgr SessionManager
	ttl        time.Duration

	errCh chan error

	lock      sync.Mutex
	id        string
	destroyed bool
	doneCh    chan struct{}
	lostLock  string
}

func NewSession(sessionName string, ttl time.Duration, client *api.Client, sessionMgr SessionManager) (*Session, error) {
	return newSession(sessionName, ttl, client.KV(), sessionMgr)
}

func newSession(sessionName string, ttl time.Duration, kv *api.KV, sessionMgr SessionManager) (*Session, error) {
	doneCh := make(chan struct{}, 1)
	errCh := make(chan error, 1)

	s := &Session{
		kv:         kv,
		name:       sessionName,
		sessionMgr: sessionMgr,
		ttl:        ttl,
		doneCh:     doneCh,
		errCh:      errCh,
	}

	return s, nil
}

func (s *Session) ID() string {
	return s.id
}

func (s *Session) Err() chan error {
	return s.errCh
}

func (s *Session) Destroy() {
	s.lock.Lock()
	s.destroy()
	s.lock.Unlock()
}

func (s *Session) destroy() {
	if s.destroyed == false {
		close(s.doneCh)

		if s.id != "" {
			s.sessionMgr.Destroy(s.id, nil)
		}

		s.destroyed = true
	}
}

func (s *Session) createOrRenewSession() error {
	if s.destroyed {
		return ErrDestroyed
	}

	if s.id != "" {
		return nil
	}

	se := &api.SessionEntry{
		Name:      s.name,
		Behavior:  api.SessionBehaviorDelete,
		TTL:       s.ttl.String(),
		LockDelay: 1 * time.Nanosecond,
	}

	id, renewTTL, err := renewOrCreate(se, s.sessionMgr)
	if err != nil {
		return err
	}

	s.id = id

	go func() {
		err := s.sessionMgr.RenewPeriodic(renewTTL, id, nil, s.doneCh)
		if s.lostLock != "" {
			err = LostLockError(s.lostLock)
		} else {
			err = convertError(err)
		}

		s.errCh <- err
	}()

	return err
}

func (s *Session) Recreate() (*Session, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	session, err := newSession(s.name, s.ttl, s.kv, s.sessionMgr)
	if err != nil {
		return nil, err
	}

	err = session.createOrRenewSession()
	if err != nil {
		return nil, err
	}

	if s.id != session.id {
		s.destroy()
	}

	return session, err
}

func (s *Session) AcquireLock(key string, value []byte) error {
	s.lock.Lock()
	err := s.createOrRenewSession()
	s.lock.Unlock()
	if err != nil {
		return err
	}

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
	s.lock.Lock()
	err := s.createOrRenewSession()
	s.lock.Unlock()
	if err != nil {
		return nil, err
	}

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
