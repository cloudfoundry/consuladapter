package consuladapter_test

import (
	"errors"
	"net"
	"net/url"
	"time"

	"github.com/cloudfoundry-incubator/consuladapter"
	"github.com/cloudfoundry-incubator/consuladapter/fakes"
	"github.com/hashicorp/consul/api"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Locks and Presence", func() {
	var client *api.Client
	var sessionMgr *fakes.FakeSessionManager
	var sessionErr error

	BeforeEach(func() {
		startCluster()
		client = clusterRunner.NewClient()
		sessionMgr = newFakeSessionManager(client)
	})

	AfterEach(stopClusterAndSession)

	JustBeforeEach(func() {
		session, sessionErr = consuladapter.NewSession("a-session", 20*time.Second, client, sessionMgr)
	})

	AfterEach(func() {
		if session != nil {
			session.Destroy()
		}
	})

	sessionCreationTests := func(operationErr func() error) {

		It("is set with the expected defaults", func() {
			entries, _, err := client.Session().List(nil)
			Ω(err).ShouldNot(HaveOccurred())
			entry := findSession(session.ID(), entries)
			Ω(entry).ShouldNot(BeNil())
			Ω(entry.Name).Should(Equal("a-session"))
			Ω(entry.ID).Should(Equal(session.ID()))
			Ω(entry.Behavior).Should(Equal(api.SessionBehaviorDelete))
			Ω(entry.TTL).Should(Equal("20s"))
			Ω(entry.LockDelay).Should(BeZero())
		})

		It("renews the session periodically", func() {
			Eventually(sessionMgr.RenewPeriodicCallCount).ShouldNot(Equal(0))
		})

		Context("when NodeName() fails", func() {
			BeforeEach(func() {
				sessionMgr.NodeNameReturns("", errors.New("nodename failed"))
			})

			It("returns an error", func() {
				Ω(operationErr().Error()).Should(Equal("nodename failed"))
			})
		})

		Context("when retrieving the node sessions fail", func() {
			BeforeEach(func() {
				sessionMgr.NodeReturns(nil, nil, errors.New("session list failed"))
			})

			It("returns an error", func() {
				Ω(operationErr().Error()).Should(Equal("session list failed"))
			})
		})

		Context("when Create fails", func() {
			BeforeEach(func() {
				sessionMgr.CreateReturns("", nil, errors.New("create failed"))
			})

			It("returns an error", func() {
				Ω(operationErr()).Should(HaveOccurred())
				Ω(operationErr().Error()).Should(Equal("create failed"))
			})
		})
	}

	Describe("Session#AcquireLock", func() {
		const lockKey = "lockme"
		var lockValue = []byte{'1'}
		var lockErr error

		Context("when the store is up", func() {
			JustBeforeEach(func() {
				lockErr = session.AcquireLock(lockKey, lockValue)
			})

			sessionCreationTests(func() error { return lockErr })

			It("creates acquired key/value", func() {
				Ω(lockErr).ShouldNot(HaveOccurred())

				kvpair, _, err := client.KV().Get(lockKey, nil)
				Ω(err).ShouldNot(HaveOccurred())
				Ω(kvpair.Session).Should(Equal(session.ID()))
				Ω(kvpair.Value).Should(Equal(lockValue))
			})

			It("destroys the session when the lock is lost", func() {
				ok, _, err := client.KV().Release(&api.KVPair{Key: lockKey, Session: session.ID()}, nil)
				Ω(err).ShouldNot(HaveOccurred())
				Ω(ok).Should(BeTrue())

				Eventually(session.Err()).Should(Receive(Equal(consuladapter.LostLockError(lockKey))))
			})

			Context("when recreating the Session", func() {
				var newSession *consuladapter.Session

				JustBeforeEach(func() {
					var err error
					newSession, err = session.Recreate()
					Ω(err).ShouldNot(HaveOccurred())
				})

				AfterEach(func() {
					newSession.Destroy()
				})

				It("creates a new session", func() {
					Ω(newSession.ID()).ShouldNot(Equal(session.ID()))
				})

				It("can contend for the Lock", func() {
					errCh := make(chan error, 1)
					go func() {
						errCh <- newSession.AcquireLock(lockKey, lockValue)
					}()

					Eventually(errCh).Should(Receive(BeNil()))
				})
			})

			Context("with another session", func() {
				It("acquires the lock when released", func() {
					bsession, err := consuladapter.NewSession("b-session", 20*time.Second, client, sessionMgr)
					Ω(err).ShouldNot(HaveOccurred())
					defer bsession.Destroy()

					errChan := make(chan error, 1)
					go func() {
						defer GinkgoRecover()
						errChan <- bsession.AcquireLock(lockKey, lockValue)
					}()

					Consistently(errChan).ShouldNot(Receive())

					session.Destroy()

					Eventually(errChan, api.DefaultLockRetryTime*2).Should(Receive(BeNil()))
					kvpair, _, err := client.KV().Get(lockKey, nil)
					Ω(err).ShouldNot(HaveOccurred())
					Ω(kvpair.Session).Should(Equal(bsession.ID()))
				})
			})

			Context("and the store goes down", func() {
				JustBeforeEach(stopCluster)

				It("loses the lock", func() {
					Eventually(session.Err()).Should(Receive(Equal(consuladapter.LostLockError(lockKey))))
				})

				Context("when acquiring a lock", func() {
					It("fails", func() {
						bsession, err := consuladapter.NewSession("b-session", 20*time.Second, client, sessionMgr)
						Ω(err).ShouldNot(HaveOccurred())

						err = bsession.AcquireLock(lockKey, lockValue)
						Ω(err).Should(HaveOccurred())
					})
				})
			})

			Context("and consul goes down during a renew", func() {
				BeforeEach(func() {
					oldStub := sessionMgr.RenewPeriodicStub
					sessionMgr.RenewPeriodicStub = func(initialTTL string, id string, q *api.WriteOptions, doneCh chan struct{}) error {
						stopCluster()
						return oldStub("1s", id, q, doneCh)
					}
				})

				It("reports an error", func() {
					var err error
					Eventually(session.Err()).Should(Receive(&err))

					// a race between 2 possibilities
					if urlErr, ok := err.(*url.Error); ok {
						Ω(ok).Should(BeTrue())
						opErr, ok := urlErr.Err.(*net.OpError)
						Ω(ok).Should(BeTrue())
						Ω(opErr.Op).Should(Equal("dial"))
					} else {
						Ω(err).Should(Equal(consuladapter.LostLockError(lockKey)))
					}
				})
			})
		})
	})

	Describe("Session#SetPresence", func() {
		const presenceKey = "presenceme"
		var presenceValue = []byte{'1'}
		var presenceLost <-chan string
		var presenceErr error

		JustBeforeEach(func() {
			presenceLost, presenceErr = session.SetPresence(presenceKey, presenceValue)
		})

		sessionCreationTests(func() error { return presenceErr })

		It("creates an acquired key/value", func() {
			Ω(presenceErr).ShouldNot(HaveOccurred())

			kvpair, _, err := client.KV().Get(presenceKey, nil)
			Ω(err).ShouldNot(HaveOccurred())
			Ω(kvpair.Session).Should(Equal(session.ID()))
			Ω(kvpair.Value).Should(Equal(presenceValue))
		})

		It("the session remains when the presence is lost", func() {
			ok, _, err := client.KV().Release(&api.KVPair{Key: presenceKey, Session: session.ID()}, nil)
			Ω(err).ShouldNot(HaveOccurred())
			Ω(ok).Should(BeTrue())

			Consistently(session.Err()).ShouldNot(Receive())
			Eventually(presenceLost).Should(Receive(Equal(presenceKey)))
		})

		Context("with another session", func() {
			It("acquires the lock when released", func() {
				bsession, err := consuladapter.NewSession("b-session", 20*time.Second, client, sessionMgr)
				Ω(err).ShouldNot(HaveOccurred())
				defer bsession.Destroy()

				errChan := make(chan error, 1)
				go func() {
					defer GinkgoRecover()
					_, err := bsession.SetPresence(presenceKey, presenceValue)
					errChan <- err
				}()

				Consistently(errChan).ShouldNot(Receive())

				session.Destroy()

				Eventually(errChan, api.DefaultLockRetryTime*2).Should(Receive(BeNil()))
				kvpair, _, err := client.KV().Get(presenceKey, nil)
				Ω(err).ShouldNot(HaveOccurred())
				Ω(kvpair.Session).Should(Equal(bsession.ID()))
			})
		})

		Context("and consul goes down", func() {
			JustBeforeEach(stopCluster)

			It("loses its presence", func() {
				Eventually(presenceLost).Should(Receive(Equal(presenceKey)))
			})

			Context("when setting presence", func() {
				It("fails", func() {
					bsession, err := consuladapter.NewSession("b-session", 20*time.Second, client, sessionMgr)
					Ω(err).ShouldNot(HaveOccurred())

					_, err = bsession.SetPresence(presenceKey, presenceValue)
					Ω(err).Should(HaveOccurred())
				})
			})
		})

		Context("and consul goes down during a renew", func() {
			BeforeEach(func() {
				oldStub := sessionMgr.RenewPeriodicStub
				sessionMgr.RenewPeriodicStub = func(initialTTL string, id string, q *api.WriteOptions, doneCh chan struct{}) error {
					stopCluster()
					return oldStub("1s", id, q, doneCh)
				}
			})

			It("reports an error", func() {
				var err error
				Eventually(session.Err()).Should(Receive(&err))
				urlErr, ok := err.(*url.Error)
				Ω(ok).Should(BeTrue())
				opErr, ok := urlErr.Err.(*net.OpError)
				Ω(ok).Should(BeTrue())
				Ω(opErr.Op).Should(Equal("dial"))
			})
		})
	})
})
