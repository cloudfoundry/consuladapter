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
	var fakeAgent *fakes.FakeAgent
	var fakeClient *fakes.FakeClient
	var fakeSession *fakes.FakeISession
	var sessionErr error
	var noChecks bool

	BeforeEach(func() {
		startCluster()
		client := clusterRunner.NewClient()
		fakeClient, fakeAgent, _, fakeSession = newFakeClient(client)
		noChecks = false
	})

	AfterEach(stopClusterAndSession)

	JustBeforeEach(func() {
		if noChecks {
			session, sessionErr = consuladapter.NewSessionNoChecks("a-session", 20*time.Second, fakeClient)
		} else {
			session, sessionErr = consuladapter.NewSession("a-session", 20*time.Second, fakeClient)
		}
	})

	AfterEach(func() {
		if session != nil {
			session.Destroy()
		}
	})

	sessionCreationTests := func(operationErr func() error) {

		It("is set with the expected defaults", func() {
			entries, _, err := fakeClient.Session().List(nil)
			Expect(err).NotTo(HaveOccurred())
			entry := findSession(session.ID(), entries)
			Expect(entry).NotTo(BeNil())
			Expect(entry.Name).To(Equal("a-session"))
			Expect(entry.ID).To(Equal(session.ID()))
			Expect(entry.Behavior).To(Equal(api.SessionBehaviorDelete))
			Expect(entry.TTL).To(Equal("20s"))
			Expect(entry.LockDelay).To(BeZero())
		})

		It("renews the session periodically", func() {
			Eventually(fakeSession.RenewPeriodicCallCount).ShouldNot(Equal(0))
		})

		Context("when NodeName() fails", func() {
			BeforeEach(func() {
				fakeAgent.NodeNameReturns("", errors.New("nodename failed"))
			})

			It("returns an error", func() {
				Expect(operationErr()).To(MatchError("nodename failed"))
			})
		})

		Context("when retrieving the node sessions fail", func() {
			BeforeEach(func() {
				fakeSession.NodeReturns(nil, nil, errors.New("session list failed"))
			})

			It("returns an error", func() {
				Expect(operationErr()).To(MatchError("session list failed"))
			})
		})

		Context("when Create fails", func() {
			BeforeEach(func() {
				fakeSession.CreateReturns("", nil, errors.New("create failed"))
				fakeSession.CreateNoChecksReturns("", nil, errors.New("create failed"))
			})

			It("returns an error", func() {
				Expect(operationErr()).To(MatchError("create failed"))
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
				Expect(lockErr).NotTo(HaveOccurred())

				kvpair, _, err := fakeClient.KV().Get(lockKey, nil)
				Expect(err).NotTo(HaveOccurred())
				Expect(kvpair.Session).To(Equal(session.ID()))
				Expect(kvpair.Value).To(Equal(lockValue))
			})

			It("destroys the session when the lock is lost", func() {
				ok, _, err := fakeClient.KV().Release(&api.KVPair{Key: lockKey, Session: session.ID()}, nil)
				Expect(err).NotTo(HaveOccurred())
				Expect(ok).To(BeTrue())

				Eventually(session.Err()).Should(Receive(Equal(consuladapter.LostLockError(lockKey))))
			})

			Context("when Lock() is stopped (session is destroyed)", func() {
				BeforeEach(func() {
					lock := &fakes.FakeLock{}
					lock.LockReturns(nil, nil)
					fakeClient.LockOptsReturns(lock, nil)
				})

				It("returns an error", func() {
					Expect(lockErr).To(Equal(consuladapter.ErrCancelled))
				})
			})

			Context("when recreating the Session", func() {
				var newSession *consuladapter.Session

				JustBeforeEach(func() {
					var err error
					newSession, err = session.Recreate()
					Expect(err).NotTo(HaveOccurred())
				})

				AfterEach(func() {
					newSession.Destroy()
				})

				It("creates a new session", func() {
					Expect(newSession.ID()).NotTo(Equal(session.ID()))
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
					bsession, err := consuladapter.NewSession("b-session", 20*time.Second, fakeClient)
					Expect(err).NotTo(HaveOccurred())
					defer bsession.Destroy()

					errChan := make(chan error, 1)
					go func() {
						defer GinkgoRecover()
						errChan <- bsession.AcquireLock(lockKey, lockValue)
					}()

					Consistently(errChan).ShouldNot(Receive())

					session.Destroy()

					Eventually(errChan, api.DefaultLockRetryTime*2).Should(Receive(BeNil()))
					kvpair, _, err := fakeClient.KV().Get(lockKey, nil)
					Expect(err).NotTo(HaveOccurred())
					Expect(kvpair.Session).To(Equal(bsession.ID()))
				})
			})

			Context("and the store goes down", func() {
				JustBeforeEach(stopCluster)

				It("loses the lock", func() {
					Eventually(session.Err()).Should(Receive(Equal(consuladapter.LostLockError(lockKey))))
				})

				Context("when acquiring a lock", func() {
					It("fails", func() {
						bsession, err := consuladapter.NewSession("b-session", 20*time.Second, fakeClient)
						Expect(err).NotTo(HaveOccurred())

						err = bsession.AcquireLock(lockKey, lockValue)
						Expect(err).To(HaveOccurred())
					})
				})
			})

			Context("and consul goes down during a renew", func() {
				BeforeEach(func() {
					oldStub := fakeSession.RenewPeriodicStub
					fakeSession.RenewPeriodicStub = func(initialTTL string, id string, q *api.WriteOptions, doneCh chan struct{}) error {
						stopCluster()
						return oldStub("1s", id, q, doneCh)
					}
				})

				It("reports an error", func() {
					var err error
					Eventually(session.Err(), 3*time.Second).Should(Receive(&err))

					// a race between 2 possibilities
					if urlErr, ok := err.(*url.Error); ok {
						Expect(ok).To(BeTrue())
						opErr, ok := urlErr.Err.(*net.OpError)
						Expect(ok).To(BeTrue())
						Expect(opErr.Op).To(Equal("dial"))
					} else {
						Expect(err).To(Equal(consuladapter.LostLockError(lockKey)))
					}
				})

				It("destroys the session", func() {
					var err error
					Eventually(session.Err(), 2*time.Second).Should(Receive(&err))

					acquireErr := make(chan error, 1)
					go func() {
						acquireErr <- session.AcquireLock(lockKey, lockValue)
					}()

					Eventually(acquireErr).Should(Receive(Equal(consuladapter.ErrDestroyed)))
				})
			})
		})
	})

	Describe("Session#SetPresence", func() {
		const presenceKey = "presenceme"
		var presenceValue = []byte{'1'}
		var presenceLost <-chan string
		var presenceErr error

		BeforeEach(func() {
			noChecks = true
		})

		JustBeforeEach(func() {
			presenceLost, presenceErr = session.SetPresence(presenceKey, presenceValue)
		})

		sessionCreationTests(func() error { return presenceErr })

		It("creates an acquired key/value", func() {
			Expect(presenceErr).NotTo(HaveOccurred())

			kvpair, _, err := fakeClient.KV().Get(presenceKey, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(kvpair.Session).To(Equal(session.ID()))
			Expect(kvpair.Value).To(Equal(presenceValue))
		})

		It("the session remains when the presence is lost", func() {
			ok, _, err := fakeClient.KV().Release(&api.KVPair{Key: presenceKey, Session: session.ID()}, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())

			Consistently(session.Err()).ShouldNot(Receive())
			Eventually(presenceLost).Should(Receive(Equal(presenceKey)))
		})

		Context("when Lock() is stopped (session is destroyed)", func() {
			BeforeEach(func() {
				lock := &fakes.FakeLock{}
				lock.LockReturns(nil, nil)
				fakeClient.LockOptsReturns(lock, nil)
			})

			It("returns an error", func() {
				Eventually(fakeClient.LockOptsCallCount).Should(Equal(1))
				Expect(presenceErr).To(Equal(consuladapter.ErrCancelled))
			})
		})

		Context("with another session", func() {
			It("acquires the lock when released", func() {
				bsession, err := consuladapter.NewSession("b-session", 20*time.Second, fakeClient)
				Expect(err).NotTo(HaveOccurred())
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
				kvpair, _, err := fakeClient.KV().Get(presenceKey, nil)
				Expect(err).NotTo(HaveOccurred())
				Expect(kvpair.Session).To(Equal(bsession.ID()))
			})
		})

		Context("and consul goes down", func() {
			JustBeforeEach(stopCluster)

			It("loses its presence", func() {
				Eventually(presenceLost).Should(Receive(Equal(presenceKey)))
			})

			Context("when setting presence", func() {
				It("fails", func() {
					bsession, err := consuladapter.NewSession("b-session", 20*time.Second, fakeClient)
					Expect(err).NotTo(HaveOccurred())

					_, err = bsession.SetPresence(presenceKey, presenceValue)
					Expect(err).To(HaveOccurred())
				})
			})
		})

		Context("and consul goes down during a renew", func() {
			BeforeEach(func() {
				oldStub := fakeSession.RenewPeriodicStub
				fakeSession.RenewPeriodicStub = func(initialTTL string, id string, q *api.WriteOptions, doneCh chan struct{}) error {
					stopCluster()
					return oldStub("500ms", id, q, doneCh)
				}
			})

			It("reports an error", func() {
				var err error
				Eventually(session.Err(), 3*time.Second).Should(Receive(&err))
				urlErr, ok := err.(*url.Error)
				Expect(ok).To(BeTrue())
				opErr, ok := urlErr.Err.(*net.OpError)
				Expect(ok).To(BeTrue())
				Expect(opErr.Op).To(Equal("dial"))
			})
		})
	})
})
