package consuladapter_test

import (
	"errors"
	"time"

	"github.com/cloudfoundry-incubator/consuladapter"
	"github.com/hashicorp/consul/api"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Locks and Presence", func() {
	var client *api.Client
	var sessionMgr consuladapter.SessionManager

	BeforeEach(func() {
		startClusterAndSession()
		client = clusterRunner.NewClient()
		sessionMgr = consuladapter.NewSessionManager(client)
	})

	AfterEach(stopClusterAndSession)

	Describe("Session#AcquireLock", func() {
		const lockKey = "lockme"
		var lockValue = []byte{'1'}
		var lockErr error

		Context("when the store is up", func() {

			JustBeforeEach(func() {
				lockErr = session.AcquireLock(lockKey, lockValue)
			})

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
			})
		})

		Context("when the store is down", func() {
			Context("acquiring a lock", func() {
				It("fails", func() {
					bsession, err := consuladapter.NewSession("b-session", 20*time.Second, client, sessionMgr)
					Ω(err).ShouldNot(HaveOccurred())
					defer bsession.Destroy()

					errChan := make(chan error, 1)
					go func() {
						defer GinkgoRecover()
						errChan <- bsession.AcquireLock(lockKey, lockValue)
					}()

					stopCluster()
					Eventually(func() error {
						select {
						case err := <-bsession.Err():
							return err
						case err := <-errChan:
							return err
						}
					}).ShouldNot(BeNil())
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

		Context("and the store goes down", func() {
			JustBeforeEach(stopCluster)

			It("loses its presence", func() {
				Eventually(presenceLost).Should(Receive(Equal(presenceKey)))
			})
		})

		Context("when the store is down", func() {
			Context("acquiring a lock", func() {
				It("fails", func() {
					bsession, err := consuladapter.NewSession("b-session", 20*time.Second, client, sessionMgr)
					Ω(err).ShouldNot(HaveOccurred())
					defer bsession.Destroy()

					errChan := make(chan error, 1)
					go func() {
						defer GinkgoRecover()
						lostPresence, err := bsession.SetPresence(presenceKey, presenceValue)
						if err == nil {
							Eventually(lostPresence).Should(Receive(Equal(presenceKey)))
							err = errors.New("lost presence")
						}
						errChan <- err
					}()

					stopCluster()

					Eventually(errChan).Should(Receive(HaveOccurred()))
				})
			})
		})
	})
})
