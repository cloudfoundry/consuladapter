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
	"github.com/pivotal-golang/lager/lagertest"
)

var _ = Describe("Session", func() {
	BeforeEach(startCluster)
	AfterEach(stopCluster)

	var client *api.Client
	var sessionMgr *fakes.FakeSessionManager
	var session *consuladapter.Session
	var newSessionErr error
	var logger *lagertest.TestLogger

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test")

		client = clusterRunner.NewClient()
		sessionMgr = &fakes.FakeSessionManager{}
		realSM := consuladapter.NewSessionManager(client)
		sessionMgr.NodeNameStub = realSM.NodeName
		sessionMgr.NodeStub = realSM.Node
		sessionMgr.CreateStub = realSM.Create
		sessionMgr.DestroyStub = realSM.Destroy
		sessionMgr.RenewStub = realSM.Renew
		sessionMgr.RenewPeriodicStub = realSM.RenewPeriodic
		sessionMgr.NewLockStub = realSM.NewLock
	})

	JustBeforeEach(func() {
		session, newSessionErr = consuladapter.NewSession("a-session", 20*time.Second, client, sessionMgr)
	})

	AfterEach(func() {
		if session != nil {
			session.Destroy()
		}
	})

	Describe("NewSession", func() {
		Context("when consul is down", func() {
			BeforeEach(stopCluster)

			It("fails to create a session", func() {
				_, err := consuladapter.NewSession("a-session", 20*time.Second, client, sessionMgr)
				Ω(err).Should(HaveOccurred())
			})
		})

		It("creates a new session", func() {
			Ω(newSessionErr).ShouldNot(HaveOccurred())
			Ω(session).ShouldNot(BeNil())
		})

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

		Context("and consul goes down", func() {
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

		Context("when NodeName() fails", func() {
			BeforeEach(func() {
				sessionMgr.NodeNameStub = nil
				sessionMgr.NodeNameReturns("", errors.New("nodename failed"))
			})

			It("returns an error", func() {
				Ω(newSessionErr.Error()).Should(Equal("nodename failed"))
			})
		})

		Context("when retrieving the node sessions fail", func() {
			BeforeEach(func() {
				sessionMgr.NodeStub = nil
				sessionMgr.NodeReturns(nil, nil, errors.New("session list failed"))
			})

			It("returns an error", func() {
				Ω(newSessionErr.Error()).Should(Equal("session list failed"))
			})
		})

		Context("when the session already exists", func() {
			var session2 *consuladapter.Session
			var err2 error

			JustBeforeEach(func() {
				session2, err2 = consuladapter.NewSession("a-session", 30*time.Second, client, sessionMgr)
			})

			AfterEach(func() {
				session2.Destroy()
			})

			It("renews the existing session", func() {
				Ω(sessionMgr.CreateCallCount()).Should(Equal(1))
				Ω(err2).ShouldNot(HaveOccurred())
				Ω(session2).ShouldNot(BeNil())

				entries, _, err := client.Session().List(nil)
				Ω(err).ShouldNot(HaveOccurred())
				entry := findSession(session2.ID(), entries)
				Ω(entry.TTL).Should(Equal("20s"))
				Ω(entry.Name).Should(Equal("a-session"))
				Ω(entry.ID).Should(Equal(session2.ID()))
			})

			It("renews the session periodically", func() {
				Eventually(sessionMgr.RenewPeriodicCallCount).ShouldNot(Equal(0))
			})

			Context("when renew fails", func() {
				BeforeEach(func() {
					oldStub := sessionMgr.RenewStub
					sessionMgr.RenewStub = func(id string, q *api.WriteOptions) (*api.SessionEntry, *api.WriteMeta, error) {
						return oldStub("bad id", q)
					}
				})

				It("creates a new session", func() {
					Ω(sessionMgr.CreateCallCount()).Should(Equal(2))
					Ω(newSessionErr).ShouldNot(HaveOccurred())
					Ω(session).ShouldNot(BeNil())
				})
			})
		})

		Context("when Create fails", func() {
			BeforeEach(func() {
				sessionMgr.CreateStub = nil
				sessionMgr.CreateReturns("", nil, errors.New("create failed"))
			})

			It("returns an error", func() {
				Ω(newSessionErr.Error()).Should(Equal("create failed"))
			})
		})
	})

	Describe("Session#Renew", func() {
		var renewedSession *consuladapter.Session

		JustBeforeEach(func() {
			var err error
			renewedSession, err = session.Renew()
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("destroys the current session", func() {
			Eventually(func() bool {
				entries, _, err := client.Session().List(nil)
				Ω(err).ShouldNot(HaveOccurred())
				return findSession(session.ID(), entries) == nil
			}).Should(BeTrue())
		})

		It("creates a new session", func() {
			Eventually(func() bool {
				entries, _, err := client.Session().List(nil)
				Ω(err).ShouldNot(HaveOccurred())

				return findSession(renewedSession.ID(), entries) != nil
			}).Should(BeTrue())
		})

		It("fails if called again", func() {
			_, err := session.Renew()
			Ω(err).Should(Equal(consuladapter.ErrAlreadyRenewed))
		})

		Context("when renew fails", func() {
			JustBeforeEach(func() {
				sessionMgr.CreateReturns("", nil, errors.New("create failed"))
				sessionMgr.RenewReturns(nil, nil, errors.New("renew failed"))
			})

			It("allows renew to be called again", func() {
				_, err := renewedSession.Renew()
				Ω(err.Error()).Should(Equal("create failed"))

				_, err = renewedSession.Renew()
				Ω(err.Error()).Should(Equal("create failed"))
			})
		})
	})

	Describe("Session#Destroy", func() {
		JustBeforeEach(func() {
			Eventually(func() []*api.SessionEntry {
				entries, _, err := client.Session().List(nil)
				Ω(err).ShouldNot(HaveOccurred())
				return entries
			}).Should(HaveLen(1))

			session.Destroy()
		})

		It("destroys the session", func() {
			Ω(sessionMgr.DestroyCallCount()).Should(Equal(1))
			id, _ := sessionMgr.DestroyArgsForCall(0)
			Ω(id).Should(Equal(session.ID()))
		})

		It("removes the session", func() {
			Eventually(func() *api.SessionEntry {
				entries, _, err := client.Session().List(nil)
				Ω(err).ShouldNot(HaveOccurred())
				return findSession(session.ID(), entries)
			}).Should(BeNil())
		})

		It("sends a nil error", func() {
			Eventually(session.Err()).Should(Receive(BeNil()))
		})

		It("can be called multiple times", func() {
			session.Destroy()
		})
	})
})

func findSession(sessionID string, entries []*api.SessionEntry) *api.SessionEntry {
	for _, e := range entries {
		if e.ID == sessionID {
			return e
		}
	}

	return nil
}
