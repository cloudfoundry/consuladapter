package consuladapter_test

import (
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
		sessionMgr = newFakeSessionManager(client)
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

			It("a session can be created", func() {
				Ω(newSessionErr).ShouldNot(HaveOccurred())
				Ω(session).ShouldNot(BeNil())
			})
		})

		It("creates a new session", func() {
			Ω(newSessionErr).ShouldNot(HaveOccurred())
			Ω(session).ShouldNot(BeNil())
		})

		Describe("Session#Recreate", func() {
			JustBeforeEach(func() {
				err := session.AcquireLock("foo", []byte{})
				Ω(err).ShouldNot(HaveOccurred())
			})

			It("destroys the current session if present", func() {
				_, err := session.Recreate()
				Ω(err).ShouldNot(HaveOccurred())

				Eventually(func() *api.SessionEntry {
					entries, _, err := client.Session().List(nil)
					Ω(err).ShouldNot(HaveOccurred())
					return findSession(session.ID(), entries)
				}).Should(BeNil())
			})

			It("creates a new session if not present", func() {
				session.Destroy()
				renewedSession, err := session.Recreate()
				Ω(err).ShouldNot(HaveOccurred())

				Eventually(func() *api.SessionEntry {
					entries, _, err := client.Session().List(nil)
					Ω(err).ShouldNot(HaveOccurred())
					return findSession(renewedSession.ID(), entries)
				}).ShouldNot(BeNil())
			})
		})

		Describe("Session#Destroy", func() {
			JustBeforeEach(func() {
				err := session.AcquireLock("a-key", []byte("a-value"))
				Ω(err).ShouldNot(HaveOccurred())

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
})

func findSession(sessionID string, entries []*api.SessionEntry) *api.SessionEntry {
	for _, e := range entries {
		if e.ID == sessionID {
			return e
		}
	}

	return nil
}
