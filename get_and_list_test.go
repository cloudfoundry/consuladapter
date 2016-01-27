package consuladapter_test

import (
	"github.com/cloudfoundry-incubator/consuladapter"
	"github.com/hashicorp/consul/api"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Get and List Acquired Data", func() {
	BeforeEach(startClusterAndSession)
	AfterEach(stopClusterAndSession)

	Describe("Retrieving Locks and Presence", func() {
		const lockKey = "lock"
		var lockValue = []byte{'1'}

		const presenceKey = "presence"
		var presenceValue = []byte{'p'}

		var client *api.Client
		var logger *lagertest.TestLogger

		BeforeEach(func() {
			client = clusterRunner.NewClient()
			logger = lagertest.NewTestLogger("test")
		})

		Context("when a lock is present", func() {
			BeforeEach(func() {
				err := session.AcquireLock(lockKey, lockValue)
				Expect(err).NotTo(HaveOccurred())
			})

			It("retrieves the lock data", func() {
				val, err := session.GetAcquiredValue(lockKey)
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal(lockValue))
			})

			Context("when the session is destroyed", func() {
				BeforeEach(func() {
					session.Destroy()
				})

				It("eventually returns KeyNotFound", func() {
					otherAdapter := clusterRunner.NewSession("otherSession")
					keyNotFound := consuladapter.NewKeyNotFoundError(lockKey)
					Eventually(func() error {
						_, err := otherAdapter.GetAcquiredValue(lockKey)
						return err
					}).Should(Equal(keyNotFound))
				})
			})
		})

		Context("when presence is set", func() {
			BeforeEach(func() {
				_, err := session.SetPresence(presenceKey, presenceValue)
				Expect(err).NotTo(HaveOccurred())
			})

			It("retrieves the presence data", func() {
				val, err := session.GetAcquiredValue(presenceKey)
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal(presenceValue))
			})

			Context("when the session is destroyed", func() {
				BeforeEach(func() {
					session.Destroy()
				})

				It("eventually returns KeyNotFound", func() {
					otherAdapter := clusterRunner.NewSession("otherSession")
					keyNotFound := consuladapter.NewKeyNotFoundError(presenceKey)
					Eventually(func() error {
						_, err := otherAdapter.GetAcquiredValue(presenceKey)
						return err
					}).Should(Equal(keyNotFound))
				})
			})
		})

		Context("when a key is unowned", func() {
			const unowned = "unowned"

			BeforeEach(func() {
				_, err := client.KV().Put(&api.KVPair{Key: unowned}, nil)
				Expect(err).NotTo(HaveOccurred())
			})

			It("returns KeyNotFound", func() {
				_, err := session.GetAcquiredValue(unowned)
				Expect(err).To(Equal(consuladapter.NewKeyNotFoundError(unowned)))
			})
		})

		Context("when the key not present", func() {
			It("returns a KeyNotFound error", func() {
				_, err := session.GetAcquiredValue("not-present")
				Expect(err).To(Equal(consuladapter.NewKeyNotFoundError("not-present")))
			})
		})
	})
})
