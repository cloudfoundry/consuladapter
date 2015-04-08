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
				Ω(err).ShouldNot(HaveOccurred())
			})

			It("retrieves the lock data", func() {
				val, err := session.GetAcquiredValue(lockKey)
				Ω(err).ShouldNot(HaveOccurred())
				Ω(val).Should(Equal(lockValue))
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
				Ω(err).ShouldNot(HaveOccurred())
			})

			It("retrieves the presence data", func() {
				val, err := session.GetAcquiredValue(presenceKey)
				Ω(err).ShouldNot(HaveOccurred())
				Ω(val).Should(Equal(presenceValue))
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
				Ω(err).ShouldNot(HaveOccurred())
			})

			It("returns KeyNotFound", func() {
				_, err := session.GetAcquiredValue(unowned)
				Ω(err).Should(Equal(consuladapter.NewKeyNotFoundError(unowned)))
			})
		})

		Context("when the key not present", func() {
			It("returns a KeyNotFound error", func() {
				_, err := session.GetAcquiredValue("not-present")
				Ω(err).Should(Equal(consuladapter.NewKeyNotFoundError("not-present")))
			})
		})
	})

	Describe("Listing locks and presence", func() {
		const lockKey = "under/lock"
		var lockValue = []byte{'1'}

		const presenceKey = "under/presence"
		var presenceValue = []byte{'p'}

		const unowned = "under/unowned"

		var client *api.Client

		BeforeEach(func() {
			client = clusterRunner.NewClient()

			err := session.AcquireLock(lockKey, lockValue)
			Ω(err).ShouldNot(HaveOccurred())

			_, err = session.SetPresence(presenceKey, presenceValue)
			Ω(err).ShouldNot(HaveOccurred())

			_, err = client.KV().Put(&api.KVPair{Key: unowned}, nil)
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("list acquired keys", func() {
			data, err := session.ListAcquiredValues("under")
			Ω(err).ShouldNot(HaveOccurred())
			Ω(data).Should(HaveLen(2))
			Ω(data[lockKey]).Should(Equal(lockValue))
			Ω(data[presenceKey]).Should(Equal(presenceValue))
		})

		Context("when the session is destroyed", func() {
			BeforeEach(func() {
				session.Destroy()
			})

			It("eventually returns no data", func() {
				otherAdapter := clusterRunner.NewSession("otherSession")
				Eventually(func() map[string][]byte {
					pairs, err := otherAdapter.ListAcquiredValues("under")
					Ω(err).ShouldNot(HaveOccurred())
					return pairs
				}).Should(Equal(map[string][]byte{}))
			})
		})

		Context("when no keys are present", func() {
			It("returns an empty set", func() {
				_, err := session.ListAcquiredValues("not-present")
				Ω(err).Should(Equal(consuladapter.NewPrefixNotFoundError("not-present")))
			})

			Context("when the prefix is present", func() {
				BeforeEach(func() {
					_, err := client.KV().Put(&api.KVPair{Key: unowned, Value: []byte{}}, nil)
					Ω(err).ShouldNot(HaveOccurred())
				})

				It("returns an empty result", func() {
					pairs, err := session.ListAcquiredValues(unowned)
					Ω(err).ShouldNot(HaveOccurred())
					Ω(pairs).Should(Equal(map[string][]byte{}))
				})
			})
		})
	})
})
