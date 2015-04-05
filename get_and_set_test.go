package consuladapter_test

import (
	"github.com/cloudfoundry-incubator/cf_http"
	"github.com/cloudfoundry-incubator/consuladapter"
	"github.com/hashicorp/consul/api"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Setting and Getting Data", func() {
	BeforeEach(startClusterAndAdapter)
	AfterEach(stopCluster)

	It("Returns a KeyNotFound error when getting a non-existent key", func() {
		_, err := adapter.GetValue("not-present")
		Ω(err).Should(Equal(consuladapter.NewKeyNotFoundError("not-present")))
	})

	Describe("Retrieving KVs and Locks", func() {
		var client *api.Client
		var lock *api.Lock

		BeforeEach(func() {
			var err error
			client, err = api.NewClient(&api.Config{
				Address:    clusterRunner.Addresses()[0],
				Scheme:     "http",
				HttpClient: cf_http.NewStreamingClient(),
			})
			Ω(err).ShouldNot(HaveOccurred())

			lock, err = client.LockKey("a/b")
			Ω(err).ShouldNot(HaveOccurred())

			stopCh := make(chan struct{})
			_, err = lock.Lock(stopCh)
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("retrieves acquired values", func() {
			bytes, err := adapter.GetValue("a/b")
			Ω(err).ShouldNot(HaveOccurred())
			Ω(bytes).Should(BeZero())
		})

		It("returns all acquired/locked values", func() {
			allChildren, err := adapter.ListPairsExtending("a/")
			Ω(err).ShouldNot(HaveOccurred())
			Ω(allChildren).Should(HaveLen(1))
			Ω(allChildren).Should(HaveKey("a/b"))
		})

		Context("when unlocked", func() {
			BeforeEach(func() {
				err := lock.Unlock()
				Ω(err).ShouldNot(HaveOccurred())
			})

			It("retrieves nothing", func() {
				_, err := adapter.GetValue("a/b")
				Ω(err).Should(Equal(consuladapter.NewKeyNotFoundError("a/b")))
			})

			It("returns nothing", func() {
				allChildren, err := adapter.ListPairsExtending("a/")
				Ω(err).ShouldNot(HaveOccurred())
				Ω(allChildren).Should(HaveLen(0))
			})

			Context("when destroyed", func() {
				BeforeEach(func() {
					err := lock.Destroy()
					Ω(err).ShouldNot(HaveOccurred())
				})

				It("retrieves nothing", func() {
					_, err := adapter.GetValue("a/b")
					Ω(err).Should(Equal(consuladapter.NewKeyNotFoundError("a/b")))
				})

				It("returns nothing", func() {
					allChildren, err := adapter.ListPairsExtending("a/")
					Ω(err).ShouldNot(HaveOccurred())
					Ω(allChildren).Should(HaveLen(0))
				})
			})
		})
	})
})
