package consuladapter_test

import (
	"time"

	"github.com/cloudfoundry-incubator/consuladapter"
	"github.com/hashicorp/consul/consul/structs"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const clusterStartingPort = 9000
const clusterSize = 3

var _ = Describe("Cluster Runner Integration", func() {
	var clusterRunner consuladapter.ClusterRunner
	var adapter consuladapter.Adapter

	BeforeEach(func() {
		clusterRunner = consuladapter.NewClusterRunner(clusterStartingPort, clusterSize)
		clusterRunner.Start()
		adapter = clusterRunner.NewAdapter()
	})

	AfterEach(func() {
		clusterRunner.Stop()
	})

	It("Provides mutual exclusion via locks", func() {
		By("one adapter acquiring the lock")
		_, err := adapter.AcquireAndMaintainLock("key", []byte("value"), structs.SessionTTLMin, make(chan struct{}))
		Ω(err).ShouldNot(HaveOccurred())

		By("another adapter trying to aquire the lock")
		otherAdapter := clusterRunner.NewAdapter()
		cancelChan := make(chan struct{})
		errChan := make(chan error)
		go func() {
			_, err = otherAdapter.AcquireAndMaintainLock("key", []byte("value"), structs.SessionTTLMin, cancelChan)
			if err != nil {
				errChan <- err
			}
		}()
		time.Sleep(1 * time.Second)
		cancelChan <- struct{}{}
		Eventually(errChan).Should(Receive(Equal(consuladapter.NewCancelledLockAttemptError("key"))))

		By("the other adapter trying to acquire the lock after the TTL")
		time.Sleep(structs.SessionTTLMin + time.Second)
		go func() {
			_, err = otherAdapter.AcquireAndMaintainLock("key", []byte("value"), structs.SessionTTLMin, cancelChan)
			if err != nil {
				errChan <- err
			}
		}()
		time.Sleep(1 * time.Second)
		cancelChan <- struct{}{}
		Eventually(errChan).Should(Receive(Equal(consuladapter.NewCancelledLockAttemptError("key"))))

		By("releasing the lock")
		err = adapter.ReleaseAndDeleteLock("key")
		Ω(err).ShouldNot(HaveOccurred())

		By("the second adapter acquiring the lock")
		_, err = otherAdapter.AcquireAndMaintainLock("key", []byte("value"), structs.SessionTTLMin, make(chan struct{}))
		Ω(err).ShouldNot(HaveOccurred())
	})

	It("Associates data with locks", func() {
		_, err := adapter.AcquireAndMaintainLock("key", []byte("value"), structs.SessionTTLMin, make(chan struct{}))
		Ω(err).ShouldNot(HaveOccurred())

		Consistently(func() ([]byte, error) {
			return adapter.GetValue("key")
		}).Should(Equal([]byte("value")))

		err = adapter.ReleaseAndDeleteLock("key")
		Ω(err).ShouldNot(HaveOccurred())

		Eventually(func() error {
			_, err := adapter.GetValue("key")
			return err
		}).Should(HaveOccurred())
	})

	It("Creates data, reads individual key data, and lists data extending given prefixes", func() {
		allChildren, err := adapter.ListPairsExtending("")
		Ω(err).ShouldNot(HaveOccurred())
		Ω(allChildren).Should(BeEmpty())

		err = adapter.SetValue("key", []byte("value"))
		Ω(err).ShouldNot(HaveOccurred())

		err = adapter.SetValue("nested", []byte("directory-metadata"))
		Ω(err).ShouldNot(HaveOccurred())

		err = adapter.SetValue("nested/key", []byte("nested-value"))
		Ω(err).ShouldNot(HaveOccurred())

		topKeyValue, err := adapter.GetValue("key")
		Ω(err).ShouldNot(HaveOccurred())
		Ω(topKeyValue).Should(Equal([]byte("value")))

		directoryValue, err := adapter.GetValue("nested")
		Ω(err).ShouldNot(HaveOccurred())
		Ω(directoryValue).Should(Equal([]byte("directory-metadata")))

		nestedKeyValue, err := adapter.GetValue("nested/key")
		Ω(err).ShouldNot(HaveOccurred())
		Ω(nestedKeyValue).Should(Equal([]byte("nested-value")))

		allChildren, err = adapter.ListPairsExtending("")
		Ω(err).ShouldNot(HaveOccurred())
		Ω(allChildren).Should(Equal(map[string][]byte{
			"key":        []byte("value"),
			"nested":     []byte("directory-metadata"),
			"nested/key": []byte("nested-value"),
		}))

		nestedChildren, err := adapter.ListPairsExtending("nested/")
		Ω(err).ShouldNot(HaveOccurred())
		Ω(nestedChildren).Should(Equal(map[string][]byte{
			"nested/key": []byte("nested-value"),
		}))
	})

	It("returns a KeyNotFound error when getting a non-existent key", func() {
		_, err := adapter.GetValue("not-present")
		Ω(err).Should(Equal(consuladapter.NewKeyNotFoundError("not-present")))
	})
})
