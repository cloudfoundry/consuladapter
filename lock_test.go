package consuladapter_test

import (
	"time"

	"github.com/cloudfoundry-incubator/consuladapter"
	"github.com/hashicorp/consul/consul/structs"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Locking", func() {
	It("Provides mutual exclusion via locks", func() {
		By("One adapter acquiring the lock")
		_, err := adapter.AcquireAndMaintainLock("key", []byte("value"), structs.SessionTTLMin, make(chan struct{}))
		Ω(err).ShouldNot(HaveOccurred())

		By("Another adapter trying to aquire the lock")
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

		By("The other adapter trying to acquire the lock after the TTL")
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

		By("Releasing the lock")
		err = adapter.ReleaseAndDeleteLock("key")
		Ω(err).ShouldNot(HaveOccurred())

		By("The second adapter acquiring the lock")
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
})
