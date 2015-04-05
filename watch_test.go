package consuladapter_test

import (
	"github.com/cloudfoundry-incubator/cf_http"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/consul/structs"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"time"
)

var _ = Describe("Watching", func() {
	var client *api.Client

	var disappearChan <-chan []string
	var cancelChan chan<- struct{}
	var errChan <-chan error

	BeforeEach(startClusterAndAdapter)
	AfterEach(stopCluster)

	BeforeEach(func() {
		var err error
		client, err = api.NewClient(&api.Config{
			Address:    clusterRunner.Addresses()[0],
			Scheme:     "http",
			HttpClient: cf_http.NewStreamingClient(),
		})
		Ω(err).ShouldNot(HaveOccurred())

		disappearChan, cancelChan, errChan = adapter.WatchForDisappearancesUnder("under")
	})

	Context("when there are keys", func() {
		BeforeEach(func() {
			Eventually(func() error {
				_, _, err := client.KV().Get("under", nil)
				return err
			}, 1, 50*time.Millisecond).ShouldNot(HaveOccurred())

			_, err := adapter.AcquireAndMaintainLock("under/here", []byte("value"), structs.SessionTTLMin, nil)
			Ω(err).ShouldNot(HaveOccurred())
		})

		AfterEach(func() {
			_ = adapter.ReleaseAndDeleteLock("under/here")
		})

		It("detects removals of keys", func() {
			err := adapter.ReleaseAndDeleteLock("under/here")
			Ω(err).ShouldNot(HaveOccurred())

			Eventually(disappearChan).Should(Receive(Equal([]string{"under/here"})))
		})

		Context("with other prefixes", func() {
			BeforeEach(func() {
				_, err := adapter.AcquireAndMaintainLock("other", []byte("value"), structs.SessionTTLMin, nil)
				Ω(err).ShouldNot(HaveOccurred())
			})

			It("does not detect removal of keys under other prefixes", func() {
				err := adapter.ReleaseAndDeleteLock("other")
				Ω(err).ShouldNot(HaveOccurred())
				Consistently(disappearChan).ShouldNot(Receive())
			})
		})

		Context("when cancelling", func() {
			It("does not detect removals", func() {
				close(cancelChan)
				Eventually(disappearChan).Should(BeClosed())
			})
		})

		Context("when an error occurs", func() {
			It("reports an error", func() {
				clusterRunner.Stop()
				Eventually(errChan).Should(Receive())
				Eventually(errChan).Should(BeClosed())
				Eventually(disappearChan).Should(BeClosed())
			})
		})
	})

	Context("when a lock is locked", func() {
		var lock *api.Lock

		BeforeEach(func() {
			var err error
			lock, err = client.LockOpts(&api.LockOptions{
				Key:   "under/there",
				Value: []byte("there"),
			})
			Ω(err).ShouldNot(HaveOccurred())

			stopCh := make(chan struct{})
			_, err = lock.Lock(stopCh)
			Ω(err).ShouldNot(HaveOccurred())

			v, err := adapter.GetValue("under/there")
			Ω(err).ShouldNot(HaveOccurred())
			Ω(v).ShouldNot(BeNil())
		})

		It("does not notice", func() {
			Consistently(disappearChan).ShouldNot(Receive())
		})

		Context("when its unlocked", func() {
			BeforeEach(func() {
				err := lock.Unlock()
				Ω(err).ShouldNot(HaveOccurred())
			})

			It("detects the disappearance", func() {
				Eventually(disappearChan).Should(Receive(Equal([]string{"under/there"})))
			})
		})
	})

	Context("when there are no keys", func() {
		It("will not receive PrefixNotFound error", func() {
			Consistently(errChan).ShouldNot(Receive())
		})
	})
})
