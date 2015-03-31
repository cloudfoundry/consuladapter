package consuladapter_test

import (
	"github.com/cloudfoundry-incubator/consuladapter"
	"github.com/hashicorp/consul/consul/structs"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Watching", func() {
	var disappearChan <-chan []string
	var cancelChan chan<- struct{}
	var errChan <-chan error

	BeforeEach(startClusterAndAdapter)
	AfterEach(stopCluster)

	JustBeforeEach(func() {
		disappearChan, cancelChan, errChan = adapter.WatchForDisappearancesUnder("under")
	})

	Context("when there are keys", func() {
		BeforeEach(func() {
			Eventually(func() error {
				_, err := adapter.AcquireAndMaintainLock("under/here", []byte("value"), structs.SessionTTLMin, nil)
				return err
			}).ShouldNot(HaveOccurred())

			Eventually(func() []byte {
				v, _ := adapter.GetValue("under/here")
				return v
			}).ShouldNot(BeNil())
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
			})
		})
	})

	Context("when there are no keys", func() {
		It("will receive PrefixNotFound error", func() {
			Eventually(errChan).Should(Receive(Equal(consuladapter.NewPrefixNotFoundError("under"))))
		})
	})
})
