package consuladapter_test

import (
	"github.com/cloudfoundry-incubator/consuladapter"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Setting and Getting Data", func() {
	BeforeEach(startClusterAndAdapter)
	AfterEach(stopCluster)

	It("Creates data, reads individual key data, and lists data extending given prefixes", func() {
		allChildren, err := adapter.ListPairsExtending("")
		Ω(err).ShouldNot(HaveOccurred())
		Ω(allChildren).Should(HaveLen(0))

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

	It("Returns a KeyNotFound error when getting a non-existent key", func() {
		_, err := adapter.GetValue("not-present")
		Ω(err).Should(Equal(consuladapter.NewKeyNotFoundError("not-present")))
	})
})
