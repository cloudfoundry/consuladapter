package consuladapter_test

import (
	"github.com/cloudfoundry-incubator/consuladapter"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Adapter", func() {
	Describe("Parse", func() {
		It("errors when passed an invalid URL", func() {
			_, _, err := consuladapter.Parse(":/")
			Ω(err).Should(HaveOccurred())
		})

		It("errors when passed a scheme that is not http or https", func() {
			_, _, err := consuladapter.Parse("sftp://")
			Ω(err).Should(HaveOccurred())
		})

		It("errors when passed an empty host", func() {
			_, _, err := consuladapter.Parse("http:///")
			Ω(err).Should(HaveOccurred())
		})

		It("errors when passed mismatched schemes", func() {
			_, _, err := consuladapter.Parse("http://1.2.3.4:5678,https://6.7.8.9:9876")
			Ω(err).Should(HaveOccurred())
		})

		It("returns the scheme and set of addresses", func() {
			scheme, addresses, err := consuladapter.Parse("https://1.2.3.4:5678,https://6.7.8.9:9876")
			Ω(err).ShouldNot(HaveOccurred())
			Ω(scheme).Should(Equal("https"))
			Ω(addresses).Should(ConsistOf("1.2.3.4:5678", "6.7.8.9:9876"))
		})

	})

})
