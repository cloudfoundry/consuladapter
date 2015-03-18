package consuladapter_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestConsulAdapter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Consul Adapter Suite")
}
