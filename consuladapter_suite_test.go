package consuladapter_test

import (
	"github.com/cloudfoundry-incubator/consuladapter"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestConsulAdapter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Consul Adapter Suite")
}

const clusterStartingPort = 9000
const clusterSize = 3

var clusterRunner consuladapter.ClusterRunner
var adapter consuladapter.Adapter

var _ = BeforeSuite(func() {
	clusterRunner = consuladapter.NewClusterRunner(clusterStartingPort, clusterSize)
	clusterRunner.Start()

	adapter = clusterRunner.NewAdapter()
})

var _ = AfterSuite(func() {
	clusterRunner.Stop()
})

var _ = BeforeEach(func() {
	clusterRunner.Reset()
})
