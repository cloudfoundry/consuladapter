package consuladapter_test

import (
	"github.com/cloudfoundry-incubator/consuladapter"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"

	"testing"
)

func TestConsulAdapter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Adapter <-> Cluster-Runner Integration Suite")
}

const clusterSize = 3

var clusterRunner *consuladapter.ClusterRunner
var adapter consuladapter.Adapter

var _ = BeforeSuite(func() {
	clusterStartingPort := 5001 + config.GinkgoConfig.ParallelNode*consuladapter.PortOffsetLength*clusterSize
	clusterRunner = consuladapter.NewClusterRunner(clusterStartingPort, clusterSize, "http")
})

func stopCluster() {
	clusterRunner.Stop()
}

func startClusterAndAdapter() {
	clusterRunner.Start()
	adapter = clusterRunner.NewAdapter()
}
