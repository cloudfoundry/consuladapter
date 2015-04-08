package consuladapter_test

import (
	"fmt"

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

const clusterSize = 1

var clusterRunner *consuladapter.ClusterRunner
var session *consuladapter.Session

var _ = BeforeSuite(func() {
	clusterStartingPort := 5001 + config.GinkgoConfig.ParallelNode*consuladapter.PortOffsetLength*clusterSize
	clusterRunner = consuladapter.NewClusterRunner(clusterStartingPort, clusterSize, "http")
})

func stopCluster() {
	clusterRunner.Stop()
}
func stopClusterAndSession() {
	if session != nil {
		session.Destroy()
	}
	stopCluster()
}

func startClusterAndSession() {
	startCluster()
	session = clusterRunner.NewSession(fmt.Sprintf("session-%d", config.GinkgoConfig.ParallelNode))
}

func startCluster() {
	clusterRunner.Start()
	clusterRunner.WaitUntilReady()
}
