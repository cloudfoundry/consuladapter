package consuladapter_test

import (
	"fmt"
	"time"

	"github.com/cloudfoundry-incubator/consuladapter"
	"github.com/cloudfoundry-incubator/consuladapter/consulrunner"
	"github.com/cloudfoundry-incubator/consuladapter/fakes"
	"github.com/hashicorp/consul/api"

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

var clusterRunner *consulrunner.ClusterRunner
var session *consuladapter.Session

var _ = BeforeSuite(func() {
	clusterStartingPort := 5001 + config.GinkgoConfig.ParallelNode*consulrunner.PortOffsetLength*clusterSize
	clusterRunner = consulrunner.NewClusterRunner(clusterStartingPort, clusterSize, "http")
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
	client := clusterRunner.NewClient()

	var err error
	session, err = consuladapter.NewSession(fmt.Sprintf("session-%d", config.GinkgoConfig.ParallelNode), 10*time.Second, consuladapter.NewConsulClient(client))
	Expect(err).NotTo(HaveOccurred())
}

func startCluster() {
	clusterRunner.Start()
	clusterRunner.WaitUntilReady()
}

func newFakeClient(realClient *api.Client) (*fakes.FakeClient, *fakes.FakeAgent, *fakes.FakeKV, *fakes.FakeISession) {
	client, agent, kv, session := fakes.NewFakeClient()
	agent.NodeNameStub = realClient.Agent().NodeName

	kv.ReleaseStub = realClient.KV().Release
	kv.GetStub = realClient.KV().Get
	kv.ListStub = realClient.KV().List

	session.NodeStub = realClient.Session().Node
	session.CreateStub = realClient.Session().Create
	session.CreateNoChecksStub = realClient.Session().CreateNoChecks
	session.DestroyStub = realClient.Session().Destroy
	session.RenewStub = realClient.Session().Renew
	session.RenewPeriodicStub = realClient.Session().RenewPeriodic
	session.ListStub = realClient.Session().List

	client.LockOptsStub = func(opts *api.LockOptions) (consuladapter.Lock, error) {
		return realClient.LockOpts(opts)
	}
	return client, agent, kv, session
}
