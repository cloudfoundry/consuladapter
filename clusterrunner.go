package consuladapter

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/cloudfoundry-incubator/cf_http"
	"github.com/hashicorp/consul/api"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"

	. "github.com/onsi/gomega"
)

type ClusterRunner struct {
	startingPort    int
	numNodes        int
	consulProcesses []ifrit.Process
	running         bool
	dataDir         string
	configDir       string
	scheme          string

	mutex *sync.RWMutex
}

const defaultDataDirPrefix = "consul_data"
const defaultConfigDirPrefix = "consul_config"

func NewClusterRunner(startingPort int, numNodes int, scheme string) *ClusterRunner {
	Ω(startingPort).Should(BeNumerically(">", 0))
	Ω(startingPort).Should(BeNumerically("<", 1<<16))
	Ω(numNodes).Should(BeNumerically(">", 0))

	return &ClusterRunner{
		startingPort: startingPort,
		numNodes:     numNodes,
		scheme:       scheme,

		mutex: &sync.RWMutex{},
	}
}

func (cr *ClusterRunner) Start() {
	cr.mutex.Lock()
	defer cr.mutex.Unlock()

	if cr.running {
		return
	}

	tmpDir, err := ioutil.TempDir("", defaultDataDirPrefix)
	Ω(err).ShouldNot(HaveOccurred())
	cr.dataDir = tmpDir

	tmpDir, err = ioutil.TempDir("", defaultConfigDirPrefix)
	Ω(err).ShouldNot(HaveOccurred())
	cr.configDir = tmpDir

	cr.consulProcesses = make([]ifrit.Process, cr.numNodes)

	for i := 0; i < cr.numNodes; i++ {
		iStr := fmt.Sprintf("%d", i)
		nodeDataDir := path.Join(cr.dataDir, iStr)
		os.MkdirAll(nodeDataDir, 0700)

		configFilePath := writeConfigFile(
			cr.configDir,
			nodeDataDir,
			iStr,
			cr.startingPort,
			i,
			cr.numNodes,
		)

		process := ginkgomon.Invoke(ginkgomon.New(ginkgomon.Config{
			Name:              fmt.Sprintf("consul_cluster[%d]", i),
			AnsiColorCode:     "35m",
			StartCheck:        "agent: Join completed.",
			StartCheckTimeout: 5 * time.Second,
			Command: exec.Command(
				"consul",
				"agent",
				"--config-file", configFilePath,
			),
		}))
		cr.consulProcesses[i] = process

		ready := process.Ready()
		Eventually(ready, 10, 0.05).Should(BeClosed(), "Expected consul to be up and running")
	}

	cr.running = true
}

func (cr *ClusterRunner) WaitUntilReady() {
	client, err := api.NewClient(&api.Config{
		Address:    cr.Addresses()[0],
		Scheme:     cr.scheme,
		HttpClient: cf_http.NewStreamingClient(),
	})
	Ω(err).ShouldNot(HaveOccurred())
	catalog := client.Catalog()

	Eventually(func() error {
		_, qm, err := catalog.Nodes(nil)
		if err != nil {
			return err
		}
		if qm.KnownLeader && qm.LastIndex > 0 {
			return nil
		}
		return errors.New("not ready")
	}, 10, 100*time.Millisecond).Should(BeNil())
}

func (cr *ClusterRunner) Stop() {
	cr.mutex.Lock()
	defer cr.mutex.Unlock()

	if !cr.running {
		return
	}

	for i := 0; i < cr.numNodes; i++ {
		ginkgomon.Kill(cr.consulProcesses[i], 5*time.Second)
	}

	os.RemoveAll(cr.dataDir)
	os.RemoveAll(cr.configDir)
	cr.consulProcesses = nil
	cr.running = false
}

func (cr *ClusterRunner) ConsulCluster() string {
	urls := make([]string, cr.numNodes)
	for i := 0; i < cr.numNodes; i++ {
		urls[i] = fmt.Sprintf("%s://127.0.0.1:%d", cr.scheme, cr.startingPort+i*PortOffsetLength+PortOffsetHTTP)
	}

	return strings.Join(urls, ",")
}

func (cr *ClusterRunner) Addresses() []string {
	addresses := make([]string, cr.numNodes)
	for i := 0; i < cr.numNodes; i++ {
		addresses[i] = fmt.Sprintf("127.0.0.1:%d", cr.startingPort+i*PortOffsetLength+PortOffsetHTTP)
	}

	return addresses
}

func (cr *ClusterRunner) NewAdapter() *Adapter {
	adapter, err := NewAdapter(cr.Addresses(), cr.scheme)
	Ω(err).ShouldNot(HaveOccurred())

	return adapter
}

func (cr *ClusterRunner) Reset() {
	err := cr.NewAdapter().reset()
	Ω(err).ShouldNot(HaveOccurred())
}
