package simulation_test

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"os/exec"
	"strings"

	"github.com/cloudfoundry-incubator/auction/auctionrep"
	"github.com/cloudfoundry-incubator/auction/auctionrunner"
	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/auction/communication/nats/auction_nats_client"
	"github.com/cloudfoundry-incubator/auction/simulation/auctiondistributor"
	"github.com/cloudfoundry-incubator/auction/simulation/communication/inprocess"
	"github.com/cloudfoundry-incubator/auction/simulation/simulationrepdelegate"
	"github.com/cloudfoundry-incubator/auction/simulation/visualization"
	"github.com/cloudfoundry-incubator/auction/util"
	"github.com/cloudfoundry/gunk/natsrunner"
	"github.com/cloudfoundry/yagnats"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
	"github.com/pivotal-golang/lager"

	"testing"
	"time"
)

var communicationMode string
var auctioneerMode string

const InProcess = "inprocess"
const NATS = "nats"
const KetchupNATS = "ketchup-nats"
const Remote = "remote"

//these are const because they are fixed on ketchup
const numAuctioneers = 10
const numReps = 100

var repResources = auctiontypes.Resources{
	MemoryMB:   100.0,
	DiskMB:     100.0,
	Containers: 100,
}

var maxConcurrent int

var timeout time.Duration
var runTimeout time.Duration
var auctionDistributor *auctiondistributor.AuctionDistributor

var svgReport *visualization.SVGReport
var reportName string
var reports []*visualization.Report

var sessionsToTerminate []*gexec.Session
var natsRunner *natsrunner.NATSRunner
var client auctiontypes.SimulationRepPoolClient
var repGuids []string

var numAZs int

var disableSVGReport bool

func init() {
	flag.StringVar(&communicationMode, "communicationMode", "inprocess", "one of inprocess, nats, ketchup")
	flag.StringVar(&auctioneerMode, "auctioneerMode", "inprocess", "one of inprocess, remote")
	flag.DurationVar(&timeout, "timeout", 500*time.Millisecond, "timeout when waiting for responses from remote calls")
	flag.DurationVar(&runTimeout, "runTimeout", 10*time.Second, "timeout when waiting for the run command to respond")

	flag.StringVar(&(auctionrunner.DefaultStartAuctionRules.Algorithm), "algorithm", auctionrunner.DefaultStartAuctionRules.Algorithm, "the auction algorithm to use")
	flag.IntVar(&(auctionrunner.DefaultStartAuctionRules.MaxRounds), "maxRounds", auctionrunner.DefaultStartAuctionRules.MaxRounds, "the maximum number of rounds per auction")
	flag.Float64Var(&(auctionrunner.DefaultStartAuctionRules.MaxBiddingPoolFraction), "maxBiddingPoolFraction", auctionrunner.DefaultStartAuctionRules.MaxBiddingPoolFraction, "the maximum number of participants in the pool")

	flag.IntVar(&maxConcurrent, "maxConcurrent", 20, "the maximum number of concurrent auctions to run")

	flag.IntVar(&numAZs, "numAZs", 5, "the number of availability zones or clusters to distribute the executors across")

	flag.BoolVar(&disableSVGReport, "disableSVGReport", false, "disable displaying SVG reports of the simulation runs")
}

func TestAuction(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Auction Suite")
}

var _ = BeforeSuite(func() {
	fmt.Printf("Running in %s communicationMode\n", communicationMode)
	fmt.Printf("Running in %s auctioneerMode\n", auctioneerMode)

	startReport()

	sessionsToTerminate = []*gexec.Session{}
	hosts := []string{}
	switch communicationMode {
	case InProcess:
		client, repGuids = buildInProcessReps()
		if auctioneerMode == Remote {
			panic("it doesn't make sense to use remote auctioneers when the reps are in-process")
		}
	case NATS:
		natsAddrs := startNATS()
		var err error

		natsLogger := lager.NewLogger("test")
		natsLogger.RegisterSink(lager.NewWriterSink(GinkgoWriter, lager.DEBUG))

		client, err = auction_nats_client.New(natsRunner.MessageBus, timeout, runTimeout, natsLogger)
		Ω(err).ShouldNot(HaveOccurred())
		repGuids = launchExternalReps("-natsAddrs", natsAddrs)
		if auctioneerMode == Remote {
			hosts = launchExternalAuctioneers("-natsAddrs", natsAddrs)
		}
	case KetchupNATS:
		repGuids = computeKetchupGuids()
		client = ketchupNATSClient()
		if auctioneerMode == Remote {
			hosts = ketchupAuctioneerHosts()
		}
	default:
		panic(fmt.Sprintf("unknown communication mode: %s", communicationMode))
	}

	if auctioneerMode == InProcess {
		auctionDistributor = auctiondistributor.NewInProcessAuctionDistributor(client, maxConcurrent)
	} else if auctioneerMode == Remote {
		auctionDistributor = auctiondistributor.NewRemoteAuctionDistributor(hosts, client, maxConcurrent)
	}
})

var _ = BeforeEach(func() {
	for _, repGuid := range repGuids {
		client.Reset(repGuid)
	}

	util.ResetGuids()
})

var _ = AfterSuite(func() {
	if !disableSVGReport {
		finishReport()
	}
	for _, sess := range sessionsToTerminate {
		sess.Kill().Wait()
	}

	if natsRunner != nil {
		natsRunner.Stop()
	}
})

func buildInProcessReps() (auctiontypes.SimulationRepPoolClient, []string) {
	inprocess.LatencyMin = 1 * time.Millisecond
	inprocess.LatencyMax = 2 * time.Millisecond
	inprocess.Timeout = 50 * time.Millisecond

	repGuids := []string{}
	repMap := map[string]*auctionrep.AuctionRep{}

	for i := 0; i < numReps; i++ {
		repGuid := util.NewGuid("REP")
		repGuids = append(repGuids, repGuid)

		repDelegate := simulationrepdelegate.New(
			repResources,
			int(math.Mod(float64(i), float64(numAZs))),
		)
		repMap[repGuid] = auctionrep.New(repGuid, repDelegate)
	}

	client := inprocess.New(repMap)
	return client, repGuids
}

func startNATS() string {
	natsPort := 5222 + GinkgoParallelNode()
	natsAddrs := []string{fmt.Sprintf("127.0.0.1:%d", natsPort)}
	natsRunner = natsrunner.NewNATSRunner(natsPort)
	natsRunner.Start()
	return strings.Join(natsAddrs, ",")
}

func launchExternalReps(communicationFlag string, communicationValue string) []string {
	repNodeBinary, err := gexec.Build("github.com/cloudfoundry-incubator/auction/simulation/repnode")
	Ω(err).ShouldNot(HaveOccurred())

	repGuids := []string{}

	for i := 0; i < numReps; i++ {
		repGuid := util.NewGuid("REP")

		serverCmd := exec.Command(
			repNodeBinary,
			"-repGuid", repGuid,
			communicationFlag, communicationValue,
			"-memoryMB", fmt.Sprintf("%d", repResources.MemoryMB),
			"-diskMB", fmt.Sprintf("%d", repResources.DiskMB),
			"-containers", fmt.Sprintf("%d", repResources.Containers),
		)

		sess, err := gexec.Start(serverCmd, GinkgoWriter, GinkgoWriter)
		Ω(err).ShouldNot(HaveOccurred())
		Eventually(sess).Should(gbytes.Say("listening"))
		sessionsToTerminate = append(sessionsToTerminate, sess)

		repGuids = append(repGuids, repGuid)
	}

	return repGuids
}

func launchExternalAuctioneers(communicationFlag string, communicationValue string) []string {
	auctioneerNodeBinary, err := gexec.Build("github.com/cloudfoundry-incubator/auction/simulation/auctioneernode")
	Ω(err).ShouldNot(HaveOccurred())

	auctioneerHosts := []string{}
	for i := 0; i < numAuctioneers; i++ {
		port := 48710 + i
		auctioneerCmd := exec.Command(
			auctioneerNodeBinary,
			communicationFlag, communicationValue,
			"-timeout", fmt.Sprintf("%s", timeout),
			"-httpAddr", fmt.Sprintf("127.0.0.1:%d", port),
		)
		auctioneerHosts = append(auctioneerHosts, fmt.Sprintf("127.0.0.1:%d", port))

		sess, err := gexec.Start(auctioneerCmd, GinkgoWriter, GinkgoWriter)
		Ω(err).ShouldNot(HaveOccurred())
		Eventually(sess).Should(gbytes.Say("auctioneering"))
		sessionsToTerminate = append(sessionsToTerminate, sess)
	}

	return auctioneerHosts
}

func computeKetchupGuids() []string {
	repGuids = []string{}
	for _, name := range []string{"executor_z1", "executor_z2"} {
		for jobIndex := 0; jobIndex < 5; jobIndex++ {
			for index := 0; index < 10; index++ {
				repGuids = append(repGuids, fmt.Sprintf("%s-%d-%d", name, jobIndex, index))
			}
		}
	}

	return repGuids
}

func ketchupAuctioneerHosts() []string {
	return []string{
		"10.10.50.23:48710",
		"10.10.50.24:48710",
		"10.10.50.25:48710",
		"10.10.50.26:48710",
		"10.10.50.27:48710",
		"10.10.114.23:48710",
		"10.10.114.24:48710",
		"10.10.114.25:48710",
		"10.10.114.26:48710",
		"10.10.114.27:48710",
	}
}

func ketchupNATSClient() auctiontypes.SimulationRepPoolClient {
	natsAddrs := []string{
		"10.10.50.20:4222",
		"10.10.114.20:4222",
	}

	natsClient := yagnats.NewClient()
	clusterInfo := &yagnats.ConnectionCluster{}

	for _, addr := range natsAddrs {
		clusterInfo.Members = append(clusterInfo.Members, &yagnats.ConnectionInfo{
			Addr: addr,
		})
	}

	err := natsClient.Connect(clusterInfo)
	Ω(err).ShouldNot(HaveOccurred())

	natsLogger := lager.NewLogger("test")
	natsLogger.RegisterSink(lager.NewWriterSink(GinkgoWriter, lager.DEBUG))

	client, err := auction_nats_client.New(natsClient, timeout, runTimeout, natsLogger)
	Ω(err).ShouldNot(HaveOccurred())

	return client
}

func startReport() {
	reportName = fmt.Sprintf("./runs/%s_%s_pool%.1f_conc%d.svg", auctionrunner.DefaultStartAuctionRules.Algorithm, communicationMode, auctionrunner.DefaultStartAuctionRules.MaxBiddingPoolFraction, maxConcurrent)
	svgReport = visualization.StartSVGReport(reportName, 2, 3)
	svgReport.DrawHeader(communicationMode, auctionrunner.DefaultStartAuctionRules, maxConcurrent)
}

func finishReport() {
	svgReport.Done()
	exec.Command("open", "-a", "safari", reportName).Run()

	reportJSONName := fmt.Sprintf("./runs/%s_%s_pool%.1f_conc%d.json", auctionrunner.DefaultStartAuctionRules.Algorithm, communicationMode, auctionrunner.DefaultStartAuctionRules.MaxBiddingPoolFraction, maxConcurrent)
	data, err := json.Marshal(reports)
	Ω(err).ShouldNot(HaveOccurred())
	ioutil.WriteFile(reportJSONName, data, 0777)
}
