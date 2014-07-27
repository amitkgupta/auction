package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/cloudfoundry-incubator/auction/auctionrep"
	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	auction_nats_server "github.com/cloudfoundry-incubator/auction/communication/nats/auction_nats_server"
	"github.com/cloudfoundry-incubator/auction/simulation/simulationrepdelegate"
	"github.com/cloudfoundry-incubator/cf-lager"
	"github.com/cloudfoundry/yagnats"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/sigmon"
)

var memoryMB = flag.Int("memoryMB", 100, "total available memory in MB")
var diskMB = flag.Int("diskMB", 100, "total available disk in MB")
var containers = flag.Int("containers", 100, "total available containers")
var repGuid = flag.String("repGuid", "", "rep-guid")
var natsAddrs = flag.String("natsAddrs", "", "nats server addresses")
var azNumber = flag.Int("azNumber", -1, "number indicating which AZ this rep is running on")

func main() {
	flag.Parse()

	if *repGuid == "" {
		panic("need rep-guid")
	}

	if *natsAddrs == "" {
		panic("need nats addr")
	}

	if *azNumber < 0 {
		panic("needs (non-negative) AZ number")
	}

	repDelegate := simulationrepdelegate.New(auctiontypes.Resources{
		MemoryMB:   *memoryMB,
		DiskMB:     *diskMB,
		Containers: *containers,
	}, *azNumber)
	rep := auctionrep.New(*repGuid, repDelegate)

	if *natsAddrs != "" {
		client := yagnats.NewClient()

		clusterInfo := &yagnats.ConnectionCluster{}

		for _, addr := range strings.Split(*natsAddrs, ",") {
			clusterInfo.Members = append(clusterInfo.Members, &yagnats.ConnectionInfo{
				Addr: addr,
			})
		}

		err := client.Connect(clusterInfo)

		if err != nil {
			log.Fatalln("no nats:", err)
		}

		log.Println("starting rep nats server")
		natsRunner := auction_nats_server.New(client, rep, cf_lager.New("repnode").Session(*repGuid))
		server := ifrit.Envoke(natsRunner)
		monitor := ifrit.Envoke(sigmon.New(server))
		fmt.Println("rep node listening")
		err = <-monitor.Wait()
		if err != nil {
			println("NATS SERVER EXITED WITH ERROR: ", err.Error())
		}
	}

	select {}
}
