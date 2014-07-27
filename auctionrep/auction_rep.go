package auctionrep

import (
	"errors"
	"sync"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
)

type AuctionRep struct {
	repGuid  string
	delegate auctiontypes.AuctionRepDelegate
	lock     *sync.Mutex
}

type StartInstanceScoreInfo struct {
	RemainingResources         auctiontypes.Resources
	TotalResources             auctiontypes.Resources
	NumInstancesForProcessGuid int
}

type StopIndexScoreInfo struct {
	InstanceScoreInfo            StartInstanceScoreInfo
	InstanceGuidsForProcessIndex []string
}

func New(repGuid string, delegate auctiontypes.AuctionRepDelegate) *AuctionRep {
	return &AuctionRep{
		repGuid:  repGuid,
		delegate: delegate,
		lock:     &sync.Mutex{},
	}
}

func (rep *AuctionRep) Guid() string {
	return rep.repGuid
}

func (rep *AuctionRep) AZNumber() int {
	return rep.delegate.AZNumber()
}

// must lock here; the publicly visible operations should be atomic
func (rep *AuctionRep) BidForStartAuction(startAuctionInfo auctiontypes.StartAuctionInfo) (float64, error) {
	rep.lock.Lock()
	defer rep.lock.Unlock()

	repInstanceScoreInfo, err := rep.repInstanceScoreInfo(startAuctionInfo.ProcessGuid)
	if err != nil {
		return 0, err
	}

	err = rep.satisfiesConstraints(startAuctionInfo, repInstanceScoreInfo)
	if err != nil {
		return 0, err
	}

	return rep.startAuctionBid(repInstanceScoreInfo), nil
}

// must lock here; the publicly visible operations should be atomic
func (rep *AuctionRep) BidForStopAuction(stopAuctionInfo auctiontypes.StopAuctionInfo) (float64, []string, error) {
	rep.lock.Lock()
	defer rep.lock.Unlock()

	repStopIndexScoreInfo, err := rep.repStopIndexScoreInfo(stopAuctionInfo)
	if err != nil {
		return 0, nil, err
	}

	err = rep.isRunningProcessIndex(repStopIndexScoreInfo)
	if err != nil {
		return 0, nil, err
	}

	return rep.startAuctionBid(repStopIndexScoreInfo.InstanceScoreInfo), repStopIndexScoreInfo.InstanceGuidsForProcessIndex, nil
}

// must lock here; the publicly visible operations should be atomic
func (rep *AuctionRep) RebidThenTentativelyReserve(startAuctionInfo auctiontypes.StartAuctionInfo) (float64, error) {
	rep.lock.Lock()
	defer rep.lock.Unlock()

	repInstanceScoreInfo, err := rep.repInstanceScoreInfo(startAuctionInfo.ProcessGuid)
	if err != nil {
		return 0, err
	}

	err = rep.satisfiesConstraints(startAuctionInfo, repInstanceScoreInfo)
	if err != nil {
		return 0, err
	}

	bid := rep.startAuctionBid(repInstanceScoreInfo)

	//then reserve
	err = rep.delegate.Reserve(startAuctionInfo)
	if err != nil {
		return 0, err
	}

	return bid, nil
}

// must lock here; the publicly visible operations should be atomic
func (rep *AuctionRep) ReleaseReservation(startAuctionInfo auctiontypes.StartAuctionInfo) error {
	rep.lock.Lock()
	defer rep.lock.Unlock()

	return rep.delegate.ReleaseReservation(startAuctionInfo)
}

// must lock here; the publicly visible operations should be atomic
func (rep *AuctionRep) Run(startAuction models.LRPStartAuction) error {
	rep.lock.Lock()
	defer rep.lock.Unlock()

	return rep.delegate.Run(startAuction)
}

// must lock here; the publicly visible operations should be atomic
func (rep *AuctionRep) Stop(stopInstance models.StopLRPInstance) error {
	rep.lock.Lock()
	defer rep.lock.Unlock()

	return rep.delegate.Stop(stopInstance)
}

// simulation-only
func (rep *AuctionRep) TotalResources() auctiontypes.Resources {
	totalResources, _ := rep.delegate.TotalResources()
	return totalResources
}

// simulation-only
// must lock here; the publicly visible operations should be atomic
func (rep *AuctionRep) Reset() {
	rep.lock.Lock()
	defer rep.lock.Unlock()

	simDelegate, ok := rep.delegate.(auctiontypes.SimulationAuctionRepDelegate)
	if !ok {
		println("not reseting")
		return
	}
	simDelegate.SetSimulatedInstances([]auctiontypes.SimulatedInstance{})
}

// simulation-only
// must lock here; the publicly visible operations should be atomic
func (rep *AuctionRep) SetSimulatedInstances(instances []auctiontypes.SimulatedInstance) {
	rep.lock.Lock()
	defer rep.lock.Unlock()

	simDelegate, ok := rep.delegate.(auctiontypes.SimulationAuctionRepDelegate)
	if !ok {
		println("not setting instances")
		return
	}
	simDelegate.SetSimulatedInstances(instances)
}

// simulation-only
// must lock here; the publicly visible operations should be atomic
func (rep *AuctionRep) SimulatedInstances() []auctiontypes.SimulatedInstance {
	rep.lock.Lock()
	defer rep.lock.Unlock()

	simDelegate, ok := rep.delegate.(auctiontypes.SimulationAuctionRepDelegate)
	if !ok {
		println("not fetching instances")
		return []auctiontypes.SimulatedInstance{}
	}
	return simDelegate.SimulatedInstances()
}

// private internals -- no locks here
func (rep *AuctionRep) repInstanceScoreInfo(processGuid string) (StartInstanceScoreInfo, error) {
	remaining, err := rep.delegate.RemainingResources()
	if err != nil {
		return StartInstanceScoreInfo{}, err
	}

	total, err := rep.delegate.TotalResources()
	if err != nil {
		return StartInstanceScoreInfo{}, err
	}

	nInstances, err := rep.delegate.NumInstancesForProcessGuid(processGuid)
	if err != nil {
		return StartInstanceScoreInfo{}, err
	}

	return StartInstanceScoreInfo{
		RemainingResources:         remaining,
		TotalResources:             total,
		NumInstancesForProcessGuid: nInstances,
	}, nil
}

// private internals -- no locks here
func (rep *AuctionRep) repStopIndexScoreInfo(stopAuctionInfo auctiontypes.StopAuctionInfo) (StopIndexScoreInfo, error) {
	instanceScoreInfo, err := rep.repInstanceScoreInfo(stopAuctionInfo.ProcessGuid)
	if err != nil {
		return StopIndexScoreInfo{}, err
	}

	instanceGuids, err := rep.delegate.InstanceGuidsForProcessGuidAndIndex(stopAuctionInfo.ProcessGuid, stopAuctionInfo.Index)
	if err != nil {
		return StopIndexScoreInfo{}, err
	}

	return StopIndexScoreInfo{
		InstanceScoreInfo:            instanceScoreInfo,
		InstanceGuidsForProcessIndex: instanceGuids,
	}, nil
}

// private internals -- no locks here
func (rep *AuctionRep) satisfiesConstraints(startAuctionInfo auctiontypes.StartAuctionInfo, repInstanceScoreInfo StartInstanceScoreInfo) error {
	remaining := repInstanceScoreInfo.RemainingResources
	hasEnoughMemory := remaining.MemoryMB >= startAuctionInfo.MemoryMB
	hasEnoughDisk := remaining.DiskMB >= startAuctionInfo.DiskMB
	hasEnoughContainers := remaining.Containers > 0

	if hasEnoughMemory && hasEnoughDisk && hasEnoughContainers {
		return nil
	} else {
		return auctiontypes.InsufficientResources
	}
}

func (rep *AuctionRep) isRunningProcessIndex(repStopIndexScoreInfo StopIndexScoreInfo) error {
	if len(repStopIndexScoreInfo.InstanceGuidsForProcessIndex) == 0 {
		return errors.New("not-running-instance")
	}
	return nil
}

// private internals -- no locks here
func (rep *AuctionRep) startAuctionBid(repInstanceScoreInfo StartInstanceScoreInfo) float64 {
	remaining := repInstanceScoreInfo.RemainingResources
	total := repInstanceScoreInfo.TotalResources

	fractionUsedContainers := 1.0 - float64(remaining.Containers)/float64(total.Containers)
	fractionUsedDisk := 1.0 - float64(remaining.DiskMB)/float64(total.DiskMB)
	fractionUsedMemory := 1.0 - float64(remaining.MemoryMB)/float64(total.MemoryMB)

	return ((fractionUsedContainers + fractionUsedDisk + fractionUsedMemory) / 3.0) + float64(repInstanceScoreInfo.NumInstancesForProcessGuid)
}
