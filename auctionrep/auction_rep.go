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

type InstanceScoreInfo struct {
	RemainingResources                auctiontypes.Resources
	TotalResources                    auctiontypes.Resources
	NumInstancesOnRepForProcessGuid   int
	NumInstancesDesiredForProcessGuid int
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

	repInstanceScoreInfo, err := rep.repInstanceScoreInfo(startAuctionInfo.ProcessGuid, startAuctionInfo.NumInstances)
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
func (rep *AuctionRep) RebidThenTentativelyReserve(startAuctionInfo auctiontypes.StartAuctionInfo) (float64, error) {
	rep.lock.Lock()
	defer rep.lock.Unlock()

	repInstanceScoreInfo, err := rep.repInstanceScoreInfo(startAuctionInfo.ProcessGuid, startAuctionInfo.NumInstances)
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
func (rep *AuctionRep) BidForStopAuction(stopAuctionInfo auctiontypes.StopAuctionInfo) (float64, []string, error) {
	rep.lock.Lock()
	defer rep.lock.Unlock()

	instanceScoreInfo, err := rep.repInstanceScoreInfo(stopAuctionInfo.ProcessGuid, stopAuctionInfo.NumInstances)
	if err != nil {
		return 0, nil, err
	}

	instanceGuids, err := rep.delegate.InstanceGuidsForProcessGuidAndIndex(stopAuctionInfo.ProcessGuid, stopAuctionInfo.Index)
	if err != nil {
		return 0, nil, err
	}

	err = rep.isRunningProcessIndex(instanceGuids)
	if err != nil {
		return 0, nil, err
	}

	return rep.stopAuctionBid(instanceScoreInfo), instanceGuids, nil
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
func (rep *AuctionRep) repInstanceScoreInfo(processGuid string, numInstances int) (InstanceScoreInfo, error) {
	remaining, err := rep.delegate.RemainingResources()
	if err != nil {
		return InstanceScoreInfo{}, err
	}

	total, err := rep.delegate.TotalResources()
	if err != nil {
		return InstanceScoreInfo{}, err
	}

	nInstancesOnRep, err := rep.delegate.NumInstancesForProcessGuid(processGuid)
	if err != nil {
		return InstanceScoreInfo{}, err
	}

	return InstanceScoreInfo{
		RemainingResources:                remaining,
		TotalResources:                    total,
		NumInstancesOnRepForProcessGuid:   nInstancesOnRep,
		NumInstancesDesiredForProcessGuid: numInstances,
	}, nil
}

// private internals -- no locks here
func (rep *AuctionRep) satisfiesConstraints(startAuctionInfo auctiontypes.StartAuctionInfo, repInstanceScoreInfo InstanceScoreInfo) error {
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

// private internals -- no locks here
func (rep *AuctionRep) isRunningProcessIndex(instanceGuids []string) error {
	if len(instanceGuids) == 0 {
		return errors.New("not-running-instance")
	}
	return nil
}

// private internals -- no locks here
func (rep *AuctionRep) startAuctionBid(repInstanceScoreInfo InstanceScoreInfo) float64 {
	remaining := repInstanceScoreInfo.RemainingResources
	total := repInstanceScoreInfo.TotalResources

	fractionUsedContainers := 1.0 - float64(remaining.Containers)/float64(total.Containers)
	fractionUsedDisk := 1.0 - float64(remaining.DiskMB)/float64(total.DiskMB)
	fractionUsedMemory := 1.0 - float64(remaining.MemoryMB)/float64(total.MemoryMB)
	fractionInstancesForProcessGuid := float64(repInstanceScoreInfo.NumInstancesOnRepForProcessGuid) / float64(repInstanceScoreInfo.NumInstancesDesiredForProcessGuid)

	return (1.0/3.0)*fractionUsedContainers +
		(1.0/3.0)*fractionUsedDisk +
		(1.0/3.0)*fractionUsedMemory +
		(1.0/1.0)*fractionInstancesForProcessGuid
}

// private internals -- no locks here
func (rep *AuctionRep) stopAuctionBid(repInstanceScoreInfo InstanceScoreInfo) float64 {
	remaining := repInstanceScoreInfo.RemainingResources
	total := repInstanceScoreInfo.TotalResources

	fractionUsedContainers := 1.0 - float64(remaining.Containers)/float64(total.Containers)
	fractionUsedDisk := 1.0 - float64(remaining.DiskMB)/float64(total.DiskMB)
	fractionUsedMemory := 1.0 - float64(remaining.MemoryMB)/float64(total.MemoryMB)

	return (1.0/3.0)*fractionUsedContainers +
		(1.0/3.0)*fractionUsedDisk +
		(1.0/3.0)*fractionUsedMemory +
		(1.0/1.0)*float64(repInstanceScoreInfo.NumInstancesOnRepForProcessGuid)
}
