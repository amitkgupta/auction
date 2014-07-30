package simulation_test

import (
	"fmt"

	"github.com/cloudfoundry-incubator/auction/auctionrunner"
	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/auction/simulation/visualization"
	"github.com/cloudfoundry-incubator/auction/util"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Ω

var _ = Describe("Start and Stop Auctions", func() {
	var initialDistributions map[int][]auctiontypes.SimulatedInstance

	newSimulatedInstance := func(processGuid string, index int, memoryMB int) auctiontypes.SimulatedInstance {
		return auctiontypes.SimulatedInstance{
			ProcessGuid:  processGuid,
			InstanceGuid: util.NewGuid("INS"),
			Index:        index,
			MemoryMB:     memoryMB,
			DiskMB:       1,
		}
	}

	generateUniqueSingleIndexLRPsWithRedundantSimulatedInstances := func(numInstances int, memoryMB int) []auctiontypes.SimulatedInstance {
		instances := []auctiontypes.SimulatedInstance{}
		for i := 0; i < numInstances; i++ {
			instances = append(instances, newSimulatedInstance(util.NewGrayscaleGuid("AAA"), 0, memoryMB))
		}
		return instances
	}

	generateSimulatedInstancesForProcessGuidIndex := func(processGuid string, numInstances int, index int, memoryMB int) []auctiontypes.SimulatedInstance {
		instances := []auctiontypes.SimulatedInstance{}
		for i := 0; i < numInstances; i++ {
			instances = append(instances, newSimulatedInstance(processGuid, index, memoryMB))
		}
		return instances
	}

	newLRPStartAuction := func(processGuid string, memoryMB int, index int, numInstances int) models.LRPStartAuction {
		return models.LRPStartAuction{
			DesiredLRP: models.DesiredLRP{
				ProcessGuid: processGuid,
				MemoryMB:    memoryMB,
				DiskMB:      1,
				Instances:   numInstances,
			},

			InstanceGuid: util.NewGuid("INS"),
			Index:        index,
			NumAZs:       numAZs,
		}
	}

	generateLRPStartAuctionsForProcessGuid := func(numInstances int, processGuid string, memoryMB int) []models.LRPStartAuction {
		instances := []models.LRPStartAuction{}
		for i := 0; i < numInstances; i++ {
			instances = append(instances, newLRPStartAuction(processGuid, memoryMB, i, numInstances))
		}
		return instances
	}

	generateUniqueLRPStartAuctions := func(numLRPs int, memoryMB int, numInstancesPerLRP int) []models.LRPStartAuction {
		instances := []models.LRPStartAuction{}
		for i := 0; i < numLRPs; i++ {
			instances = append(
				instances,
				generateLRPStartAuctionsForProcessGuid(numInstancesPerLRP, util.NewGrayscaleGuid("BBB"), memoryMB)...,
			)
		}
		return instances
	}

	BeforeEach(func() {
		util.ResetGuids()
		initialDistributions = map[int][]auctiontypes.SimulatedInstance{}
	})

	JustBeforeEach(func() {
		for index, simulatedInstances := range initialDistributions {
			client.SetSimulatedInstances(repGuids[index], simulatedInstances)
		}
	})

	Describe("Start Auction Simulations", func() {
		Context("With no process running on any executor initially", func() {
			nexec := []int{25, 100}

			n1MBapps := []int{1800, 7000}
			n2MBapps := []int{200, 1000}
			n4MBapps := []int{50, 200}

			n1INSTapps := []int{1800, 7000}
			n2INSTapps := []int{200, 1000}
			n4INSTapps := []int{50, 200}

			nXSapps := []int{1500, 6000}
			nXLapps := []int{55, 220}

			nSPFapps := []int{1500, 6000}
			nHAapps := []int{55, 220}

			for i := range nexec {
				i := i

				Context("with variable memory requirements between apps", func() {
					It("should distribute evenly", func() {
						instances := []models.LRPStartAuction{}

						instances = append(instances, generateUniqueLRPStartAuctions(n1MBapps[i], 1, 1)...)
						instances = append(instances, generateUniqueLRPStartAuctions(n2MBapps[i], 2, 1)...)
						instances = append(instances, generateUniqueLRPStartAuctions(n4MBapps[i], 4, 1)...)

						permutedInstances := make([]models.LRPStartAuction, len(instances))
						for i, index := range util.R.Perm(len(instances)) {
							permutedInstances[i] = instances[index]
						}
						report := auctionDistributor.HoldAuctionsFor(
							"Cold start with variable memory requirements between apps",
							nexec[i],
							permutedInstances,
							repGuids[:nexec[i]],
							auctionrunner.DefaultStartAuctionRules,
						)

						visualization.PrintReport(
							numAZs,
							client,
							report.AuctionResults,
							repGuids[:nexec[i]],
							report.AuctionDuration,
							auctionrunner.DefaultStartAuctionRules,
						)

						svgReport.DrawReportCard(i, 0, report)
						reports = append(reports, report)
					})
				})

				Context("with a few very high-memory apps", func() {
					It("should distribute evenly", func() {
						instances := []models.LRPStartAuction{}

						instances = append(instances, generateUniqueLRPStartAuctions(nXSapps[i], 1, 1)...)
						instances = append(instances, generateUniqueLRPStartAuctions(nXLapps[i], 15, 1)...)

						permutedInstances := make([]models.LRPStartAuction, len(instances))
						for i, index := range util.R.Perm(len(instances)) {
							permutedInstances[i] = instances[index]
						}
						report := auctionDistributor.HoldAuctionsFor(
							"Cold start with a few very high-memory apps",
							nexec[i],
							permutedInstances,
							repGuids[:nexec[i]],
							auctionrunner.DefaultStartAuctionRules,
						)

						visualization.PrintReport(
							numAZs,
							client,
							report.AuctionResults,
							repGuids[:nexec[i]],
							report.AuctionDuration,
							auctionrunner.DefaultStartAuctionRules,
						)

						svgReport.DrawReportCard(i, 0, report)
						reports = append(reports, report)
					})
				})

				Context("with variable instance requirements between apps", func() {
					It("should distribute evenly", func() {
						instances := []models.LRPStartAuction{}

						instances = append(instances, generateUniqueLRPStartAuctions(n1INSTapps[i], 1, 1)...)
						instances = append(instances, generateUniqueLRPStartAuctions(n2INSTapps[i], 1, 2)...)
						instances = append(instances, generateUniqueLRPStartAuctions(n4INSTapps[i], 1, 4)...)

						permutedInstances := make([]models.LRPStartAuction, len(instances))
						for i, index := range util.R.Perm(len(instances)) {
							permutedInstances[i] = instances[index]
						}
						report := auctionDistributor.HoldAuctionsFor(
							"Cold start with variable instance requirements between apps",
							nexec[i],
							permutedInstances,
							repGuids[:nexec[i]],
							auctionrunner.DefaultStartAuctionRules,
						)

						visualization.PrintReport(
							numAZs,
							client,
							report.AuctionResults,
							repGuids[:nexec[i]],
							report.AuctionDuration,
							auctionrunner.DefaultStartAuctionRules,
						)

						svgReport.DrawReportCard(i, 0, report)
						reports = append(reports, report)
					})
				})

				Context("with a few very high-instance apps", func() {
					It("should distribute evenly", func() {
						instances := []models.LRPStartAuction{}

						instances = append(instances, generateUniqueLRPStartAuctions(nSPFapps[i], 1, 1)...)
						instances = append(instances, generateUniqueLRPStartAuctions(nHAapps[i], 1, 15)...)

						permutedInstances := make([]models.LRPStartAuction, len(instances))
						for i, index := range util.R.Perm(len(instances)) {
							permutedInstances[i] = instances[index]
						}
						report := auctionDistributor.HoldAuctionsFor(
							"Cold start with a few very high-instance apps",
							nexec[i],
							permutedInstances,
							repGuids[:nexec[i]],
							auctionrunner.DefaultStartAuctionRules,
						)

						visualization.PrintReport(
							numAZs,
							client,
							report.AuctionResults,
							repGuids[:nexec[i]],
							report.AuctionDuration,
							auctionrunner.DefaultStartAuctionRules,
						)

						svgReport.DrawReportCard(i, 0, report)
						reports = append(reports, report)
					})
				})
			}
		})

		Context("With some executors empty, and the remaining executors running many perfectly-balanced process instances", func() {
			nexec := []int{100, 100}
			nempty := []int{5, 1}
			napps := []int{500, 100}
			numInstancesPerNewLRP := 3
			numInitialProcessPerNonEmptyRep := 50

			for i := range nexec {
				i := i
				description := fmt.Sprintf(
					"%d Executors, %d Initially Empty, %d Initially Running %d Process, %d New %d-Instance Processes",
					nexec[i],
					nempty[i],
					nexec[i]-nempty[i],
					numInitialProcessPerNonEmptyRep,
					napps[i],
					numInstancesPerNewLRP,
				)

				Context(description, func() {
					BeforeEach(func() {
						for j := 0; j < nexec[i]-nempty[i]; j++ {
							initialDistributions[j] = generateUniqueSingleIndexLRPsWithRedundantSimulatedInstances(numInitialProcessPerNonEmptyRep, 1)
						}
					})

					It("should distribute evenly", func() {
						instances := generateUniqueLRPStartAuctions(napps[i], 1, numInstancesPerNewLRP)

						report := auctionDistributor.HoldAuctionsFor(
							description,
							nexec[i],
							instances,
							repGuids[:nexec[i]],
							auctionrunner.DefaultStartAuctionRules,
						)

						visualization.PrintReport(
							numAZs,
							client,
							report.AuctionResults,
							repGuids[:nexec[i]],
							report.AuctionDuration,
							auctionrunner.DefaultStartAuctionRules,
						)

						svgReport.DrawReportCard(i, 1, report)
						reports = append(reports, report)
					})
				})
			}
		})

		Context("With all executors running well-balanced process instances", func() {
			nexec := []int{30, 100}
			napps := []int{200, 400}
			maxInitialProcessesPerExecutor := 80

			for i := range nexec {
				i := i
				description := fmt.Sprintf(
					"%d Executors, roughly %d Initial Processes per Executor, %d New Processes",
					nexec[i],
					maxInitialProcessesPerExecutor,
					napps[i],
				)

				Context(description, func() {
					BeforeEach(func() {
						for j := 0; j < nexec[i]; j++ {
							numInstances := util.RandomIntIn(maxInitialProcessesPerExecutor-2, maxInitialProcessesPerExecutor)
							initialDistributions[j] = generateUniqueSingleIndexLRPsWithRedundantSimulatedInstances(numInstances, 1)
						}
					})

					It("should distribute evenly", func() {
						instances := generateLRPStartAuctionsForProcessGuid(napps[i], "red", 1)

						report := auctionDistributor.HoldAuctionsFor(
							description,
							nexec[i],
							instances,
							repGuids[:nexec[i]],
							auctionrunner.DefaultStartAuctionRules,
						)

						visualization.PrintReport(
							numAZs,
							client,
							report.AuctionResults,
							repGuids[:nexec[i]],
							report.AuctionDuration,
							auctionrunner.DefaultStartAuctionRules,
						)

						svgReport.DrawReportCard(i, 2, report)
						reports = append(reports, report)
					})
				})
			}
		})
	})

	Describe("Stop Auctions Specifications", func() {
		Describe("When more than one instance is running for a particular index of a process", func() {
			processGuid := util.NewGrayscaleGuid("AAA")

			Context("when the redundant instances are on executors with different amounts of available resources", func() {
				BeforeEach(func() {
					initialDistributions[0] = generateUniqueSingleIndexLRPsWithRedundantSimulatedInstances(50, 1)
					initialDistributions[0] = append(initialDistributions[0], generateSimulatedInstancesForProcessGuidIndex(processGuid, 1, 0, 1)...)

					initialDistributions[1] = generateUniqueSingleIndexLRPsWithRedundantSimulatedInstances(30, 1)
					initialDistributions[1] = append(initialDistributions[1], generateSimulatedInstancesForProcessGuidIndex(processGuid, 1, 0, 1)...)
				})

				It("should favor removing the instance from the heavy-laden executor", func() {
					stopAuctions := []models.LRPStopAuction{
						{
							ProcessGuid: processGuid,
							Index:       0,
						},
					}

					results := auctionDistributor.HoldStopAuctions(stopAuctions, repGuids)
					Ω(results).Should(HaveLen(1))
					Ω(results[0].Winner).Should(Equal("REP-2"))

					instancesOn0 := client.SimulatedInstances(repGuids[0])
					instancesOn1 := client.SimulatedInstances(repGuids[1])

					Ω(instancesOn0).Should(HaveLen(50))
					Ω(instancesOn1).Should(HaveLen(31))
				})
			})

			Context("when one redundant instance is on an executor with fewer resources, and the other redundant instance is on an executor running another index of the process", func() {
				BeforeEach(func() {
					initialDistributions[0] = generateUniqueSingleIndexLRPsWithRedundantSimulatedInstances(50, 1)
					initialDistributions[0] = append(initialDistributions[0], generateSimulatedInstancesForProcessGuidIndex(processGuid, 1, 0, 1)...)

					initialDistributions[1] = generateUniqueSingleIndexLRPsWithRedundantSimulatedInstances(30, 1)
					initialDistributions[1] = append(initialDistributions[1], generateSimulatedInstancesForProcessGuidIndex(processGuid, 1, 0, 1)...)
					initialDistributions[1] = append(initialDistributions[1], generateSimulatedInstancesForProcessGuidIndex(processGuid, 1, 1, 1)...)
				})

				It("should favor removing the redundant instance from the executor running the additional index of the process", func() {
					stopAuctions := []models.LRPStopAuction{
						{
							ProcessGuid: processGuid,
							Index:       0,
						},
					}

					results := auctionDistributor.HoldStopAuctions(stopAuctions, repGuids)
					Ω(results).Should(HaveLen(1))
					Ω(results[0].Winner).Should(Equal("REP-1"))

					instancesOn0 := client.SimulatedInstances(repGuids[0])
					instancesOn1 := client.SimulatedInstances(repGuids[1])

					Ω(instancesOn0).Should(HaveLen(51))
					Ω(instancesOn1).Should(HaveLen(31))
				})
			})

			Context("when the executor with fewer available resources is running more of the redundant instances", func() {
				BeforeEach(func() {
					initialDistributions[0] = generateUniqueSingleIndexLRPsWithRedundantSimulatedInstances(50, 1)
					initialDistributions[0] = append(initialDistributions[0], generateSimulatedInstancesForProcessGuidIndex(processGuid, 1, 0, 1)...)

					initialDistributions[1] = generateUniqueSingleIndexLRPsWithRedundantSimulatedInstances(30, 1)
					initialDistributions[1] = append(initialDistributions[1], generateSimulatedInstancesForProcessGuidIndex(processGuid, 2, 0, 1)...)
				})

				It("should stop all but one instance, keeping that one instance on the executor with more free resources", func() {
					stopAuctions := []models.LRPStopAuction{
						{
							ProcessGuid: processGuid,
							Index:       0,
						},
					}

					results := auctionDistributor.HoldStopAuctions(stopAuctions, repGuids)
					Ω(results).Should(HaveLen(1))
					Ω(results[0].Winner).Should(Equal("REP-2"))

					instancesOn0 := client.SimulatedInstances(repGuids[0])
					instancesOn1 := client.SimulatedInstances(repGuids[1])

					Ω(instancesOn0).Should(HaveLen(50))
					Ω(instancesOn1).Should(HaveLen(31))
				})
			})

			Context("when there are very many redundant instances out there, and the executors initially have the same amount of free resources", func() {
				BeforeEach(func() {
					initialDistributions[0] = generateSimulatedInstancesForProcessGuidIndex(processGuid, 50, 0, 1)
					initialDistributions[0] = append(initialDistributions[0], generateSimulatedInstancesForProcessGuidIndex(processGuid, 90-50, 1, 1)...)

					initialDistributions[1] = generateSimulatedInstancesForProcessGuidIndex(processGuid, 30, 0, 1)
					initialDistributions[1] = append(initialDistributions[1], generateSimulatedInstancesForProcessGuidIndex(processGuid, 90-30, 1, 1)...)

					initialDistributions[2] = generateSimulatedInstancesForProcessGuidIndex(processGuid, 70, 0, 1)
					initialDistributions[2] = append(initialDistributions[2], generateSimulatedInstancesForProcessGuidIndex(processGuid, 90-70, 1, 1)...)
				})

				It("should stop all but one instance, keeping that one instance on the executor that ends up with the most free resources", func() {
					stopAuctions := []models.LRPStopAuction{
						{
							ProcessGuid: processGuid,
							Index:       1,
						},
					}

					results := auctionDistributor.HoldStopAuctions(stopAuctions, repGuids)
					Ω(results).Should(HaveLen(1))
					Ω(results[0].Winner).Should(Equal("REP-2"))

					instancesOn0 := client.SimulatedInstances(repGuids[0])
					instancesOn1 := client.SimulatedInstances(repGuids[1])
					instancesOn2 := client.SimulatedInstances(repGuids[2])

					Ω(instancesOn0).Should(HaveLen(50))
					Ω(instancesOn1).Should(HaveLen(31))
					Ω(instancesOn2).Should(HaveLen(70))
				})
			})
		})
	})
})
