package auctiondistributor

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/auction/util"
)

type httpRemoteAuctions struct {
	hosts []string
}

func newHttpRemoteAuctions(hosts []string) *httpRemoteAuctions {
	return &httpRemoteAuctions{hosts}
}

func (h *httpRemoteAuctions) RemoteStartAuction(auctionRequest auctiontypes.StartAuctionRequest) auctiontypes.StartAuctionResult {
	host := h.hosts[util.R.Intn(len(h.hosts))]

	payload, _ := json.Marshal(auctionRequest)
	res, err := http.Post("http://"+host+"/start-auction", "application/json", bytes.NewReader(payload))
	if err != nil {
		return auctiontypes.StartAuctionResult{Error: err}
	}

	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return auctiontypes.StartAuctionResult{Error: err}
	}

	var result auctiontypes.StartAuctionResult
	json.Unmarshal(data, &result)

	return result
}

func (h *httpRemoteAuctions) RemoteStopAuction(auctionRequest auctiontypes.StopAuctionRequest) auctiontypes.StopAuctionResult {
	host := h.hosts[util.R.Intn(len(h.hosts))]

	payload, _ := json.Marshal(auctionRequest)
	res, err := http.Post("http://"+host+"/stop-auction", "application/json", bytes.NewReader(payload))
	if err != nil {
		return auctiontypes.StopAuctionResult{Error: err}
	}

	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return auctiontypes.StopAuctionResult{Error: err}
	}

	var result auctiontypes.StopAuctionResult
	json.Unmarshal(data, &result)

	return result
}
