package tabletmanager

import (
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/gossip"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/utils"
)

type gossipConfig struct {
	enabled      bool
	listenAddr   string
	seedAddrs    []string
	phiThreshold float64
	pingInterval time.Duration
	probeTimeout time.Duration
	maxUpdateAge time.Duration
}

var vttabletGossipConfig gossipConfig

func init() {
	servenv.OnParseFor("vttablet", func(fs *pflag.FlagSet) {
		utils.SetFlagStringVar(fs, &vttabletGossipConfig.listenAddr, "gossip-listen-addr", vttabletGossipConfig.listenAddr, "Address to bind gossip gRPC server (defaults to grpc bind addr)")
		utils.SetFlagStringSliceVar(fs, &vttabletGossipConfig.seedAddrs, "gossip-seed-addrs", vttabletGossipConfig.seedAddrs, "Comma-separated list of gossip seed addresses")
		utils.SetFlagBoolVar(fs, &vttabletGossipConfig.enabled, "gossip-enabled", vttabletGossipConfig.enabled, "Enable gossip protocol participation")
		utils.SetFlagFloat64Var(fs, &vttabletGossipConfig.phiThreshold, "gossip-phi-threshold", 4, "Phi accrual threshold for suspecting peers")
		utils.SetFlagDurationVar(fs, &vttabletGossipConfig.pingInterval, "gossip-ping-interval", 1*time.Second, "Gossip ping interval")
		utils.SetFlagDurationVar(fs, &vttabletGossipConfig.probeTimeout, "gossip-probe-timeout", 500*time.Millisecond, "Gossip probe timeout")
		utils.SetFlagDurationVar(fs, &vttabletGossipConfig.maxUpdateAge, "gossip-max-update-age", 5*time.Second, "Max age before marking peer down")
	})
}

func (cfg gossipConfig) agent(nodeID string, grpcAddr string) *gossip.Gossip {
	if !cfg.enabled {
		return nil
	}

	seeds := make([]gossip.Member, 0, len(cfg.seedAddrs))
	for _, addr := range cfg.seedAddrs {
		if addr == "" {
			continue
		}
		seeds = append(seeds, gossip.Member{ID: gossip.NodeID(addr), Addr: addr})
	}

	bindAddr := cfg.listenAddr
	if bindAddr == "" {
		bindAddr = grpcAddr
	}

	transport := gossip.NewGRPCTransport(gossip.GRPCDialer{})

	return gossip.New(gossip.Config{
		NodeID:       gossip.NodeID(nodeID),
		BindAddr:     bindAddr,
		Seeds:        seeds,
		PhiThreshold: cfg.phiThreshold,
		PingInterval: cfg.pingInterval,
		ProbeTimeout: cfg.probeTimeout,
		MaxUpdateAge: cfg.maxUpdateAge,
	}, transport, nil)
}
