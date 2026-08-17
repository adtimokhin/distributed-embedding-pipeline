// Package config loads and provides the cluster configuration for a kvstore node.
//
// Each node is started with --id=<n> and --config=<path>. The config file is
// a JSON array of node descriptors. Each node has two addresses:
//   - ClientAddr: the address that clients (kvctl) use to call KVStore RPCs
//   - PeerAddr:   the address that other nodes use to call Replication RPCs
//
// This file is provided. Do not modify it.
package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// NodeConfig describes one node in the cluster.
type NodeConfig struct {
	ID         int32  `json:"id"`
	ClientAddr string `json:"client_addr"` // e.g. "localhost:7000"
	PeerAddr   string `json:"peer_addr"`   // e.g. "localhost:7100"
}

// ClusterConfig is the top-level structure parsed from nodeconfig.json.
type ClusterConfig struct {
	Nodes []NodeConfig `json:"nodes"`
}

// Load reads and parses the config file at path.
func Load(path string) (*ClusterConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	var cfg ClusterConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	if len(cfg.Nodes) == 0 {
		return nil, fmt.Errorf("config: %s contains no nodes", path)
	}
	return &cfg, nil
}

// Self returns the NodeConfig for the node with the given ID.
func (c *ClusterConfig) Self(id int32) (NodeConfig, error) {
	for _, n := range c.Nodes {
		if n.ID == id {
			return n, nil
		}
	}
	return NodeConfig{}, fmt.Errorf("config: no node with id %d", id)
}

// Peers returns all NodeConfigs except the one with the given ID.
func (c *ClusterConfig) Peers(id int32) []NodeConfig {
	var peers []NodeConfig
	for _, n := range c.Nodes {
		if n.ID != id {
			peers = append(peers, n)
		}
	}
	return peers
}
