// Package brokerclient wraps the generated Broker gRPC client with
// leader-discovery and redirect-following logic, shared by the worker and
// producer.
//
// The broker is now a Raft-replicated cluster: Submit/Poll/Complete/Heartbeat
// only succeed against the current leader, and a non-leader node returns
// redirect_addr. Client does not trust that hostname on its own — a redirect
// target reported by a Docker-internal broker (e.g. "broker1:9000") is not
// necessarily reachable from wherever the caller lives (a host-invoked
// producer, say). Instead, a redirect is treated purely as a "try someone
// else" signal: Client round-robins through its own known address list,
// jumping straight to the reported address first only if it happens to
// already be one of the addresses the caller was configured with.
package brokerclient

import (
	"context"
	"fmt"
	"time"

	pb "pipeline/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	rpcTimeout   = 5 * time.Second
	retryBackoff = 50 * time.Millisecond
)

// Client dials broker cluster nodes on demand and follows redirects/failures
// to find the current leader. It is safe for concurrent use.
type Client struct {
	addrs   []string
	current int // index into addrs of the last address believed to be leader

	// conns caches one connection per address for the lifetime of the client.
	conns map[string]*grpc.ClientConn
}

// New creates a Client over the given broker addresses.
func New(addrs []string) *Client {
	return &Client{addrs: addrs, conns: make(map[string]*grpc.ClientConn)}
}

func (c *Client) clientFor(addr string) (pb.BrokerClient, error) {
	if conn, ok := c.conns[addr]; ok {
		return pb.NewBrokerClient(conn), nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	c.conns[addr] = conn
	return pb.NewBrokerClient(conn), nil
}

// addrIndex returns the index of addr in c.addrs, or -1 if not found.
func (c *Client) addrIndex(addr string) int {
	for i, a := range c.addrs {
		if a == addr {
			return i
		}
	}
	return -1
}

// call invokes fn against the current best-guess leader, following redirects
// and failures by advancing through the known address list. fn returns
// (redirectAddr, err); redirectAddr non-empty means "not leader, try this
// address instead" and err non-nil means "could not complete the call at
// all" (network error). Both are treated as reasons to try the next node.
func (c *Client) call(fn func(pb.BrokerClient) (redirectAddr string, err error)) error {
	maxHops := 2 * len(c.addrs)
	if maxHops < 2 {
		maxHops = 2
	}

	var lastErr error
	for hop := 0; hop < maxHops; hop++ {
		addr := c.addrs[c.current]
		client, err := c.clientFor(addr)
		if err != nil {
			lastErr = err
			c.current = (c.current + 1) % len(c.addrs)
			continue
		}

		redirect, err := fn(client)
		if err != nil {
			lastErr = err
			c.current = (c.current + 1) % len(c.addrs)
			time.Sleep(retryBackoff)
			continue
		}
		if redirect != "" {
			lastErr = fmt.Errorf("not leader (redirect to %s)", redirect)
			if idx := c.addrIndex(redirect); idx >= 0 {
				c.current = idx
			} else {
				c.current = (c.current + 1) % len(c.addrs)
			}
			continue
		}

		return nil // success
	}
	return fmt.Errorf("broker cluster unreachable after %d hops: %w", maxHops, lastErr)
}

// Submit submits a new task payload and returns its assigned task ID.
func (c *Client) Submit(ctx context.Context, payload string) (string, error) {
	var taskID string
	err := c.call(func(client pb.BrokerClient) (string, error) {
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		defer cancel()
		resp, err := client.Submit(rctx, &pb.SubmitRequest{Payload: payload})
		if err != nil {
			return "", err
		}
		if !resp.Ok {
			return resp.RedirectAddr, nil
		}
		taskID = resp.TaskId
		return "", nil
	})
	return taskID, err
}

// Poll asks the leader for one task to work on. ok is false if the queue is
// currently empty (not an error — caller should sleep and poll again).
func (c *Client) Poll(ctx context.Context, workerID string) (task *pb.Task, ok bool, err error) {
	err = c.call(func(client pb.BrokerClient) (string, error) {
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		defer cancel()
		resp, err := client.Poll(rctx, &pb.PollRequest{WorkerId: workerID})
		if err != nil {
			return "", err
		}
		if !resp.HasTask && resp.RedirectAddr != "" {
			return resp.RedirectAddr, nil
		}
		task, ok = resp.Task, resp.HasTask
		return "", nil
	})
	return task, ok, err
}

// Complete reports that workerID finished taskID (errMsg empty on success).
func (c *Client) Complete(ctx context.Context, taskID, workerID, errMsg string) error {
	return c.call(func(client pb.BrokerClient) (string, error) {
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		defer cancel()
		resp, err := client.Complete(rctx, &pb.CompleteRequest{TaskId: taskID, WorkerId: workerID, Error: errMsg})
		if err != nil {
			return "", err
		}
		if !resp.Ok {
			return resp.RedirectAddr, nil
		}
		return "", nil
	})
}

// Heartbeat reports that workerID is still alive and working on taskID.
func (c *Client) Heartbeat(ctx context.Context, workerID, taskID string) error {
	return c.call(func(client pb.BrokerClient) (string, error) {
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		defer cancel()
		resp, err := client.Heartbeat(rctx, &pb.HeartbeatRequest{WorkerId: workerID, TaskId: taskID})
		if err != nil {
			return "", err
		}
		if !resp.Ok {
			return resp.RedirectAddr, nil
		}
		return "", nil
	})
}

// GetResult reports whether taskID is done and, if so, any error. Served by
// any node from local (possibly slightly stale) replicated state — no
// redirect needed.
func (c *Client) GetResult(ctx context.Context, taskID string) (done bool, errMsg string, err error) {
	err = c.call(func(client pb.BrokerClient) (string, error) {
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		defer cancel()
		resp, err := client.GetResult(rctx, &pb.GetResultRequest{TaskId: taskID})
		if err != nil {
			return "", err
		}
		done, errMsg = resp.Done, resp.Error
		return "", nil
	})
	return done, errMsg, err
}
