// Package redis allows reading/writing from/to a Redis DB.
package redis

import (
	"context"
	"fmt"
	"github.com/mediocregopher/radix/v3"
	"strconv"

	"github.com/stickermule/rump/pkg/message"
	"sync"
)

// Redis holds references to a DB pool and a shared message bus.
// Silent disables verbose mode.
// TTL enables TTL sync.
type Redis struct {
	Pool   *radix.Pool
	Bus    message.Bus
	Silent bool
	TTL    bool
	MaxTTL int
}

// New creates the Redis struct, used to read/write.
func New(source *radix.Pool, bus message.Bus, silent, ttl bool, maxTtl int) *Redis {
	return &Redis{
		Pool:   source,
		Bus:    bus,
		Silent: silent,
		TTL:    ttl,
		MaxTTL: maxTtl,
	}
}

// maybeLog may log, depending on the Silent flag
func (r *Redis) maybeLog(s string) {
	if r.Silent {
		return
	}
	fmt.Print(s)
}

// maybeTTL may sync the TTL, depending on the TTL flag
func (r *Redis) maybeTTL(key string) (string, error) {
	// noop if TTL is disabled, speeds up sync process
	if !r.TTL {
		return "0", nil
	}

	var ttl string

	// Try getting key TTL.
	err := r.Pool.Do(radix.Cmd(&ttl, "PTTL", key))

	if err != nil {
		return ttl, err
	}

	if 0 == r.MaxTTL {
		return ttl, nil
	}

	t, _ := strconv.ParseInt(ttl, 10, 0)

	maxTTL := r.MaxTTL * 1000
	if maxTTL < int(t) || ttl == "-1" {
		return strconv.Itoa(r.MaxTTL), nil
	}

	return ttl, nil
}

// Read gently scans an entire Redis DB for keys, then dumps
// the key/value pair (Payload) on the message Bus channel.
// It leverages implicit pipelining to speedup large DB reads.
// To be used in an ErrGroup.
func (r *Redis) Read(ctx context.Context, pattern string, count int) error {
	defer close(r.Bus)

	opts := radix.ScanOpts{
		Command: "SCAN",
		Pattern: pattern,
		Count:   count,
	}
	scanner := radix.NewScanner(r.Pool, opts)

	// Limit number of concurrent goroutines
	limit := make(chan struct{}, 1000)
	errors := make(chan error)

	defer close(limit)
	defer close(errors)

	var wg sync.WaitGroup
	var key string

	// Wait with closing the bus for all pending dumps
	defer wg.Wait()

	keys := 0
	// Scan and push to bus until no keys are left.
	// If context Done, exit early.
	for scanner.Next(&key) {
		select {
		case err := <-errors:
			return err
		case <-ctx.Done():
			return ctx.Err()
		case limit <- struct{}{}:
		}

		wg.Add(1)
		keys++
		go func(key string) {
			defer func() {
				<-limit
				wg.Add(-1)
			}()

			var value string
			var ttl string

			err := r.Pool.Do(radix.Cmd(&value, "DUMP", key))
			if err != nil {
				errors <- err
				return
			}

			ttl, err = r.maybeTTL(key)
			if err != nil {
				errors <- err
				return
			}

			select {
			case <-ctx.Done():
				fmt.Println("")
				fmt.Println("redis read: exit")
			case r.Bus <- message.Payload{Key: key, Value: value, TTL: ttl}:
				r.maybeLog("r")
			}
		}(key)
	}

	fmt.Println("")
	fmt.Println("migrated", keys, "keys")

	return scanner.Close()
}

// Write restores keys on the db as they come on the message bus.
func (r *Redis) Write(ctx context.Context) error {
	// Loop until channel is open
	for r.Bus != nil {
		select {
		// Exit early if context done.
		case <-ctx.Done():
			fmt.Println("")
			fmt.Println("redis write: exit")
			return ctx.Err()
		// Get Messages from Bus
		case p, ok := <-r.Bus:
			// if channel closed, set to nil, break loop
			if !ok {
				r.Bus = nil
				continue
			}

			err := r.Pool.Do(radix.Cmd(nil, "RESTORE", p.Key, p.TTL, p.Value, "REPLACE"))
			if err != nil {
				return err
			}
			r.maybeLog("w")
		}
	}

	return nil
}
