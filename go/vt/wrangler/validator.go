package wrangler

import (
	"context"
	"errors"

	"vitess.io/vitess/go/vt/logutil"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// consumeValidationResults consumes results from Validate(Keyspace|Shard)? methods.
// If there are any results (synonymous with "validation failure") then the
// overall method returns a generic error instructing the user to look in the
// vtctld logs, and each validation failure is logged at the error level.
func consumeValidationResults(logger logutil.Logger, results []string) (err error) {
	for _, result := range results {
		if err == nil {
			err = errors.New("some validation errors - see log")
		}

		logger.Error(errors.New(result))
	}

	return err
}

// Validate a whole TopologyServer tree
func (wr *Wrangler) Validate(ctx context.Context, pingTablets bool) error {
	resp, err := wr.VtctldServer().Validate(ctx, &vtctldatapb.ValidateRequest{
		PingTablets: pingTablets,
	})
	if err != nil {
		return err
	}

	aggrResults := resp.Results
	for _, keyspaceResults := range resp.ResultsByKeyspace {
		aggrResults = append(aggrResults, keyspaceResults.Results...)
		for _, shardResults := range keyspaceResults.ResultsByShard {
			aggrResults = append(aggrResults, shardResults.Results...)
		}
	}

	return consumeValidationResults(wr.Logger(), aggrResults)
}

// ValidateKeyspace will validate a bunch of information in a keyspace
// is correct.
func (wr *Wrangler) ValidateKeyspace(ctx context.Context, keyspace string, pingTablets bool) error {
	resp, err := wr.VtctldServer().ValidateKeyspace(ctx, &vtctldatapb.ValidateKeyspaceRequest{
		Keyspace:    keyspace,
		PingTablets: pingTablets,
	})
	if err != nil {
		return err
	}

	aggrResults := resp.Results
	for _, shardResults := range resp.ResultsByShard {
		aggrResults = append(aggrResults, shardResults.Results...)
	}

	return consumeValidationResults(wr.Logger(), aggrResults)
}

// ValidateShard will validate a bunch of information in a shard is correct.
func (wr *Wrangler) ValidateShard(ctx context.Context, keyspace, shard string, pingTablets bool) error {
	resp, err := wr.VtctldServer().ValidateShard(ctx, &vtctldatapb.ValidateShardRequest{
		Keyspace:    keyspace,
		Shard:       shard,
		PingTablets: pingTablets,
	})
	if err != nil {
		return err
	}

	return consumeValidationResults(wr.Logger(), resp.Results)
}
