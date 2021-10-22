package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	fslock "github.com/ipfs/go-fs-lock"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

type dagSource struct {
	SourceID    int64
	Project     int
	CreatedAt   time.Time
	SourceLabel string
	Details     string
	Weight      *int
}
type dagSourceEntry struct {
	SourceID    *int64
	CidV1Str    string
	SourceKey   string
	SizeClaimed *int64
	CreatedAt   time.Time
	UpdatedAt   time.Time
	RemovedAt   *time.Time
	Details     string

	sourceLabel string
	cidV1       cid.Cid
}

// these two are the structs placed in the corresponding `details` JSONB
type dagSourceMeta struct {
	GithubID      json.Number `json:"github_id,omitempty" graphql:"github"`
	Name          string      `json:"name,omitempty"`
	Email         string      `json:"email,omitempty"`
	PublicAddress string      `json:"public_address,omitempty"` // FIXME why do we have this? SELECT * FROM public.user WHERE public_address IS NOT NULL AND public_address != SUBSTRING( magic_link_id, 10 )
	MagicLinkID   string      `json:"magic_link_id,omitempty" graphql:"issuer"`
	Picture       string      `json:"picture,omitempty"`
	UsedStorage   int64       `json:"storage_used,omitempty,string"`
	Nickname      string      `json:"nickname,omitempty" graphql:""`
}
type dagSourceEntryMeta struct {
	OriginalCid string       `json:"original_cid"`
	Label       string       `json:"label,omitempty"`
	UploadType  string       `json:"upload_type,omitempty"`
	TokenUsed   *sourceToken `json:"token_used,omitempty"`
	PinnedAt    *time.Time   `json:"pin_reported_at,omitempty"`
}
type sourceToken struct {
	ID    string `json:"id,omitempty" graphql:"_id"`
	Label string `json:"label,omitempty" graphql:"name"`
}

var getNewDags = &cli.Command{
	Usage: "Pull new CIDs from various sources",
	Name:  "get-new-dags",
	Flags: []cli.Flag{
		&cli.IntSliceFlag{
			Name:     "project",
			Usage:    "List of project ids to query",
			Required: true,
		},
		&cli.UintFlag{
			Name:  "skip-entries-aged",
			Usage: "Query the states of uploads and users last changed within that many days",
			Value: 3,
		},
	},
	Action: func(cctx *cli.Context) error {

		requestedProjects := make(map[int]struct{})
		for _, v := range cctx.IntSlice("project") {
			if _, known := projects[fmt.Sprintf("%d", v)]; !known {
				return xerrors.Errorf("unknown project '%d'", v)
			}
			projLock, err := fslock.Lock(os.TempDir(), fmt.Sprintf("cargocron-importdags-%d", v))
			if err != nil {
				return xerrors.Errorf("unable to obtain exlock for project %d: %w", v, err)
			}
			defer projLock.Close() //nolint:errcheck

			requestedProjects[v] = struct{}{}
		}

		log.Infow(fmt.Sprintf("=== BEGIN '%s' run", currentCmd))

		ctx, closer := context.WithCancel(cctx.Context)
		defer closer()

		// Sometimes we end up pinning something before cluster reports it as such
		// ( or we had something from a different source )
		knownCids, err := cidListFromQuery(
			ctx,
			`SELECT cid_v1 FROM cargo.dags`,
		)
		if err != nil {
			return err
		}

		// Need that list so we can filter them out
		ownAggregates, err := cidListFromQuery(
			ctx,
			`SELECT aggregate_cid FROM cargo.aggregates`,
		)
		if err != nil {
			return err
		}

		log.Infow("pre-selected", "knownCids", len(knownCids), "aggregateCids", len(ownAggregates))
		cutoffTime := time.Now().Add(time.Hour * -24 * time.Duration(cctx.Uint("skip-entries-aged")))

		var wg sync.WaitGroup
		errs := make(chan error, 256)

		for i := range pgProjects {
			p := pgProjects[i]
			if _, requested := requestedProjects[p.id]; !requested {
				continue
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				err := getPgDags(cctx, p, cutoffTime, knownCids, ownAggregates)
				if err != nil {
					errs <- err
				}
			}()
		}

		for i := range faunaProjects {
			p := faunaProjects[i]
			if _, requested := requestedProjects[p.id]; !requested {
				continue
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				err := getFaunaDags(cctx, p, cutoffTime, knownCids, ownAggregates)
				if err != nil {
					errs <- err
				}
			}()
		}

		wg.Wait()
		close(errs)
		return <-errs
	},
}
