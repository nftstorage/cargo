package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	fslock "github.com/ipfs/go-fs-lock"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var sysUserSource = "INTERNAL SYSTEM USER"
var sysUserCreateTime, _ = time.Parse("2006-01-02", "2021-08-01")

// these two are the structs placed in the corresponding `details` JSONB
type dagSourceEntryMeta struct {
	OriginalCid   string       `json:"original_cid"`
	Label         string       `json:"label,omitempty"`
	UploadType    string       `json:"upload_type,omitempty"`
	TokenUsed     *sourceToken `json:"token_used,omitempty"`
	ClaimedSize   *int64       `json:"claimed_size,omitempty,string"`
	PinnedAt      *time.Time   `json:"_pin_reported_at,omitempty"`
	PinLagForUser string       `json:"_lagging_pin_redirect_from_user,omitempty"`
}
type dagSourceMeta struct {
	Github        string `json:"github,omitempty"`
	Name          string `json:"name,omitempty"`
	Email         string `json:"email,omitempty"`
	PublicAddress string `json:"public_address,omitempty"`
	Issuer        string `json:"issuer,omitempty"`
	Picture       string `json:"picture,omitempty"`
	UsedStorage   int64  `json:"storage_used,omitempty,string"`
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

		ctx, closer := context.WithCancel(cctx.Context)
		defer closer()

		// Sometimes we end up pinning something before cluster reports it as such
		// ( or we had something from a different source )
		availableCids, err := cidListFromQuery(
			ctx,
			`SELECT cid_v1 FROM cargo.dags WHERE size_actual IS NOT NULL`,
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

		cutoffTime := time.Now().Add(time.Hour * -24 * time.Duration(cctx.Uint("skip-entries-aged")))

		var wg sync.WaitGroup
		errs := make(chan error, 256)

		for i := range faunaProjects {
			p := faunaProjects[i]
			if _, requested := requestedProjects[p.id]; !requested {
				continue
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				err := getFaunaDags(cctx, p, cutoffTime, availableCids, ownAggregates)
				if err != nil {
					errs <- err
				}
			}()
		}

		wg.Wait()
		close(errs)
		if err := <-errs; err != nil {
			return err
		}

		// we got that far without an error: all good
		// cleanup all records of the sysusers that are now properly attached to the original source
		// (if any )
		_, err = cargoDb.Exec(
			cctx.Context,
			`
			DELETE FROM cargo.dag_sources WHERE ( srcid, source_key ) IN (
				SELECT ds.srcid, ds.source_key
					FROM cargo.dag_sources ds
					JOIN cargo.sources s
						ON
							ds.srcid = s.srcid
								AND
							s.source = $1
					JOIN cargo.dag_sources subds
						ON ds.cid_v1 = subds.cid_v1
					JOIN cargo.sources subs
						ON
							subds.srcid = subs.srcid
								AND
							subs.project = s.project
								AND
							subs.source = ds.details->>'_lagging_pin_redirect_from_user'
			)
			`,
			sysUserSource,
		)
		return err
	},
}
