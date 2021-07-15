package main

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cloudflare/cloudflare-go"
	"github.com/davecgh/go-spew/spew"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var nftKeyparse = regexp.MustCompile(`^\s*(.+):([^:\s]+)\s*$`)

var getNewNftCids = &cli.Command{
	Usage: "Pull new CIDs from nft.storage",
	Name:  "get-new-nfts",
	Action: func(cctx *cli.Context) error {

		ctx, closer := context.WithCancel(cctx.Context)
		defer closer()

		db, err := connectDb(cctx)
		if err != nil {
			return err
		}

		nftKvID := cctx.String("cf-kvnamespace-nfts")
		if nftKvID == "" {
			return xerrors.New("config `cf-kvnamespace-nfts` is not set")
		}
		userKvID := cctx.String("cf-kvnamespace-users")
		if userKvID == "" {
			return xerrors.New("config `cf-kvnamespace-users` is not set")
		}

		api, err := cfAPI(cctx)
		if err != nil {
			return err
		}

		// kick off pulling in the background, while we pull out our current set
		errCh := make(chan error, 2)
		resCh := make(chan cloudflare.StorageKey, bufPresize)
		go listAllNftKeys(ctx, api, nftKvID, resCh, errCh)

		knownSources := make(map[string]struct{}, bufPresize)
		rows, err := db.Query(
			ctx,
			`SELECT source FROM cargo.sources WHERE details IS NOT NULL`,
		)
		if err != nil {
			return err
		}
		var src string
		for rows.Next() {
			if err = rows.Scan(&src); err != nil {
				return err
			}
			knownSources[src] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			return err
		}

		initiallyInDb := make(map[[2]string]struct{}, bufPresize)
		rows, err = db.Query(
			ctx,
			`SELECT cid_original, source FROM cargo.dag_sources WHERE entry_removed IS NULL`,
		)
		if err != nil {
			return err
		}
		var orig string
		for rows.Next() {
			if err = rows.Scan(&orig, &src); err != nil {
				return err
			}
			initiallyInDb[[2]string{orig, src}] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			return err
		}

		ownAggregates := make(map[string]struct{}, 1<<10)
		rows, err = db.Query(
			ctx,
			`SELECT batch_cid FROM cargo.batches`,
		)
		if err != nil {
			return err
		}
		var agg string
		for rows.Next() {
			if err = rows.Scan(&agg); err != nil {
				return err
			}
			ownAggregates[agg] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			return err
		}

		log.Infof("loaded %d already-known sources and %d cid+source pairs", len(knownSources), len(initiallyInDb))

		var lastPct, seen, new, newSources, removed int
		projected := len(initiallyInDb)
		initially := projected

		defer func() {
			log.Infow("summary",
				"totalDags", seen,
				"initialDags", initially,
				"newDags", new,
				"newSources", newSources,
				"removedDags", removed,
			)
		}()

		// main loop parsing and inserting
		for {
			r, isOpen := <-resCh
			if !isOpen {
				break
			}

			if len(errCh) > 0 {
				return <-errCh
			}

			seen++

			if showProgress && projected > 0 && 100*seen/projected != lastPct {
				lastPct = 100 * seen / projected
				fmt.Fprintf(os.Stderr, "%d%%\r", lastPct)
			}

			keyParts := nftKeyparse.FindStringSubmatch(r.Name)
			if len(keyParts) != 3 {
				return xerrors.Errorf("Unable to parse key '%s': %d matches found", r.Name, len(keyParts)-1)
			}

			if _, ourOwn := ownAggregates[keyParts[2]]; ourOwn {
				// someone is pulling our leg
				continue
			}

			source := keyParts[1]
			cidOriginal, err := cid.Parse(keyParts[2])
			if err != nil {
				return xerrors.Errorf("failed parsing user cid '%s': %w", keyParts[2], err)
			}

			if _, found := initiallyInDb[[2]string{cidOriginal.String(), source}]; found {
				delete(initiallyInDb, [2]string{cidOriginal.String(), source})
				continue
			}

			projected++
			new++

			cidNormStr := cidv1(cidOriginal).String()

			var sizeClaimed *int64
			var entryCreated *time.Time

			if r.Metadata != nil {
				m, castOk := r.Metadata.(map[string]interface{})
				if !castOk {
					return xerrors.Errorf("unexpected metadata shape:\n%s\n", spew.Sdump(r.Metadata))
				}

				if ctime, ctimeFound := m["created"]; ctimeFound && ctime != nil {
					t, err := time.Parse(time.RFC3339Nano, fmt.Sprintf("%v", ctime))
					if err != nil {
						return xerrors.Errorf("unexpected created time '%s'", ctime)
					}
					entryCreated = &t
				}

				if size, sizeFound := m["size"]; sizeFound && size != nil {
					s, err := strconv.ParseFloat(fmt.Sprintf("%v", size), 64)
					if err != nil {
						return xerrors.Errorf("unexpected claimed size '%s'", size)
					}
					si := int64(s)
					sizeClaimed = &si
				}
			}

			_, err = db.Exec(
				ctx,
				`
				INSERT INTO cargo.dags ( cid_v1, size_claimed, entry_created ) VALUES ( $1, $2, COALESCE( $3, NOW() ) )
					ON CONFLICT DO NOTHING
				`,
				cidNormStr,
				sizeClaimed,
				entryCreated,
			)
			if err != nil {
				return err
			}

			if _, known := knownSources[source]; !known {

				sourceDetails, err := api.ReadWorkersKV(ctx, userKvID, source)
				if err != nil {
					log.Warnf("failure retrieving details for source '%s': %s", source, err)
				} else {
					newSources++
				}

				_, err = db.Exec(
					ctx,
					`
					INSERT INTO cargo.sources ( source, details ) VALUES ( $1, $2 )
						ON CONFLICT ( source ) DO UPDATE SET details = COALESCE( sources.details, EXCLUDED.details ) -- no overwrites
					`,
					source,
					sourceDetails,
				)
				if err != nil {
					return err
				}

				knownSources[source] = struct{}{}
			}

			_, err = db.Exec(
				ctx,
				`
				INSERT INTO cargo.dag_sources ( cid_v1, cid_original, source, entry_created ) VALUES ( $1, $2, $3, COALESCE( $4, NOW() ) )
					ON CONFLICT ON CONSTRAINT singleton_source_record DO UPDATE SET entry_removed = NULL
				`,
				cidNormStr,
				cidOriginal.String(),
				source,
				entryCreated,
			)
			if err != nil {
				return err
			}
		}

		if err := <-errCh; err != nil {
			return err
		}

		removed = len(initiallyInDb)
		if removed > 0 {
			for k := range initiallyInDb {
				if _, err = db.Exec(
					ctx,
					`
					UPDATE cargo.dag_sources SET entry_removed = NOW() WHERE cid_original = $1 AND source = $2
					`,
					k[0], k[1],
				); err != nil {
					return err
				}
			}
		}

		return nil
	},
}

func listAllNftKeys(ctx context.Context, api *cloudflare.API, nftKvID string, resCh chan<- cloudflare.StorageKey, errCh chan<- error) {
	defer close(resCh)
	defer close(errCh)

	var nextPageCursor string

	for {
		opts := cloudflare.ListWorkersKVsOptions{}
		if nextPageCursor != "" {
			opts.Cursor = &nextPageCursor
		}

		resp, err := api.ListWorkersKVsWithOptions(ctx, nftKvID, opts)
		if err != nil {
			errCh <- err
			return
		}

		notOk := make([]string, 0)
		if len(resp.Errors) > 0 {
			notOk = append(notOk, "\nErrors:")
			for _, e := range resp.Errors {
				notOk = append(notOk, "\t"+e.Message)
			}
		}
		if len(resp.Messages) > 0 {
			notOk = append(notOk, "\nUnexpected Messages:")
			for _, m := range resp.Messages {
				notOk = append(notOk, "\t"+m.Message)
			}
		}
		if len(notOk) > 0 {
			errCh <- xerrors.New(strings.Join(notOk, "\n"))
			return
		}

		for _, r := range resp.Result {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case resCh <- r:
				// feeder
			}
		}

		if resp.Cursor == "" {
			break
		}
		nextPageCursor = resp.Cursor
	}
}
