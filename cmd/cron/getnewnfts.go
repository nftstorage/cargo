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
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var nftKeyparse = regexp.MustCompile(`^\s*(.+):([^:\s]+)\s*$`)

var getNewNftCids = &cli.Command{
	Usage: "Pull new CIDs from nft.storage",
	Name:  "get-new-nfts",
	Action: func(cctx *cli.Context) error {

		log.Info("starting new nft poll")

		ctx, closer := context.WithCancel(cctx.Context)
		defer closer()

		dbConnCfg, err := pgxpool.ParseConfig(cctx.String("pg-connstring"))
		if err != nil {
			return err
		}
		db, err := pgxpool.ConnectConfig(ctx, dbConnCfg)
		if err != nil {
			return err
		}

		nftKvId := cctx.String("cf-kvnamespace-nfts")
		if nftKvId == "" {
			return xerrors.New("config `cf-kvnamespace-nfts` is not set")
		}

		api, err := cfApi(cctx)
		if err != nil {
			return err
		}

		// kick off pulling in the background, while we pull out our current set
		errCh := make(chan error, 1)
		resCh := make(chan cloudflare.StorageKey, bufPresize)
		go listAllNftKeys(ctx, api, nftKvId, resCh, errCh)

		alreadyKnown := make(map[string]struct{}, bufPresize)

		rows, err := db.Query(
			ctx,
			`SELECT cid_original, source FROM cargo.sources`,
		)
		if err != nil {
			return err
		}
		var orig, src string
		for rows.Next() {
			if err = rows.Scan(&orig, &src); err != nil {
				return err
			}
			alreadyKnown[orig+src] = struct{}{}
		}

		log.Infof("retrieved %d already known cid+source pairs", len(alreadyKnown))

		var lastPct, seen, new int64
		projected := int64(len(alreadyKnown))

		defer func() { log.Infof("finished: %d total, %d new CIDs", seen, new) }()
		if ShowProgress {
			defer fmt.Fprint(os.Stderr, "100%\n")
		}

	listDone:
		for {
			r, isOpen := <-resCh
			if !isOpen {
				break listDone
			}

			if len(errCh) > 0 {
				return <-errCh
			}

			seen++

			if ShowProgress && projected > 0 && 100*seen/projected != lastPct {
				lastPct = 100 * seen / projected
				fmt.Fprintf(os.Stderr, "%d%%\r", lastPct)
			}

			keyParts := nftKeyparse.FindStringSubmatch(r.Name)
			if len(keyParts) != 3 {
				return xerrors.Errorf("Unable to parse key '%s': %d matches found", r.Name, len(keyParts)-1)
			}

			source := keyParts[1]
			cidOriginal, err := cid.Parse(keyParts[2])
			if err != nil {
				return xerrors.Errorf("failed parsing user cid '%s': %w", keyParts[2], err)
			}

			if _, found := alreadyKnown[cidOriginal.String()+source]; found {
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

			_, err = db.Exec(
				ctx,
				`
				INSERT INTO cargo.sources ( cid_v1, cid_original, source, entry_created ) VALUES ( $1, $2, $3, COALESCE( $4, NOW() ) )
					ON CONFLICT DO NOTHING
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
		return <-errCh
	},
}

func listAllNftKeys(ctx context.Context, api *cloudflare.API, nftKvId string, resCh chan<- cloudflare.StorageKey, errCh chan<- error) {
	defer close(resCh)
	defer close(errCh)

	var nextPageCursor string

	for {
		opts := cloudflare.ListWorkersKVsOptions{}
		if nextPageCursor != "" {
			opts.Cursor = &nextPageCursor
		}

		resp, err := api.ListWorkersKVsWithOptions(ctx, nftKvId, opts)
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
			resCh <- r
		}

		if resp.Cursor == "" {
			break
		}
		nextPageCursor = resp.Cursor
	}
}
