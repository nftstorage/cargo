package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/hasura/go-graphql-client"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
)

var faunaProjects = map[int]string{
	0: "w3s-stage",
	1: "w3s-prod",
}

// these two are the structs placed in the corresponding `details` JSONB
type dagSourceEntryMeta struct {
	Label      string       `json:"label,omitempty"`
	UploadType string       `json:"upload_type,omitempty"`
	TokenUsed  *sourceToken `json:"token_used,omitempty"`
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
	ID    string `graphql:"_id" json:"id,omitempty"`
	Label string `graphql:"name" json:"label,omitempty"`
}

// query/result for grabbing updates
type dagsQuery struct {
	FindUploadsCreatedAfter struct {
		CursorNext *string `graphql:"after"`
		Data       []struct {
			ID      string `graphql:"_id"`
			Created time.Time
			Deleted *time.Time
			Name    string
			Type    string
			Content struct {
				CidString string `graphql:"cid"`
				Created   time.Time
				DagSize   int64
				Pins      struct {
					Data []struct {
						Status string
					}
				}
			}
			User struct {
				UserID  string `graphql:"_id"`
				Created time.Time
				dagSourceMeta
			}
			AuthToken sourceToken
		}
	} `graphql:"findUploadsCreatedAfter (since: $since _cursor: $cursor_position _size: $page_size)"`
}

var getNewDags = &cli.Command{
	Usage: "Pull new CIDs from various sources",
	Name:  "get-new-dags",
	Flags: []cli.Flag{
		&cli.UintFlag{
			Name:  "skip-uploads-aged",
			Usage: "Query the states of uploads last changed within that many days",
			Value: 3,
		},
	},
	Action: func(cctx *cli.Context) error {

		ctx, closer := context.WithCancel(cctx.Context)
		defer closer()

		// Sometimes we end up pinning something before cluster reports it as such
		// ( or we had something from a different sourec )
		// Grab a list so we do not turn away folks we definitely can serve
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

		for pID := range faunaProjects {
			if err := getProjectDags(cctx, pID, availableCids, ownAggregates); err != nil {
				return err
			}
		}

		return nil
	},
}

func getProjectDags(cctx *cli.Context, projectNum int, availableDags, ownAggregates map[cid.Cid]struct{}) error {

	cutoff := time.Now().Add(time.Hour * -24 * time.Duration(cctx.Uint("skip-uploads-aged")))

	var newSources, totalQueryDags, newDags, removedDags int
	defer func() {
		log.Infow("summary",
			"project", faunaProjects[projectNum],
			"queryPeriodSince", cutoff,
			"totalQueryPeriodDags", totalQueryDags,
			"newSources", newSources,
			"newDags", newDags,
			"removedDags", removedDags,
		)
	}()

	// Theoretically one has a limit of 100k entries per page: that number won't work:
	//     Transaction does too much compute: got 12479, limit: 12000
	//
	// faunaPageSize = 99_999
	faunaPageSize := 9_999

	gql, err := faunaClient(cctx, faunaProjects[projectNum])
	if err != nil {
		return err
	}
	var cursorNext *graphql.String
	for {
		var resultPage dagsQuery
		if err := gql.Query(cctx.Context, &resultPage, map[string]interface{}{
			"page_size":       graphql.Int(faunaPageSize),
			"cursor_position": cursorNext,
			"since":           cutoff,
		}); err != nil {
			return err
		}

		seenSources := make(map[string]int64)

		for _, d := range resultPage.FindUploadsCreatedAfter.Data {

			c, err := cid.Parse(d.Content.CidString)
			if err != nil {
				return err
			}

			// why would anyone be so mean?
			if _, own := ownAggregates[cidv1(c)]; own {
				continue
			}

			// we might already have the CID - amount of pins not relevant
			if _, avail := availableDags[cidv1(c)]; !avail {
				// Only consider things with at least one `Pinned` state
				// everything else may or may not be bogus
				pinStates := make(map[string]int)
				for _, p := range d.Content.Pins.Data {
					pinStates[p.Status]++
				}
				if pinStates["Pinned"] == 0 {
					// we are unlikely to find the data for this
					continue
				}
			}

			totalQueryDags++

			if _, known := seenSources[d.User.UserID]; !known {
				sourceMeta, err := json.Marshal(dagSourceMeta{
					Github:        d.User.Github,
					Name:          d.User.Name,
					Email:         d.User.Email,
					PublicAddress: d.User.PublicAddress,
					Issuer:        d.User.Issuer,
					Picture:       d.User.Picture,
					UsedStorage:   d.User.UsedStorage,
				})
				if err != nil {
					return err
				}

				var srcid int64
				var isNew bool
				err = db.QueryRow(
					cctx.Context,
					`
					INSERT INTO cargo.sources ( project, source, entry_created, details )
						VALUES ( $1, $2, $3, $4 )
						ON CONFLICT ( project, source ) DO UPDATE SET
							details = EXCLUDED.details
					RETURNING srcid, (xmax = 0)
					`,
					projectNum,
					d.User.UserID,
					d.User.Created,
					sourceMeta,
				).Scan(&srcid, &isNew)
				if err != nil {
					return err
				}

				if isNew {
					newSources++
				}

				seenSources[d.User.UserID] = srcid
			}

			em := dagSourceEntryMeta{
				Label:      d.Name,
				UploadType: d.Type,
			}
			if d.AuthToken.ID != "" {
				em.TokenUsed = &d.AuthToken
			}
			entryMeta, err := json.Marshal(em)
			if err != nil {
				return err
			}

			_, err = db.Exec(
				cctx.Context,
				`
				INSERT INTO cargo.dags ( cid_v1, entry_created ) VALUES ( $1, $2 )
					ON CONFLICT DO NOTHING
				`,
				cidv1(c).String(),
				d.Content.Created,
			)
			if err != nil {
				return err
			}

			var isNew bool
			var entryLastUpdate time.Time
			err = db.QueryRow(
				cctx.Context,
				`
				INSERT INTO cargo.dag_sources ( cid_v1, entry_id, srcid, details, entry_created, entry_removed ) VALUES ( $1, $2, $3, $4, $5, $6 )
					ON CONFLICT ON CONSTRAINT singleton_dag_source_record DO UPDATE SET
						entry_removed = $6
				RETURNING (xmax = 0), entry_last_updated
				`,
				cidv1(c).String(),
				d.ID,
				seenSources[d.User.UserID],
				entryMeta,
				d.Created,
				d.Deleted,
			).Scan(&isNew, &entryLastUpdate)
			if err != nil {
				return err
			}

			if d.Deleted != nil && time.Since(entryLastUpdate) < 5*time.Second {
				removedDags++
			} else if isNew {
				newDags++
			}
		}

		if resultPage.FindUploadsCreatedAfter.CursorNext == nil {
			break
		}
		cursorNext = (*graphql.String)(resultPage.FindUploadsCreatedAfter.CursorNext)
	}

	return nil
}
