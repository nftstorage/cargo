package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/hasura/go-graphql-client"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
)

// these two are the structs placed in the corresponding `details` JSONB
type dagSourceEntryMeta struct {
	OriginalCid   string       `json:"original_cid"`
	Label         string       `json:"label,omitempty"`
	UploadType    string       `json:"upload_type,omitempty"`
	TokenUsed     *sourceToken `json:"token_used,omitempty"`
	ClaimedSize   int64        `json:"claimed_size,omitempty,string"`
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
	ID    string `graphql:"_id" json:"id,omitempty"`
	Label string `graphql:"name" json:"label,omitempty"`
}

// query/result for grabbing user list
type faunaQueryUsers struct {
	FindUsersByCreated struct {
		CursorNext *string `graphql:"after"`
		Data       []faunaQueryUsersResult
	} `graphql:"findUsersByCreated (from: $since _cursor: $cursor_position _size: $page_size)"`
}
type faunaQueryUsersResult struct {
	UserID  string `graphql:"_id"`
	Created time.Time
	dagSourceMeta
}

// query/result for grabbing dag updates
type faunaQueryDags struct {
	FindUploadsCreatedAfter struct {
		CursorNext *string `graphql:"after"`
		Data       []faunaQueryDagsResult
	} `graphql:"findUploadsCreatedAfter (since: $since _cursor: $cursor_position _size: $page_size)"`
}
type faunaQueryDagsResult struct {
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
				Status  string
				Updated time.Time
			}
		}
	}
	User      faunaQueryUsersResult
	AuthToken sourceToken
}

var sysUserTime, _ = time.Parse("2006-01-02", "2021-08-01")
var sysUser = faunaQueryUsersResult{
	dagSourceMeta: dagSourceMeta{
		Name: "INTERNAL SYSTEM USER",
	},
	UserID:  "INTERNAL SYSTEM USER",
	Created: sysUserTime,
}

var getNewDags = &cli.Command{
	Usage: "Pull new CIDs from various sources",
	Name:  "get-new-dags",
	Flags: []cli.Flag{
		&cli.UintFlag{
			Name:  "skip-entries-aged",
			Usage: "Query the states of uploads and users last changed within that many days",
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

		for _, p := range faunaProjects {
			if err := getProjectDags(cctx, p, availableCids, ownAggregates); err != nil {
				return err
			}
		}

		// we got that far: all good
		// cleanup all records of the sysuser than are now properly attached to the original source
		_, err = db.Exec(
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
			sysUser.UserID,
		)
		return err
	},
}

func getProjectDags(cctx *cli.Context, project faunaProject, availableDags, ownAggregates map[cid.Cid]struct{}) error {

	cutoff := time.Now().Add(time.Hour * -24 * time.Duration(cctx.Uint("skip-entries-aged")))

	var newSources, totalQueryDags, newDags, newPending, removedDags int
	defer func() {
		log.Infow("summary",
			"project", project.label,
			"queryPeriodSince", cutoff,
			"totalQueryPeriodDags", totalQueryDags,
			"newSources", newSources,
			"newDags", newDags,
			"newPendingDags", newPending,
			"removedDags", removedDags,
		)
	}()

	// Theoretically one has a limit of 100k entries per page: that number won't work:
	//     Transaction does too much compute: got 12479, limit: 12000
	//
	// faunaPageSize = 99_999
	faunaPageSize := 9_999

	gql, err := faunaClient(cctx, project.label)
	if err != nil {
		return err
	}

	userLookup := make(map[string]int64)
	var cursorNext *graphql.String

	for {
		var resultPage faunaQueryUsers
		if err := gql.Query(cctx.Context, &resultPage, map[string]interface{}{
			"page_size":       graphql.Int(faunaPageSize),
			"cursor_position": cursorNext,
			"since":           cutoff,
		}); err != nil {
			return err
		}

		// pick up even users without uploads, for reporting purposes
		for _, u := range resultPage.FindUsersByCreated.Data {
			isNew, err := upsertUser(cctx.Context, project.id, userLookup, u)
			if err != nil {
				return err
			}
			if isNew {
				newSources++
			}
		}

		if resultPage.FindUsersByCreated.CursorNext == nil {
			break
		}
		cursorNext = (*graphql.String)(resultPage.FindUsersByCreated.CursorNext)
	}

	for {
		var resultPage faunaQueryDags
		if err := gql.Query(cctx.Context, &resultPage, map[string]interface{}{
			"page_size":       graphql.Int(faunaPageSize),
			"cursor_position": cursorNext,
			"since":           cutoff,
		}); err != nil {
			return err
		}

		for _, d := range resultPage.FindUploadsCreatedAfter.Data {

			c, err := cid.Parse(d.Content.CidString)
			if err != nil {
				return err
			}
			c = cidv1(c)

			// why would anyone be so mean?
			if _, own := ownAggregates[c]; own {
				continue
			}

			totalQueryDags++
			var pendingPinForUser string

			earliestPinSeen := new(time.Time)
			pinStates := make(map[string]int)
			for _, p := range d.Content.Pins.Data {
				pinStates[p.Status]++
				if p.Status == "Pinned" &&
					(earliestPinSeen.IsZero() || earliestPinSeen.After(p.Updated)) {
					*earliestPinSeen = p.Updated // copy
				}
			}
			if earliestPinSeen.IsZero() {
				// removes it from the JSON blob
				earliestPinSeen = nil
			}

			// we might already have the CID - amount of pins not relevant
			if _, avail := availableDags[c]; !avail {

				// Only consider things with at least one `Pinned` state
				// everything else may or may not be bogus
				if pinStates["Pinned"] == 0 {
					// we are *possibly* not going to find the data for this
					// but we also want to register it *in case* sweepers find it
					// to do that we swapout the user and proceed
					pendingPinForUser = d.User.UserID

					// create the user so we do not lose track of them
					isNew, err := upsertUser(cctx.Context, project.id, userLookup, d.User)
					if err != nil {
						return err
					}
					if isNew {
						newSources++
					}

					d.AuthToken = sourceToken{}
					d.User = sysUser
				}
			}

			isNew, err := upsertUser(cctx.Context, project.id, userLookup, d.User)
			if err != nil {
				return err
			}
			if isNew {
				newSources++
			}

			em := dagSourceEntryMeta{
				OriginalCid:   d.Content.CidString,
				Label:         d.Name,
				UploadType:    d.Type,
				ClaimedSize:   d.Content.DagSize,
				PinLagForUser: pendingPinForUser,
				PinnedAt:      earliestPinSeen,
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
				c.String(),
				d.Content.Created,
			)
			if err != nil {
				return err
			}

			var wasDeleted bool
			err = db.QueryRow(
				cctx.Context,
				`
				INSERT INTO cargo.dag_sources ( cid_v1, source_key, srcid, details, entry_created, entry_removed ) VALUES ( $1, $2, $3, $4, $5, $6 )
					ON CONFLICT ON CONSTRAINT singleton_dag_source_record DO UPDATE SET
						details = EXCLUDED.details,
						entry_created = EXCLUDED.entry_created,
						entry_removed = EXCLUDED.entry_removed
				RETURNING
					(xmax = 0),
					-- this select sees the table as it was before the upsert
					-- the COALESCE is needed in case of INSERTs - we won't find anything prior
					COALESCE (
						(
							SELECT resel.entry_removed IS NOT NULL
								FROM cargo.dag_sources resel
							WHERE
								resel.srcid = dag_sources.srcid
									AND
								resel.source_key = dag_sources.source_key
						),
						false
					)
				`,
				c.String(),
				d.ID,
				userLookup[d.User.UserID],
				entryMeta,
				d.Created,
				d.Deleted,
			).Scan(&isNew, &wasDeleted)
			if err != nil {
				return err
			}

			if d.Deleted != nil && !wasDeleted {
				removedDags++
			} else if isNew {
				if pendingPinForUser != "" {
					newPending++
				} else {
					newDags++
				}
			}
		}

		if resultPage.FindUploadsCreatedAfter.CursorNext == nil {
			break
		}
		cursorNext = (*graphql.String)(resultPage.FindUploadsCreatedAfter.CursorNext)
	}

	return nil
}

func upsertUser(ctx context.Context, projID int, seen map[string]int64, u faunaQueryUsersResult) (isNew bool, _ error) {

	// one upsert per round
	if _, seen := seen[u.UserID]; seen {
		return false, nil
	}

	sourceMeta, err := json.Marshal(dagSourceMeta{
		Github:        u.Github,
		Name:          u.Name,
		Email:         u.Email,
		PublicAddress: u.PublicAddress,
		Issuer:        u.Issuer,
		Picture:       u.Picture,
		UsedStorage:   u.UsedStorage,
	})
	if err != nil {
		return false, err
	}

	var srcid int64
	err = db.QueryRow(
		ctx,
		`
		INSERT INTO cargo.sources ( project, source, entry_created, details )
			VALUES ( $1, $2, $3, $4 )
			ON CONFLICT ( project, source ) DO UPDATE SET
				details = EXCLUDED.details
		RETURNING srcid, (xmax = 0)
		`,
		projID,
		u.UserID,
		u.Created,
		sourceMeta,
	).Scan(&srcid, &isNew)
	if err != nil {
		return false, err
	}

	seen[u.UserID] = srcid

	return isNew, nil
}
