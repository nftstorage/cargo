package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/hasura/go-graphql-client"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
)

type faunaProject struct {
	id    int
	label string
}

var faunaProjects = []faunaProject{
	{id: 0, label: "w3s-stage"},
	{id: 1, label: "w3s-prod"},
}

// query/result for grabbing user list
type faunaQueryUsers struct {
	FindUsersByCreated struct {
		CursorNext *string `graphql:"after"`
		Data       []faunaQueryUsersResult
	} `graphql:"findUsersByCreated (from: $since _cursor: $cursor_position _size: $page_size)"`
}
type faunaQueryUsersResult struct {
	UserID    string    `graphql:"_id"`
	CreatedAt time.Time `graphql:"created"`
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
	ID        string     `graphql:"_id"`
	CreatedAt time.Time  `graphql:"created"`
	RemovedAt *time.Time `graphql:"deleted"`
	Name      string
	Type      string
	Content   struct {
		CidString string    `graphql:"cid"`
		CreatedAt time.Time `graphql:"created"`
		DagSize   *int64
		Pins      struct {
			Data []struct {
				Status    string
				UpdatedAt time.Time `graphql:"updated"`
			}
		}
	}
	User      faunaQueryUsersResult
	AuthToken sourceToken
}

func getFaunaDags(cctx *cli.Context, project faunaProject, cutoff time.Time, knownDags, ownAggregates map[cid.Cid]struct{}) error {

	var mostRecentDag *time.Time
	var newSources, totalQueryDags, newDags, removedDags int
	defer func() {
		log.Infow("summary",
			"project", project.label,
			"queryPeriodSince", cutoff,
			"totalQueryPeriodDags", totalQueryDags,
			"mostRecentDag", mostRecentDag,
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
			isNew, err := upsertFaunaUser(cctx.Context, project.id, userLookup, u)
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

			mostRecentDag = &d.CreatedAt

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

			earliestPinSeen := new(time.Time)
			pinStates := make(map[string]int)
			for _, p := range d.Content.Pins.Data {
				pinStates[p.Status]++
				if p.Status == "Pinned" &&
					(earliestPinSeen.IsZero() || earliestPinSeen.After(p.UpdatedAt)) {
					*earliestPinSeen = p.UpdatedAt // copy
				}
			}
			if earliestPinSeen.IsZero() {
				// removes it from the JSON blob
				earliestPinSeen = nil
			}

			isNew, err := upsertFaunaUser(cctx.Context, project.id, userLookup, d.User)
			if err != nil {
				return err
			}
			if isNew {
				newSources++
			}

			em := dagSourceEntryMeta{
				OriginalCid: d.Content.CidString,
				Label:       d.Name,
				UploadType:  d.Type,
				PinnedAt:    earliestPinSeen,
			}
			if d.AuthToken.ID != "" {
				em.TokenUsed = &d.AuthToken
			}
			entryMeta, err := json.Marshal(em)
			if err != nil {
				return err
			}

			if _, known := knownDags[c]; !known {
				_, err = cargoDb.Exec(
					cctx.Context,
					`
					INSERT INTO cargo.dags ( cid_v1, entry_created ) VALUES ( $1, $2 )
						ON CONFLICT DO NOTHING
					`,
					c.String(),
					d.Content.CreatedAt,
				)
				if err != nil {
					return err
				}
			}

			var wasRemoved bool
			err = cargoDb.QueryRow(
				cctx.Context,
				`
				INSERT INTO cargo.dag_sources ( cid_v1, source_key, srcid, size_claimed, details, entry_created, entry_removed ) VALUES ( $1, $2, $3, $4, $5, $6, $7 )
					ON CONFLICT ( srcid, source_key ) DO UPDATE SET
						size_claimed = EXCLUDED.size_claimed,
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
				d.Content.DagSize,
				entryMeta,
				d.CreatedAt,
				d.RemovedAt,
			).Scan(&isNew, &wasRemoved)
			if err != nil {
				return err
			}

			if d.RemovedAt != nil && !wasRemoved {
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

func upsertFaunaUser(ctx context.Context, projID int, seen map[string]int64, u faunaQueryUsersResult) (isNew bool, _ error) {

	// one upsert per round
	if _, seen := seen[u.UserID]; seen {
		return false, nil
	}

	sourceMeta, err := json.Marshal(dagSourceMeta{
		GithubID:      u.GithubID,
		Name:          u.Name,
		Email:         u.Email,
		PublicAddress: u.PublicAddress,
		MagicLinkID:   u.MagicLinkID,
		Picture:       u.Picture,
		UsedStorage:   u.UsedStorage,
	})
	if err != nil {
		return false, err
	}

	var srcid int64
	err = cargoDb.QueryRow(
		ctx,
		`
		INSERT INTO cargo.sources ( project, source_label, entry_created, details )
			VALUES ( $1, $2, $3, $4 )
			ON CONFLICT ( project, source_label ) DO UPDATE SET
				details = EXCLUDED.details
		RETURNING srcid, (xmax = 0)
		`,
		projID,
		u.UserID,
		u.CreatedAt,
		sourceMeta,
	).Scan(&srcid, &isNew)
	if err != nil {
		return false, err
	}

	seen[u.UserID] = srcid

	return isNew, nil
}
