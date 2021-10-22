package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/urfave/cli/v2"
)

type pgProject struct {
	id           int
	label        string
	pgConnString string
}

var pgProjects = []pgProject{
	{
		id:           2,
		label:        "nft-storage",
		pgConnString: "service=nft-storage-ro",
	},
}

func getPgDags(cctx *cli.Context, p pgProject, cutoff time.Time, knownDags, ownAggregates map[cid.Cid]struct{}) error {
	ctx, closer := context.WithCancel(cctx.Context)
	defer closer()

	remoteDags := make(map[[2]string]*dagSourceEntry, 1<<20)
	var totalQueriedDags, existingDags, newSources, newDags, removedDags int
	defer func() {
		log.Infow("summary",
			"project", p.label,
			"existingDags", existingDags,
			"queryPeriodSince", cutoff,
			"totalQueryPeriodDags", totalQueriedDags,
			"totalUpsertedDags", len(remoteDags),
			"newSources", newSources,
			"newDags", newDags,
			"removedDags", removedDags,
		)
	}()

	projPrefix := fmt.Sprintf("project %s (%d)", p.label, p.id)

	srcDbConn, err := pgxpool.ParseConfig(p.pgConnString)
	if err != nil {
		return err
	}
	srcDb, err := pgxpool.ConnectConfig(ctx, srcDbConn)
	if err != nil {
		return err
	}
	defer srcDb.Close()

	// first pull all dag rows, store in ram, users come second
	dagRows, err := srcDb.Query(
		ctx,
		`
		SELECT
				ds.user_id::TEXT AS source_label,
				ds.content_cid AS cid_v1,
				ds.source_cid AS source_key,
				d.dag_size AS size_claimed,
				ds.inserted_at AS entry_created,
				ds.deleted_at AS entry_removed,
				GREATEST(
					ds.updated_at,
					(
						SELECT MAX(p.updated_at)
							FROM pin p
						WHERE
							p.content_cid = ds.content_cid
					)
				) AS entry_last_updated,

				JSONB_STRIP_NULLS( JSONB_BUILD_OBJECT(
					'original_cid', ds.source_cid,
					'mime_type', CASE WHEN ds.mime_type = '' THEN NULL ELSE ds.mime_type END,
					'upload_type', ds.type,
					'label', CASE WHEN ds.name = '' THEN NULL ELSE ds.name END,
					'files', CASE WHEN ds.files = JSONB('[]') THEN NULL ELSE ds.files END,
					'origins', ds.origins,
					'meta', ds.meta,
					'token_used', CASE WHEN ds.key_id IS NULL THEN NULL ELSE JSONB_BUILD_OBJECT(
						'id', ds.key_id,
						'label', k.name
					) END,
					'pin_reported_at', (
							SELECT MIN(p.updated_at)
								FROM pin p
							WHERE
								p.content_cid = ds.content_cid
									AND
								p.service = 'IpfsCluster'
									AND
								p.status = 'Pinned'
					)
				) ) AS details

			FROM upload ds
			JOIN content d ON ds.content_cid = d.cid
			LEFT JOIN auth_key k ON ds.key_id = k.id
		WHERE
			(
				ds.updated_at > $1
					OR
				EXISTS (
					SELECT 42
						FROM pin
					WHERE
						pin.updated_at > $1
							AND
						pin.content_cid = ds.content_cid
				)
			)
		`,
		cutoff,
	)
	if err != nil {
		return err
	}
	defer dagRows.Close()

	for dagRows.Next() {
		var e dagSourceEntry
		if err = dagRows.Scan(&e.sourceLabel, &e.CidV1Str, &e.SourceKey, &e.SizeClaimed, &e.CreatedAt, &e.RemovedAt, &e.UpdatedAt, &e.Details); err != nil {
			return err
		}

		c, err := cid.Parse(e.CidV1Str)
		if err != nil {
			return err
		}
		e.cidV1 = cidv1(c)

		// why would anyone be so mean?
		if _, own := ownAggregates[e.cidV1]; own {
			continue
		}

		totalQueriedDags++
		remoteDags[[2]string{e.sourceLabel, e.SourceKey}] = &e
	}
	if err = dagRows.Err(); err != nil {
		return err
	}
	dagRows.Close()

	log.Infof("%s: retrieved %d remote dag entries for processing", projPrefix, len(remoteDags))

	// Check for which already-existing records can we skip an upsert
	dagentryRows, err := cargoDb.Query(
		ctx,
		`
		SELECT s.source_label, ds.source_key, ds.entry_last_updated
			FROM cargo.dag_sources ds
			JOIN cargo.sources s USING ( srcid )
		WHERE s.project = $1
		`,
		p.id,
	)
	if err != nil {
		return err
	}
	defer dagentryRows.Close()
	for dagentryRows.Next() {
		var entryKey [2]string
		var ourLastUpd time.Time
		if err = dagentryRows.Scan(&entryKey[0], &entryKey[1], &ourLastUpd); err != nil {
			return err
		}
		existingDags++
		if r, found := remoteDags[entryKey]; found && ourLastUpd.After(r.UpdatedAt) {
			delete(remoteDags, entryKey)
		}
	}
	if err = dagentryRows.Err(); err != nil {
		return err
	}
	dagentryRows.Close()
	log.Infof("%s: iterated through %d already-existing dag entries", projPrefix, existingDags)

	// now deal with sources
	// since we pull them after we pulled the dags, we will see each source by definition
	seenSrcLabels := make(map[string]struct{}, len(remoteDags))
	toGetSrcLabels := make([]string, 0, len(remoteDags))
	for _, d := range remoteDags {
		if _, seen := seenSrcLabels[d.sourceLabel]; !seen {
			seenSrcLabels[d.sourceLabel] = struct{}{}
			toGetSrcLabels = append(toGetSrcLabels, d.sourceLabel)
		}
	}
	srcRows, err := srcDb.Query(
		ctx,
		`
		SELECT
				u.id::TEXT AS source_label,
				u.inserted_at AS entry_created,
				JSONB_STRIP_NULLS( JSONB_BUILD_OBJECT(
					'public_address', u.public_address, -- FIXME should go away
					'github_id', COALESCE( u.github->>'userHandle', SUBSTRING( u.github_id, 'github\|([0-9]+)') )::BIGINT,
					'email', u.email,
					'magic_link_id', CASE WHEN u.magic_link_id = '' THEN NULL ELSE u.magic_link_id END,
					'name', CASE WHEN u.name = '' THEN NULL ELSE u.name END,
					'nickname', SUBSTRING( github->'userInfo'->>'profile', '([^/]+)$' ),
					'picture', CASE WHEN u.picture = '' THEN NULL ELSE u.picture END
				) ) AS details
			FROM public.user u
		WHERE
			u.updated_at > $1
				OR
			u.id::TEXT = ANY( $2 )
		`,
		cutoff,
		toGetSrcLabels,
	)
	if err != nil {
		return err
	}
	defer srcRows.Close()

	srcMap := make(map[string]*int64)
	for srcRows.Next() {
		var isNewSource bool
		var s dagSource
		if err = srcRows.Scan(&s.SourceLabel, &s.CreatedAt, &s.Details); err != nil {
			return err
		}

		err = cargoDb.QueryRow(
			ctx,
			`
			INSERT INTO cargo.sources ( project, source_label, entry_created, details ) VALUES ( $1, $2, $3, $4 )
				ON CONFLICT ( project, source_label ) DO UPDATE SET
					entry_created = LEAST( cargo.sources.entry_created, EXCLUDED.entry_created ),
					details = EXCLUDED.details
			RETURNING srcid, (xmax = 0)
			`,
			p.id,
			s.SourceLabel,
			s.CreatedAt,
			s.Details,
		).Scan(&s.SourceID, &isNewSource)
		if err != nil {
			return err
		}
		if isNewSource {
			newSources++
		}

		srcMap[s.SourceLabel] = &s.SourceID
	}
	if err = srcRows.Err(); err != nil {
		return err
	}
	srcRows.Close()
	log.Infof("%s: upserted %d sources (users)", projPrefix, len(srcMap))

	// and insert everything
	// FIXME - iterating like this is pretty slow: need to switch to temptable COPY + INSERT SELECT
	for _, d := range remoteDags {

		d.SourceID = srcMap[d.sourceLabel]
		if _, known := knownDags[d.cidV1]; !known {
			_, err = cargoDb.Exec(
				ctx,
				`
				INSERT INTO cargo.dags ( cid_v1, entry_created ) VALUES ( $1, $2 )
					ON CONFLICT DO NOTHING
				`,
				d.CidV1Str,
				d.CreatedAt,
			)
			if err != nil {
				return err
			}
		}

		var isNew, wasAlreadyRemoved bool
		err = cargoDb.QueryRow(
			ctx,
			`
			INSERT INTO cargo.dag_sources ( cid_v1, source_key, srcid, size_claimed, entry_created, entry_removed, details )
				VALUES ( $1, $2, $3, $4, $5, $6, $7 )
				ON CONFLICT ( srcid, source_key ) DO UPDATE SET
					size_claimed = EXCLUDED.size_claimed,
					details = EXCLUDED.details,
					entry_created = LEAST( cargo.dag_sources.entry_created, EXCLUDED.entry_created ),
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
			d.CidV1Str,
			d.SourceKey,
			d.SourceID,
			d.SizeClaimed,
			d.CreatedAt,
			d.RemovedAt,
			d.Details,
		).Scan(&isNew, &wasAlreadyRemoved)
		if err != nil {
			return err
		}

		if d.RemovedAt != nil && !wasAlreadyRemoved {
			removedDags++
		} else if isNew {
			newDags++
		}
	}

	return nil
}
