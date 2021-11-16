package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/filecoin-project/go-jsonrpc"
	filabi "github.com/filecoin-project/go-state-types/abi"
	lotusapi "github.com/filecoin-project/lotus/api"
	filbuild "github.com/filecoin-project/lotus/build"
	filtypes "github.com/filecoin-project/lotus/chain/types"
	filactors "github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/ipfs/go-cid"
	ipfsapi "github.com/ipfs/go-ipfs-api"
	logging "github.com/ipfs/go-log/v2"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/mattn/go-isatty"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var (
	projects = map[string]string{
		"0": "staging.web3.storage",
		"1": "web3.storage",
		"2": "nft.storage",
	}

	cargoDb *pgxpool.Pool // singleton populated in urfaveCLIs Before()

	log          = logging.Logger(fmt.Sprintf("dagcargo-cron(%d)", os.Getpid()))
	isTerm       = isatty.IsTerminal(os.Stderr.Fd())
	showProgress = isTerm
)

func init() {
	logging.SetLogLevel("*", "INFO") //nolint:errcheck
}

func cidv1(c cid.Cid) cid.Cid {
	if c.Version() == 1 {
		return c
	}
	return cid.NewCidV1(c.Type(), c.Hash())
}

func cidListFromQuery(ctx context.Context, sql string, args ...interface{}) (map[cid.Cid]struct{}, error) {
	cidList := make(map[cid.Cid]struct{}, 1<<10)

	rows, err := cargoDb.Query(
		ctx,
		sql,
		args...,
	)
	if err != nil {
		return nil, err
	}

	var cidStr string
	for rows.Next() {
		if err = rows.Scan(&cidStr); err != nil {
			return nil, err
		}
		c, err := cid.Parse(cidStr)
		if err != nil {
			return nil, err
		}
		cidList[cidv1(c)] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return cidList, nil
}

func mainnetTime(e filabi.ChainEpoch) time.Time { return time.Unix(int64(e)*30+1598306400, 0) }

func ipfsAPI(cctx *cli.Context) *ipfsapi.Shell {
	s := ipfsapi.NewShell(cctx.String("ipfs-api"))
	s.SetTimeout(time.Second * time.Duration(cctx.Uint("ipfs-api-timeout")))
	return s
}

func lotusAPI(cctx *cli.Context) (api *lotusapi.FullNodeStruct, closer func(), err error) {
	api = new(lotusapi.FullNodeStruct)
	closer, err = jsonrpc.NewMergeClient(cctx.Context, cctx.String("lotus-api")+"/rpc/v0", "Filecoin", []interface{}{&api.Internal, &api.CommonStruct.Internal}, nil)
	if err != nil {
		api = nil
		closer = nil
	}
	return
}

func lotusLookbackTipset(cctx *cli.Context, api *lotusapi.FullNodeStruct) (*filtypes.TipSet, error) {
	latestHead, err := api.ChainHead(cctx.Context)
	if err != nil {
		return nil, xerrors.Errorf("failed getting chain head: %w", err)
	}

	wallUnix := time.Now().Unix()
	filUnix := int64(latestHead.Blocks()[0].Timestamp)

	if wallUnix < filUnix ||
		wallUnix > filUnix+int64(
			// allow up to 2 nul tipsets in a row ( 3 is virtually impossible )
			filbuild.PropagationDelaySecs+(2*filactors.EpochDurationSeconds),
		) {
		return nil, xerrors.Errorf(
			"lotus API out of sync: chainHead reports unixtime %d (height: %d) while walltime is %d (delta: %s)",
			filUnix,
			latestHead.Height(),
			wallUnix,
			time.Second*time.Duration(wallUnix-filUnix),
		)
	}

	latestHeight := latestHead.Height()

	tipsetAtLookback, err := api.ChainGetTipSetByHeight(cctx.Context, latestHeight-filabi.ChainEpoch(cctx.Uint("lotus-lookback-epochs")), latestHead.Key())
	if err != nil {
		return nil, xerrors.Errorf("determining target tipset %d epochs ago failed: %w", cctx.Uint("lotus-lookback-epochs"), err)
	}

	return tipsetAtLookback, nil
}

func retryingClient(bearerToken string) *http.Client {

	rc := retryablehttp.NewClient()
	rc.Logger = &retLogWrap{ipfslog: log}
	rc.RetryWaitMin = 2 * time.Second
	rc.RetryWaitMax = 35 * time.Second
	rc.RetryMax = 5
	rc.CheckRetry = retryablehttp.ErrorPropagatedRetryPolicy

	sc := rc.StandardClient()
	if bearerToken != "" {
		sc.Transport = &authorizer{rt: sc.Transport, token: bearerToken}
	}

	return sc
}

type retLogWrap struct{ ipfslog *logging.ZapEventLogger }

func (w *retLogWrap) Error(msg string, kv ...interface{}) { w.ipfslog.Errorw(msg, kv...) }
func (w *retLogWrap) Info(msg string, kv ...interface{})  { w.ipfslog.Infow(msg, kv...) }
func (w *retLogWrap) Debug(msg string, kv ...interface{}) { w.ipfslog.Debugw(msg, kv...) }
func (w *retLogWrap) Warn(msg string, kv ...interface{})  { w.ipfslog.Warnw(msg, kv...) }

type authorizer struct {
	token string
	rt    http.RoundTripper
}

func (a *authorizer) RoundTrip(rq *http.Request) (*http.Response, error) {
	rq.Header.Add("Authorization", "Bearer "+a.token)
	t := a.rt
	if t == nil {
		t = http.DefaultTransport
	}
	return t.RoundTrip(rq)
}
