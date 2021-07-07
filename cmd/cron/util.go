package main

import (
	"fmt"
	"os"
	"time"

	"github.com/cloudflare/cloudflare-go"
	"github.com/filecoin-project/go-jsonrpc"
	filabi "github.com/filecoin-project/go-state-types/abi"
	lotusapi "github.com/filecoin-project/lotus/api"
	filtypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	ipfsapi "github.com/ipfs/go-ipfs-api"
	logging "github.com/ipfs/go-log/v2"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/mattn/go-isatty"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

const (
	bufPresize = 128 << 20 // size to the approximate amount of NFTs we track
)

var (
	log          = logging.Logger(fmt.Sprintf("dagcargo-cron(%d)", os.Getpid()))
	showProgress = isatty.IsTerminal(os.Stderr.Fd())
)

func init() {
	logging.SetLogLevel("*", "INFO") // nolint:errcheck
}

func cidv1(c cid.Cid) cid.Cid {
	if c.Version() == 1 {
		return c
	}
	return cid.NewCidV1(c.Type(), c.Hash())
}

func mainnetTime(filEpoch int64) time.Time { return time.Unix(filEpoch*30+1598306400, 0) }

func connectDb(cctx *cli.Context) (*pgxpool.Pool, error) {
	dbConnCfg, err := pgxpool.ParseConfig(cctx.String("pg-connstring"))
	if err != nil {
		return nil, err
	}
	return pgxpool.ConnectConfig(cctx.Context, dbConnCfg)
}

func ipfsAPI(cctx *cli.Context) *ipfsapi.Shell {
	s := ipfsapi.NewShell(cctx.String("ipfs-api"))
	s.SetTimeout(time.Second * time.Duration(cctx.Uint("ipfs-api-timeout")))
	return s
}

func cfAPI(cctx *cli.Context) (*cloudflare.API, error) {
	bearer := cctx.String("cf-bearer-token")
	if bearer == "" {
		return nil, xerrors.New("config `cf-bearer-token` is not set")
	}

	acc := cctx.String("cf-account")
	if acc == "" {
		return nil, xerrors.New("config `cf-account` is not set")
	}

	return cloudflare.NewWithAPIToken(bearer,
		cloudflare.UsingRetryPolicy(5, 2, 30),
		cloudflare.UsingAccount(acc),
	)
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
	head, err := api.ChainHead(cctx.Context)
	if err != nil {
		return nil, xerrors.Errorf("failed getting chain head: %w", err)
	}

	ts, err := api.ChainGetTipSetByHeight(cctx.Context, head.Height()-filabi.ChainEpoch(cctx.Uint("lotus-lookback-epochs")), head.Key())
	if err != nil {
		return nil, xerrors.Errorf("determining target tipset %d epochs ago failed: %w", cctx.Uint("lotus-lookback-epochs"), err)
	}

	return ts, nil
}
