package main

import (
	"os"
	"time"

	"github.com/cloudflare/cloudflare-go"
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
	log          = logging.Logger("dagcargo-cron")
	ShowProgress = isatty.IsTerminal(os.Stderr.Fd())
)

func init() {
	logging.SetLogLevel("*", "INFO")
}

func cidv1(c cid.Cid) cid.Cid {
	if c.Version() == 1 {
		return c
	}
	return cid.NewCidV1(c.Type(), c.Hash())
}

func connectDb(cctx *cli.Context) (*pgxpool.Pool, error) {
	dbConnCfg, err := pgxpool.ParseConfig(cctx.String("pg-connstring"))
	if err != nil {
		return nil, err
	}
	return pgxpool.ConnectConfig(cctx.Context, dbConnCfg)
}

func ipfsApi(cctx *cli.Context) *ipfsapi.Shell {
	s := ipfsapi.NewShell(cctx.String("ipfs-api"))
	s.SetTimeout(time.Second * time.Duration(cctx.Uint("ipfs-api-timeout")))
	return s
}

func cfApi(cctx *cli.Context) (*cloudflare.API, error) {
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
