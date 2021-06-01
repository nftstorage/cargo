package main

import (
	"github.com/cloudflare/cloudflare-go"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

func cidv1(c cid.Cid) cid.Cid {
	if c.Version() == 1 {
		return c
	}
	return cid.NewCidV1(c.Type(), c.Hash())
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
