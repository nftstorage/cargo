package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/mattn/go-isatty"
	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"

	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/sys/unix"
)

const bufPresize = 128 << 20 // size to the approximate amount of NFTs we track

var log = logging.Logger("dagcargo-cron")

var isTty = isatty.IsTerminal(os.Stderr.Fd())

func main() {

	logging.SetLogLevel("*", "INFO")

	cfgFileReader := func(context *cli.Context) (altsrc.InputSourceContext, error) {
		return altsrc.NewTomlSourceFromFile("dagcargo.toml")
	}

	flags := []cli.Flag{
		&cli.StringFlag{
			Name:  "ipfs-api",
			Value: "http://localhost:5001/",
		},
		&cli.StringFlag{
			Name:  "pg-connstring",
			Value: "postgres:///postgres?user=nft&password=&host=/var/run/postgresql",
		},
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:        "cf-account",
			DefaultText: " {{ private read from config file }} ",
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:        "cf-kvnamespace-nfts",
			DefaultText: " {{ private read from config file }} ",
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:        "cf-kvnamespace-deals",
			DefaultText: " {{ private read from config file }} ",
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:        "cf-bearer-token",
			DefaultText: " {{ private read from config file }} ",
		}),
	}

	app := &cli.App{
		Name:   "cargo-cron",
		Usage:  "Misc background processes for dagcargo",
		Before: altsrc.InitInputSourceWithContext(flags, cfgFileReader),
		Flags:  flags,
		Commands: []*cli.Command{
			getNewNftCids,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, unix.SIGINT, unix.SIGTERM)
		<-sigs
	}()

	if err := app.RunContext(ctx, os.Args); err != nil {
		log.Error(err)
		os.Exit(1)
		return
	}
}
