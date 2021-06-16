package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"

	"golang.org/x/sys/unix"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, unix.SIGINT, unix.SIGTERM)
		<-sigs
		cancel()
	}()

	flags := []cli.Flag{
		&cli.StringFlag{
			Name:  "ipfs-api",
			Value: "http://localhost:5001",
		},
		&cli.UintFlag{
			Name:  "ipfs-api-timeout",
			Usage: "HTTP API timeout in seconds",
			Value: 300,
		},
		&cli.UintFlag{
			Name:  "ipfs-api-max-workers",
			Usage: "Amount of concurrent IPFS API operations",
			Value: 64,
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

	if err := (&cli.App{
		Name:  "cargo-cron",
		Usage: "Misc background processes for dagcargo",
		Before: altsrc.InitInputSourceWithContext(
			flags,
			func(context *cli.Context) (altsrc.InputSourceContext, error) {
				return altsrc.NewTomlSourceFromFile("dagcargo.toml")
			}),
		Flags: flags,
		Commands: []*cli.Command{
			getNewNftCids,
			pinDags,
		},
	}).RunContext(ctx, os.Args); err != nil {
		log.Error(err)
		os.Exit(1)
		return
	}
}
