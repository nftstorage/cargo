package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	fslock "github.com/ipfs/go-fs-lock"
	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"

	"golang.org/x/sys/unix"
)

var currentCmd string
var currentCmdLock io.Closer

var globalFlags = []cli.Flag{
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
		Name:  "lotus-api",
		Value: "http://localhost:1234",
	},
	&cli.UintFlag{
		Name:  "lotus-lookback-epochs",
		Value: 10,
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

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, unix.SIGINT, unix.SIGTERM)
		<-sigs
		cancel()
	}()

	t0 := time.Now()

	err := (&cli.App{
		Name:   "cargo-cron",
		Usage:  "Misc background processes for dagcargo",
		Before: beforeCliSetup, // obtains locks and emits the proper init loglines
		Flags:  globalFlags,
		Commands: []*cli.Command{
			getNewNftCids,
			pinDags,
			aggregateDags,
			trackDeals,
			exportStatus,
		},
	}).RunContext(ctx, os.Args)

	logHdr := fmt.Sprintf("=== FINISH '%s' run", currentCmd)
	logArgs := []interface{}{
		"success", (err == nil),
		"took", time.Since(t0).Truncate(time.Millisecond).String(),
	}

	if err != nil {
		log.Error(err)
		if currentCmdLock != nil {
			log.Warnw(logHdr, logArgs...)
		}
		os.Exit(1)
	}

	if currentCmdLock != nil {
		log.Infow(logHdr, logArgs...)
	}
}

var beforeCliSetup = func(cctx *cli.Context) error {

	//
	// urfave/cli-specific add config file
	if err := altsrc.InitInputSourceWithContext(
		globalFlags,
		func(context *cli.Context) (altsrc.InputSourceContext, error) {
			return altsrc.NewTomlSourceFromFile("dagcargo.toml")
		},
	)(cctx); err != nil {
		return err
	}
	// urfave/cli-specific end
	//

	// figure out what is the command that was invoked
	if len(os.Args) > 1 {
		cmdNames := make(map[string]string)
		for _, c := range cctx.App.Commands {
			cmdNames[c.Name] = c.Name
			for _, a := range c.Aliases {
				cmdNames[a] = c.Name
			}
		}
		for i := 1; i < len(os.Args); i++ {
			if cmd, found := cmdNames[os.Args[i]]; found {
				var err error
				if currentCmdLock, err = fslock.Lock(os.TempDir(), "cargocron-"+cmd); err != nil {
					return err
				}
				currentCmd = cmd
				log.Infow(fmt.Sprintf("=== BEGIN '%s' run", currentCmd))
				break
			}
		}
	}
	return nil
}
