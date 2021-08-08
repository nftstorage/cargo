package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	filactors "github.com/filecoin-project/specs-actors/actors/builtin"
	fslock "github.com/ipfs/go-fs-lock"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"

	"golang.org/x/sys/unix"
)

var currentCmd string
var currentCmdLock io.Closer

const filDefaultLookback = 10

var globalFlags = []cli.Flag{
	&cli.StringFlag{
		Name:  "ipfs-api",
		Value: "http://localhost:5001",
	},
	&cli.UintFlag{
		Name:  "ipfs-api-timeout",
		Usage: "HTTP API timeout in seconds",
		Value: 270,
	},
	&cli.UintFlag{
		Name:  "ipfs-api-max-workers",
		Usage: "Amount of concurrent IPFS API operations",
		Value: 128,
	},
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:  "lotus-api",
		Value: "http://localhost:1234",
	}),
	&cli.UintFlag{
		Name:  "lotus-lookback-epochs",
		Value: filDefaultLookback,
		DefaultText: fmt.Sprintf("%d epochs / %ds",
			filDefaultLookback,
			filactors.EpochDurationSeconds*filDefaultLookback,
		),
	},
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:  "pg-connstring",
		Value: "postgres:///postgres?user=cargo&password=&host=/var/run/postgresql",
	}),
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:        "cf-account",
		DefaultText: "  {{ private, read from config file }}  ",
		Hidden:      true,
	}),
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:        "cf-kvnamespace-nfts",
		DefaultText: "  {{ private, read from config file }}  ",
		Hidden:      true,
	}),
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:        "cf-kvnamespace-deals",
		DefaultText: "  {{ private, read from config file }}  ",
		Hidden:      true,
	}),
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:        "cf-kvnamespace-users",
		DefaultText: "  {{ private, read from config file }}  ",
		Hidden:      true,
	}),
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:        "cf-bearer-token",
		DefaultText: "  {{ private, read from config file }}  ",
		Hidden:      true,
	}),
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:        "fauna-token-w3s-prod",
		DefaultText: "  {{ private, read from config file }}  ",
		Hidden:      true,
	}),
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:        "fauna-token-w3s-stage",
		DefaultText: "  {{ private, read from config file }}  ",
		Hidden:      true,
	}),
}

func main() {

	ctx, cancel := context.WithCancel(context.Background())

	cleanup := func() {
		cancel()
		if db != nil {
			db.Close()
		}
		time.Sleep(250 * time.Millisecond) // give a bit of time for various parts to close
	}
	defer cleanup()

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, unix.SIGINT, unix.SIGTERM)
		<-sigs
		log.Warn("early termination signal received, cleaning up...")
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
			getNewDags,
			analyzeDags,
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

		// if we are not interactive - be quiet on a failed lock
		if !showProgress && errors.As(err, new(fslock.LockedError)) {
			cleanup()
			os.Exit(1)
		}

		log.Error(err)
		if currentCmdLock != nil {
			log.Warnw(logHdr, logArgs...)
		}
		cleanup()
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

		var firstCmdOccurence string
		for i := 1; i < len(os.Args); i++ {

			// if we are in help context - no locks and no start/stop timers
			if os.Args[i] == `-h` || os.Args[i] == `--help` {
				return nil
			}

			if firstCmdOccurence != "" {
				continue
			}
			firstCmdOccurence = cmdNames[os.Args[i]]
		}

		// wrong cmd or something
		if firstCmdOccurence == "" {
			return nil
		}

		var err error
		if currentCmdLock, err = fslock.Lock(os.TempDir(), "cargocron-"+firstCmdOccurence); err != nil {
			return err
		}

		currentCmd = firstCmdOccurence
		log.Infow(fmt.Sprintf("=== BEGIN '%s' run", currentCmd))

		// init the shared DB connection: do it here, since now we know the config *AND*
		// we want the maxConn counter shared, singleton-style
		dbConnCfg, err := pgxpool.ParseConfig(cctx.String("pg-connstring"))
		if err != nil {
			return err
		}
		db, err = pgxpool.ConnectConfig(cctx.Context, dbConnCfg)
		if err != nil {
			return err
		}
	}

	return nil
}
