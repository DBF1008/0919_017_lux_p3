package app

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/urfave/cli/v2"

	"github.com/iawia002/lux/downloader"
	"github.com/iawia002/lux/extractors"
	"github.com/iawia002/lux/request"
	"github.com/iawia002/lux/utils"
)

// Name is the name of this app.
const Name = "lux"

// This value will be injected into the corresponding git tag value at build time using `-ldflags`.
var version = "v0.0.0"

func init() {
	cli.VersionPrinter = func(c *cli.Context) {
		blue := color.New(color.FgBlue)
		cyan := color.New(color.FgCyan)
		fmt.Fprintf(
			color.Output,
			"\n%s: version %s, A fast and simple video downloader.\n\n",
			cyan.Sprintf(Name),
			blue.Sprintf(c.App.Version),
		)
	}
}

// New returns the App instance.
func New() *cli.App {
	app := &cli.App{
		Name:    Name,
		Usage:   "A fast and simple video downloader.",
		Version: version,
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "debug",
				Aliases: []string{"d"},
				Usage:   "Debug mode",
			},
			&cli.BoolFlag{
				Name:    "silent",
				Aliases: []string{"s"},
				Usage:   "Minimum outputs",
			},
			&cli.BoolFlag{
				Name:    "info",
				Aliases: []string{"i"},
				Usage:   "Information only",
			},
			&cli.BoolFlag{
				Name:    "json",
				Aliases: []string{"j"},
				Usage:   "Print extracted JSON data",
			},

			&cli.StringFlag{
				Name:    "cookie",
				Aliases: []string{"c"},
				Usage:   "Cookie",
			},
			&cli.BoolFlag{
				Name:    "playlist",
				Aliases: []string{"p"},
				Usage:   "Download playlist",
			},
			&cli.StringFlag{
				Name:    "user-agent",
				Aliases: []string{"u"},
				Usage:   "Use specified User-Agent",
			},
			&cli.StringFlag{
				Name:    "refer",
				Aliases: []string{"r"},
				Usage:   "Use specified Referrer",
			},
			&cli.StringFlag{
				Name:    "stream-format",
				Aliases: []string{"f"},
				Usage:   "Select specific stream to download",
			},
			&cli.BoolFlag{
				Name:    "audio-only",
				Aliases: []string{"ao"},
				Usage:   "Download audio only at best quality",
			},
			&cli.StringFlag{
				Name:    "file",
				Aliases: []string{"F"},
				Usage:   "URLs file path",
			},
			&cli.StringFlag{
				Name:    "output-path",
				Aliases: []string{"o"},
				Usage:   "Specify the output path",
			},
			&cli.StringFlag{
				Name:    "output-name",
				Aliases: []string{"O"},
				Usage:   "Specify the output file name",
			},
			&cli.UintFlag{
				Name:  "file-name-length",
				Value: 255,
				Usage: "The maximum length of a file name, 0 means unlimited",
			},
			&cli.BoolFlag{
				Name:    "caption",
				Aliases: []string{"C"},
				Usage:   "Download captions",
			},
			&cli.BoolFlag{
				Name:    "embed-subtitle",
				Aliases: []string{"embed"},
				Usage:   "Embed subtitles into the video (requires ffmpeg)",
			},

			&cli.UintFlag{
				Name:  "start",
				Value: 1,
				Usage: "Define the starting item of a playlist or a file input",
			},
			&cli.UintFlag{
				Name:  "end",
				Value: 0,
				Usage: "Define the ending item of a playlist or a file input",
			},
			&cli.StringFlag{
				Name:  "items",
				Usage: "Define wanted items from a file or playlist. Separated by commas like: 1,5,6,8-10",
			},

			&cli.BoolFlag{
				Name:    "multi-thread",
				Aliases: []string{"m"},
				Usage:   "Multiple threads to download single video",
			},
			&cli.UintFlag{
				Name:  "retry",
				Value: 10,
				Usage: "How many times to retry when the download failed",
			},
			&cli.UintFlag{
				Name:    "chunk-size",
				Aliases: []string{"cs"},
				Value:   1,
				Usage:   "HTTP chunk size for downloading (in MB)",
			},
			&cli.UintFlag{
				Name:    "thread",
				Aliases: []string{"n"},
				Value:   10,
				Usage:   "The number of download thread (only works for multiple-parts video)",
			},
			&cli.UintFlag{
				Name:  "max-concurrent",
				Value: 3,
				Usage: "The maximum number of concurrent download tasks",
			},
			&cli.UintFlag{
				Name:  "queue-retry",
				Value: 2,
				Usage: "How many times a failed task is rescheduled by the download queue (task-level retry, unlike --retry which retries inside a single task)",
			},
			&cli.UintFlag{
				Name:  "queue-retry-delay",
				Value: 5,
				Usage: "Delay in seconds before a failed task is rescheduled by the download queue",
			},

			// Aria2
			&cli.BoolFlag{
				Name:  "aria2",
				Usage: "Use Aria2 RPC to download",
			},
			&cli.StringFlag{
				Name:  "aria2-token",
				Usage: "Aria2 RPC Token",
			},
			&cli.StringFlag{
				Name:  "aria2-addr",
				Value: "localhost:6800",
				Usage: "Aria2 Address",
			},
			&cli.StringFlag{
				Name:  "aria2-method",
				Value: "http",
				Usage: "Aria2 Method",
			},

			// youku
			&cli.StringFlag{
				Name:    "youku-ccode",
				Aliases: []string{"ccode"},
				Value:   "0502",
				Usage:   "Youku ccode",
			},
			&cli.StringFlag{
				Name:    "youku-ckey",
				Aliases: []string{"ckey"},
				Value:   "7B19C0AB12633B22E7FE81271162026020570708D6CC189E4924503C49D243A0DE6CD84A766832C2C99898FC5ED31F3709BB3CDD82C96492E721BDD381735026",
				Usage:   "Youku ckey",
			},
			&cli.StringFlag{
				Name:    "youku-password",
				Aliases: []string{"password"},
				Usage:   "Youku password",
			},

			&cli.BoolFlag{
				Name:    "episode-title-only",
				Aliases: []string{"eto"},
				Usage:   "File name of each bilibili episode doesn't include the playlist title",
			},
		},
		Action: func(c *cli.Context) error {
			args := c.Args().Slice()

			if c.Bool("debug") {
				cli.VersionPrinter(c)
			}

			if file := c.String("file"); file != "" {
				f, err := os.Open(file)
				if err != nil {
					return err
				}
				defer f.Close() // nolint

				fileItems := utils.ParseInputFile(f, c.String("items"), int(c.Uint("start")), int(c.Uint("end")))
				args = append(args, fileItems...)
			}

			if len(args) < 1 {
				return errors.New("too few arguments")
			}

			cookie := c.String("cookie")
			if cookie != "" {
				// If cookie is a file path, convert it to a string to ensure cookie is always string
				if _, fileErr := os.Stat(cookie); fileErr == nil {
					// Cookie is a file
					data, err := os.ReadFile(cookie)
					if err != nil {
						return err
					}
					cookie = strings.TrimSpace(string(data))
				}
			}

			request.SetOptions(request.Options{
				RetryTimes: int(c.Uint("retry")),
				Cookie:     cookie,
				UserAgent:  c.String("user-agent"),
				Refer:      c.String("refer"),
				Debug:      c.Bool("debug"),
				Silent:     c.Bool("silent"),
			})

			var systemErrors []error
			var taskErrors []downloader.TaskError
			if c.Bool("json") {
				for _, videoURL := range args {
					if err := printJSON(c, videoURL); err != nil {
						printExtractError(videoURL, err)
						systemErrors = append(systemErrors, err)
					}
				}
			} else {
				taskErrors, systemErrors = downloadWithQueue(c, args)
			}
			for _, taskErr := range taskErrors {
				fmt.Fprintf(
					color.Output,
					"Downloading %s error (task-level, retries exhausted):\n",
					color.CyanString("%s", taskErr.Task.Data.URL),
				)
				fmt.Printf("%+v\n", taskErr.Err)
			}
			for _, err := range systemErrors {
				fmt.Printf("%+v\n", err)
			}
			if len(taskErrors) != 0 || len(systemErrors) != 0 {
				return cli.Exit("", 1)
			}
			return nil
		},
		EnableBashCompletion: true,
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	return app
}

func printExtractError(videoURL string, err error) {
	fmt.Fprintf(
		color.Output,
		"Extracting %s error:\n",
		color.CyanString("%s", videoURL),
	)
	fmt.Printf("%+v\n", err)
}

func printJSON(c *cli.Context, videoURL string) error {
	data, err := extract(c, videoURL)
	if err != nil {
		return err
	}
	e := json.NewEncoder(os.Stdout)
	e.SetIndent("", "\t")
	e.SetEscapeHTML(false)
	return e.Encode(data)
}

func extract(c *cli.Context, videoURL string) ([]*extractors.Data, error) {
	return extractors.Extract(videoURL, extractors.Options{
		Playlist:         c.Bool("playlist"),
		Items:            c.String("items"),
		ItemStart:        int(c.Uint("start")),
		ItemEnd:          int(c.Uint("end")),
		ThreadNumber:     int(c.Uint("thread")),
		EpisodeTitleOnly: c.Bool("episode-title-only"),
		Cookie:           c.String("cookie"),
		YoukuCcode:       c.String("youku-ccode"),
		YoukuCkey:        c.String("youku-ckey"),
		YoukuPassword:    c.String("youku-password"),
	})
}

// downloadWithQueue extracts all URLs, puts the extracted data into the
// download queue and runs the queue with a global concurrency limit.
// It returns task-level errors (a task failed after all task-level retries)
// and system-level errors (extraction/preparation failures) separately.
func downloadWithQueue(c *cli.Context, args []string) ([]downloader.TaskError, []error) {
	queueManager := downloader.NewQueueManager(downloader.QueueOptions{
		MaxConcurrent:  int(c.Uint("max-concurrent")),
		MaxTaskRetries: int(c.Uint("queue-retry")),
		RetryDelay:     time.Duration(c.Uint("queue-retry-delay")) * time.Second,
	})

	// system-level errors: an error occurred before actually starting to
	// download (there is an error in the preparation/extraction step).
	systemErrors := make([]error, 0)
	for _, videoURL := range args {
		data, err := extract(c, videoURL)
		if err != nil {
			printExtractError(videoURL, err)
			systemErrors = append(systemErrors, err)
			continue
		}
		for _, item := range data {
			if item.Err != nil {
				// the preparation step is normal, but the data extraction is
				// wrong, the data is an empty struct.
				systemErrors = append(systemErrors, item.Err)
				continue
			}
			queueManager.Add(item, 0)
		}
	}

	options := downloader.Options{
		Silent:         c.Bool("silent"),
		InfoOnly:       c.Bool("info"),
		Stream:         c.String("stream-format"),
		AudioOnly:      c.Bool("audio-only"),
		Refer:          c.String("refer"),
		OutputPath:     c.String("output-path"),
		OutputName:     c.String("output-name"),
		FileNameLength: int(c.Uint("file-name-length")),
		Caption:        c.Bool("caption"),
		EmbedSubtitle:  c.Bool("embed-subtitle"),
		MultiThread:    c.Bool("multi-thread"),
		ThreadNumber:   int(c.Uint("thread")),
		RetryTimes:     int(c.Uint("retry")),
		ChunkSizeMB:    int(c.Uint("chunk-size")),
		UseAria2RPC:    c.Bool("aria2"),
		Aria2Token:     c.String("aria2-token"),
		Aria2Method:    c.String("aria2-method"),
		Aria2Addr:      c.String("aria2-addr"),
	}
	result := queueManager.Run(func(task *downloader.Task) error {
		// build a new Downloader per task: Downloader keeps per-download
		// state (eg: the progress bar) and is not safe for concurrent use.
		taskOptions := options
		taskOptions.PauseController = task.PauseController()
		return downloader.New(taskOptions).Download(task.Data)
	})
	return result.TaskErrors, append(systemErrors, result.SystemErrors...)
}
