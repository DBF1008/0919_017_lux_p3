package app

import (
	"bufio"
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
			cyan.Sprintf("%s", Name),
			blue.Sprintf("%s", c.App.Version),
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

			// Download queue
			&cli.UintFlag{
				Name:  "max-concurrent",
				Value: 3,
				Usage: "The maximum number of videos downloaded concurrently (global limit)",
			},
			&cli.IntFlag{
				Name:  "priority",
				Value: 0,
				Usage: "User-specified scheduling priority for the input URLs, higher values are downloaded first",
			},
			&cli.UintFlag{
				Name:  "queue-retry",
				Value: 2,
				Usage: "How many times a failed task is re-scheduled by the download queue (task-level retry)",
			},
			&cli.UintFlag{
				Name:  "queue-retry-delay",
				Value: 5,
				Usage: "Base delay in seconds before a failed task is re-scheduled by the download queue",
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

			// The info and json modes only print the extracted data, keep the
			// original serial behavior for them.
			if c.Bool("info") || c.Bool("json") {
				var isErr bool
				for _, videoURL := range args {
					if err := printData(c, videoURL); err != nil {
						printTaskError(videoURL, err)
						isErr = true
					}
				}
				if isErr {
					return cli.Exit("", 1)
				}
				return nil
			}

			return downloadWithQueue(c, args)
		},
		EnableBashCompletion: true,
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	return app
}

func printTaskError(videoURL string, err error) {
	fmt.Fprintf(
		color.Output,
		"Downloading %s error:\n",
		color.CyanString("%s", videoURL),
	)
	fmt.Printf("%+v\n", err)
}

// extract extracts the data of one URL. A returned error is a task-level
// failure of this URL.
func extract(c *cli.Context, videoURL string) ([]*extractors.Data, error) {
	data, err := extractors.Extract(videoURL, extractors.Options{
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
	if err != nil {
		// if this error occurs, it means that an error occurred before actually starting to extract data
		// (there is an error in the preparation step), and the data list is empty.
		return nil, err
	}
	return data, nil
}

// printData handles the --info and --json modes for one URL.
func printData(c *cli.Context, videoURL string) error {
	data, err := extract(c, videoURL)
	if err != nil {
		return err
	}
	if c.Bool("json") {
		e := json.NewEncoder(os.Stdout)
		e.SetIndent("", "\t")
		e.SetEscapeHTML(false)
		if err := e.Encode(data); err != nil {
			return err
		}

		return nil
	}

	defaultDownloader := newDownloader(c, nil)
	errs := make([]error, 0)
	for _, item := range data {
		if item.Err != nil {
			// if this error occurs, the preparation step is normal, but the data extraction is wrong.
			// the data is an empty struct.
			errs = append(errs, item.Err)
			continue
		}
		if err = defaultDownloader.Download(item); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		return errs[0]
	}
	return nil
}

// newDownloader builds a Downloader from the CLI context. If pause is not
// nil, it is attached to the downloader so the queue manager can pause and
// resume this download.
func newDownloader(c *cli.Context, pause *downloader.PauseController) *downloader.Downloader {
	return downloader.New(downloader.Options{
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
		// PauseController is managed by the download queue manager and allows
		// pausing/resuming this download at chunk boundaries.
		PauseController: pause,
	})
}

// taskSize estimates the download size of one extracted data item, used for
// size-based scheduling.
func taskSize(c *cli.Context, data *extractors.Data) int64 {
	if streamName := c.String("stream-format"); streamName != "" {
		if stream, ok := data.Streams[streamName]; ok {
			return stream.Size
		}
	}
	var maxSize int64
	for _, stream := range data.Streams {
		if stream.Size > maxSize {
			maxSize = stream.Size
		}
	}
	return maxSize
}

// downloadWithQueue extracts all input URLs, enqueues the extracted items
// into the download queue manager and runs it with the configured global
// concurrency limit.
//
// Errors are classified into two categories:
//   - task-level failures: a single URL/item failed (extraction or download).
//     They are collected and reported per task, and failed download tasks are
//     automatically re-scheduled by the queue's delayed retry queue.
//   - system-level failures: the queue itself cannot operate (for example an
//     invalid --max-concurrent value). They abort the whole run immediately.
func downloadWithQueue(c *cli.Context, args []string) error {
	// System-level failure: invalid queue configuration.
	queueManager, err := NewQueueManager(QueueOptions{
		MaxConcurrent:   int(c.Uint("max-concurrent")),
		QueueRetryTimes: int(c.Uint("queue-retry")),
		RetryDelay:      time.Duration(c.Uint("queue-retry-delay")) * time.Second,
	})
	if err != nil {
		return cli.Exit(fmt.Sprintf("queue manager error: %s", err), 1)
	}

	// Phase 1: extract all URLs and enqueue the extracted items.
	// Extraction errors are task-level failures of the corresponding URL.
	var taskErrors []error
	priority := c.Int("priority")
	for _, videoURL := range args {
		dataList, err := extract(c, videoURL)
		if err != nil {
			printTaskError(videoURL, err)
			taskErrors = append(taskErrors, err)
			continue
		}
		for _, item := range dataList {
			if item.Err != nil {
				// if this error occurs, the preparation step is normal, but the data extraction is wrong.
				// the data is an empty struct.
				printTaskError(videoURL, item.Err)
				taskErrors = append(taskErrors, item.Err)
				continue
			}
			pause := downloader.NewPauseController()
			queueManager.Enqueue(&Task{
				URL:        videoURL,
				Title:      item.Title,
				Site:       item.Site,
				Size:       taskSize(c, item),
				Priority:   priority,
				Data:       item,
				Downloader: newDownloader(c, pause),
				pause:      pause,
			})
		}
	}

	if queueManager.Len() == 0 {
		if len(taskErrors) != 0 {
			return cli.Exit("", 1)
		}
		return nil
	}

	if !c.Bool("silent") {
		fmt.Fprintf(
			color.Output,
			"Download queue: %s task(s), max concurrent: %s\n",
			color.CyanString("%d", queueManager.Len()),
			color.CyanString("%d", int(c.Uint("max-concurrent"))),
		)
		go queueCommandListener(queueManager)
	}

	// Phase 2: run the queue. Finished tasks automatically trigger the
	// scheduling of the next highest-priority task; failed tasks are
	// re-scheduled through the delayed retry queue.
	results := queueManager.Run()
	sortResultsByID(results)
	for _, result := range results {
		if result.Err != nil {
			// Task-level failure after all queue retries are exhausted.
			printTaskError(result.Task.URL, result.Err)
			taskErrors = append(taskErrors, result.Err)
		}
	}

	if len(taskErrors) != 0 {
		return cli.Exit("", 1)
	}
	return nil
}

// queueCommandListener reads simple queue control commands from stdin:
//
//	pause <id>   pause a single task
//	resume <id>  resume a single task
//	status       print the status of all tasks
//
// It exits silently when stdin is closed or not readable.
func queueCommandListener(queueManager *QueueManager) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "pause", "resume":
			if len(fields) != 2 {
				fmt.Println("usage: pause|resume <task id>")
				continue
			}
			var id int
			if _, err := fmt.Sscanf(fields[1], "%d", &id); err != nil {
				fmt.Println("invalid task id")
				continue
			}
			var ok bool
			if fields[0] == "pause" {
				ok = queueManager.PauseTask(id)
			} else {
				ok = queueManager.ResumeTask(id)
			}
			if !ok {
				fmt.Printf("task %d can not be %sd\n", id, fields[0])
			}
		case "status":
			for _, s := range queueManager.Snapshots() {
				fmt.Printf("task %d [%s] %s %s\n", s.ID, s.Status, s.Site, s.Title)
			}
		}
	}
}
