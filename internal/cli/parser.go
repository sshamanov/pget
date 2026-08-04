package cli

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/sshamanov/pget/internal/version"
)

const (
	defaultConnections = 8
	defaultSplitSize   = 8 << 20 // 8 MiB
	defaultBufferSize  = 128 << 20 // 128 MiB
	defaultMaxTries    = 20
	defaultTimeout     = 0 // no timeout by default
	defaultProgress    = "bar"
)

// option describes a single command-line option.
type option struct {
	Long     string
	Short    rune
	ArgName  string // non-empty if the option takes an argument
	ArgCount int    // 0 for bool, 1 for value, 2 for key=value pair
}

var options = []option{
	// Startup
	{Long: "version", Short: 'V'},
	{Long: "help", Short: 'h'},

	// Logging
	{Long: "quiet", Short: 'q'},
	{Long: "verbose", Short: 'v'},
	{Long: "no-verbose"},
	{Long: "server-response", Short: 'S'},
	{Long: "output-file", Short: 'o', ArgName: "FILE"},
	{Long: "append-output", Short: 'a', ArgName: "FILE"},

	// Download behavior
	{Long: "output-document", Short: 'O', ArgName: "FILE"},
	{Long: "continue", Short: 'c'},
	{Long: "no-clobber"},
	{Long: "timestamping", Short: 'N'},
	{Long: "background", Short: 'b'},
	{Long: "input-file", Short: 'i', ArgName: "FILE"},
	{Long: "tries", Short: 't', ArgName: "NUMBER"},
	{Long: "timeout", Short: 'T', ArgName: "SECONDS"},
	{Long: "connect-timeout", ArgName: "SECONDS"},
	{Long: "read-timeout", ArgName: "SECONDS"},
	{Long: "retry-connrefused"},
	{Long: "retry-on-http-error", ArgName: "CODES"},
	{Long: "content-disposition"},
	{Long: "no-use-server-timestamps"},
	{Long: "spider"},
	{Long: "progress", ArgName: "TYPE"},

	// Request metadata
	{Long: "header", ArgName: "STRING"},
	{Long: "user-agent", ArgName: "STRING"},
	{Long: "referer", ArgName: "URL"},

	// TLS and FTPS
	{Long: "no-check-certificate"},
	{Long: "ftps-implicit"},

	// Parallel retrieval
	{Long: "connections", ArgName: "NUMBER"},
	{Long: "split-size", ArgName: "SIZE"},
	{Long: "buffer-size", ArgName: "SIZE"},
	{Long: "no-parallel"},
}

// optByName maps long option names to their definitions.
var optByName = buildOptMap()

func buildOptMap() map[string]*option {
	m := make(map[string]*option)
	for i := range options {
		m[options[i].Long] = &options[i]
	}
	return m
}

// Parse parses command-line arguments and returns an execution plan.
func Parse(args []string) (*ExecutionPlan, error) {
	p := &parser{
		plan: &ExecutionPlan{
			Connections:  defaultConnections,
			SplitSize:    defaultSplitSize,
			BufferSize:   defaultBufferSize,
			MaxTries:     defaultMaxTries,
			Timeout:      defaultTimeout,
			ProgressType: defaultProgress,
			OutputMode:   OutputFile,
		},
	}

	remaining, err := p.parseOptions(args)
	if err != nil {
		return nil, err
	}

	// Collect positional URLs.
	for _, a := range remaining {
		if a == "-" {
			// "-" as a positional argument means stdin (for -i) or stdout (for -O).
			// In this context, it's treated as a URL placeholder; the plan
			// handles it based on output mode.
			continue
		}
		p.plan.URLs = append(p.plan.URLs, a)
	}

	// Read URLs from input file.
	if p.plan.InputFile != "" {
		data, err := os.ReadFile(p.plan.InputFile)
		if err != nil {
			return nil, fmt.Errorf("reading input file %s: %w", p.plan.InputFile, err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line != "" && !strings.HasPrefix(line, "#") {
				p.plan.URLs = append(p.plan.URLs, line)
			}
		}
	}

	if err := p.validate(); err != nil {
		return nil, err
	}

	return p.plan, nil
}

type parser struct {
	plan    *ExecutionPlan
	headers []string
}

func (p *parser) parseOptions(args []string) ([]string, error) {
	var positional []string
	stopOptions := false

	for i := 0; i < len(args); i++ {
		arg := args[i]

		if stopOptions {
			positional = append(positional, arg)
			continue
		}

		if arg == "--" {
			stopOptions = true
			continue
		}

		if arg == "-" {
			positional = append(positional, arg)
			continue
		}

		if strings.HasPrefix(arg, "--") {
			if err := p.parseLongOption(arg, &i, args); err != nil {
				return nil, err
			}
		} else if strings.HasPrefix(arg, "-") && len(arg) > 1 && arg != "-" {
			// Could be "-#" where # is a number (negative number), not a flag.
			if isNegativeNumber(arg) {
				positional = append(positional, arg)
				continue
			}
			if err := p.parseShortOptions(arg[1:], &i, args); err != nil {
				return nil, err
			}
		} else {
			positional = append(positional, arg)
		}
	}

	return positional, nil
}

func (p *parser) parseLongOption(arg string, idx *int, args []string) error {
	// Strip "--".
	name := arg[2:]
	value := ""

	// Handle --name=value.
	if eq := strings.IndexByte(name, '='); eq >= 0 {
		value = name[eq+1:]
		name = name[:eq]
	}

	opt, ok := optByName[name]
	if !ok {
		return fmt.Errorf("unrecognized option '--%s'", name)
	}

	if value == "" && opt.ArgName != "" {
		// Consume next arg as value.
		if *idx+1 < len(args) {
			*idx++
			value = args[*idx]
		} else {
			return fmt.Errorf("option '--%s' requires an argument", name)
		}
	}

	return p.setOption(opt, value)
}

// knownCombined maps combined short flags to their expanded form.
var knownCombined = map[string][]string{
	"nc": {"no-clobber", "continue"},
	"nv": {"no-verbose"},
}

func (p *parser) parseShortOptions(flags string, idx *int, args []string) error {
	// Check for known combined forms first.
	if expanded, ok := knownCombined[flags]; ok {
		for _, name := range expanded {
			opt := optByName[name]
			if opt == nil {
				return fmt.Errorf("internal: unknown option %q in combined form", name)
			}
			if err := p.setOption(opt, ""); err != nil {
				return err
			}
		}
		return nil
	}

	for i := 0; i < len(flags); i++ {
		r, _ := utf8.DecodeRuneInString(flags[i:])
		if r == utf8.RuneError {
			continue
		}

		var opt *option
		for j := range options {
			if options[j].Short == r {
				opt = &options[j]
				break
			}
		}
		if opt == nil {
			return fmt.Errorf("invalid option -- '%c'", r)
		}

		value := ""
		if opt.ArgName != "" {
			if i < len(flags)-1 {
				// Rest of the short flags group is the value.
				value = flags[i+utf8.RuneLen(r):]
				i = len(flags) // consumed rest
			} else {
				// Consume next arg.
				if *idx+1 < len(args) {
					*idx++
					value = args[*idx]
				} else {
					return fmt.Errorf("option '-%c' requires an argument", r)
				}
			}
		}
		if err := p.setOption(opt, value); err != nil {
			return err
		}
	}
	return nil
}

func (p *parser) setOption(opt *option, value string) error {
	switch opt.Long {
	// Startup
	case "version":
		fmt.Printf("pget %s (%s)\n", version.Version, version.Commit)
		os.Exit(0)
	case "help":
		printHelp()
		os.Exit(0)

	// Logging
	case "quiet":
		p.plan.Quiet = true
	case "verbose":
		p.plan.Verbose = true
	case "no-verbose":
		p.plan.NoVerbose = true
	case "server-response":
		p.plan.ServerResp = true
	case "output-file":
		p.plan.LogMode = LogFile
		p.plan.LogFile = value
	case "append-output":
		p.plan.LogMode = LogAppend
		p.plan.LogFile = value

	// Download behavior
	case "output-document":
		p.plan.OutputFile = value
		if value == "-" {
			p.plan.OutputMode = OutputStdout
		} else {
			p.plan.OutputMode = OutputSingle
		}
	case "continue":
		p.plan.ContinueMode = ContinueAuto
	case "no-clobber":
		p.plan.NoClobber = true
	case "timestamping":
		p.plan.TimestampMode = TimestampCheck
	case "background":
		p.plan.Background = BackgroundChild
	case "input-file":
		p.plan.InputFile = value
	case "tries":
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 {
			return fmt.Errorf("invalid number of tries: %s", value)
		}
		p.plan.MaxTries = n
	case "timeout":
		d, err := parseSeconds(value)
		if err != nil {
			return fmt.Errorf("invalid timeout: %s", value)
		}
		p.plan.Timeout = d
	case "connect-timeout":
		d, err := parseSeconds(value)
		if err != nil {
			return fmt.Errorf("invalid connect timeout: %s", value)
		}
		p.plan.ConnectTimeout = d
	case "read-timeout":
		d, err := parseSeconds(value)
		if err != nil {
			return fmt.Errorf("invalid read timeout: %s", value)
		}
		p.plan.ReadTimeout = d
	case "retry-connrefused":
		p.plan.RetryConnRefused = true
	case "retry-on-http-error":
		codes := strings.Split(value, ",")
		for _, c := range codes {
			code, err := strconv.Atoi(strings.TrimSpace(c))
			if err != nil {
				return fmt.Errorf("invalid HTTP error code: %s", strings.TrimSpace(c))
			}
			p.plan.RetryOnHTTPError = append(p.plan.RetryOnHTTPError, code)
		}
	case "content-disposition":
		p.plan.ContentDisposition = true
	case "no-use-server-timestamps":
		p.plan.NoUseServerTimestamps = true
	case "spider":
		p.plan.Spider = true
	case "progress":
		p.plan.ProgressType = value

	// Request metadata
	case "header":
		p.headers = append(p.headers, value)
	case "user-agent":
		p.plan.UserAgent = value
	case "referer":
		p.plan.Referer = value

	// TLS and FTPS
	case "no-check-certificate":
		p.plan.InsecureSkipVerify = true
	case "ftps-implicit":
		p.plan.FTPSImplicit = true

	// Parallel
	case "connections":
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 {
			return fmt.Errorf("invalid number of connections: %s", value)
		}
		p.plan.Connections = n
	case "split-size":
		n, err := parseSize(value)
		if err != nil {
			return fmt.Errorf("invalid split size: %s", value)
		}
		p.plan.SplitSize = n
	case "buffer-size":
		n, err := parseSize(value)
		if err != nil {
			return fmt.Errorf("invalid buffer size: %s", value)
		}
		p.plan.BufferSize = n
	case "no-parallel":
		p.plan.NoParallel = true
	}
	return nil
}

func (p *parser) validate() error {
	// Reject -b -O -.
	if p.plan.Background == BackgroundChild && p.plan.OutputMode == OutputStdout {
		return fmt.Errorf("background mode with stdout output is not supported")
	}

	// Parse headers into map.
	if len(p.headers) > 0 {
		p.plan.ExtraHeaders = make(map[string]string)
		for _, h := range p.headers {
			parts := strings.SplitN(h, ":", 2)
			if len(parts) != 2 {
				return fmt.Errorf("invalid header format: %s (expected 'Name: Value')", h)
			}
			p.plan.ExtraHeaders[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}

	// Validate parallel settings.
	if p.plan.BufferSize < p.plan.SplitSize {
		return fmt.Errorf("buffer size (%d) must be at least split size (%d)",
			p.plan.BufferSize, p.plan.SplitSize)
	}

	// --no-parallel forces connections=1.
	if p.plan.NoParallel {
		p.plan.Connections = 1
	}

	// Validate output modes.
	if p.plan.OutputMode == OutputStdout && p.plan.ContinueMode == ContinueAuto {
		return fmt.Errorf("cannot use --continue with stdout output")
	}

	if p.plan.OutputMode != OutputFile && p.plan.InputFile != "" {
		// -i with -O: URLs from input file go to the concatenated output.
		// This is valid.
	}

	return nil
}

func isNegativeNumber(s string) bool {
	if len(s) < 2 || s[0] != '-' {
		return false
	}
	for _, c := range s[1:] {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func parseSeconds(s string) (time.Duration, error) {
	// Support "1.5", "90", etc.
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	return time.Duration(f * float64(time.Second)), nil
}

func parseSize(s string) (int64, error) {
	s = strings.ToUpper(strings.TrimSpace(s))
	multiplier := int64(1)

	if strings.HasSuffix(s, "K") || strings.HasSuffix(s, "KB") {
		multiplier = 1024
	} else if strings.HasSuffix(s, "M") || strings.HasSuffix(s, "MB") || strings.HasSuffix(s, "MIB") {
		multiplier = 1024 * 1024
	} else if strings.HasSuffix(s, "G") || strings.HasSuffix(s, "GB") || strings.HasSuffix(s, "GIB") {
		multiplier = 1024 * 1024 * 1024
	}

	// Strip suffix.
	for i, c := range s {
		if c < '0' || c > '9' {
			s = s[:i]
			break
		}
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return n * multiplier, nil
}

func printHelp() {
	fmt.Print(`Usage: pget [OPTION]... [URL]...

Wget-style downloader with parallel chunked retrieval and ordered streaming.

Startup:
  -V, --version                Display version and exit
  -h, --help                   Display this help and exit

Logging and status:
  -q, --quiet                  Quiet mode (no progress output)
  -v, --verbose                Verbose output
  -nv, --no-verbose            Turn off verbose without being quiet
  -S, --server-response        Print server response headers
  -o, --output-file=FILE       Log messages to FILE
  -a, --append-output=FILE     Append log messages to FILE

Download behavior:
  -O, --output-document=FILE   Write to FILE ("-" for stdout)
  -c, --continue               Resume partially downloaded file
  -nc, --no-clobber            Skip downloads that would overwrite files
  -N, --timestamping           Only download if remote file is newer
  -b, --background             Go to background after startup
  -i, --input-file=FILE        Read URLs from FILE
  -t, --tries=NUMBER           Set number of retries (default 20)
  -T, --timeout=SECONDS        Set network timeout
      --connect-timeout=SECONDS Set connect timeout
      --read-timeout=SECONDS   Set read timeout
      --retry-connrefused      Retry even if connection is refused
      --retry-on-http-error=CODES  Comma-separated list of HTTP status codes to retry
      --content-disposition    Use Content-Disposition filename
      --no-use-server-timestamps  Don't set local file mtime from server
      --spider                 Don't download anything
      --progress=TYPE          Set progress type (bar or dot)

Request metadata:
      --header=STRING          Add header to request
      --user-agent=STRING      Set user agent
      --referer=URL            Set Referer header

TLS and FTPS:
      --no-check-certificate   Don't verify server TLS certificate
      --ftps-implicit          Use implicit FTPS

Parallel retrieval:
      --connections=NUMBER     Max connections per URL (default 8)
      --split-size=SIZE        Chunk size (default 8M)
      --buffer-size=SIZE       Stream buffer limit (default 128M)
      --no-parallel            Disable parallel download
`)
}
