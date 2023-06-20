package main

import (
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/docopt/docopt-go"
	"github.com/jackc/pgx/v5"
	"github.com/reconquest/executil-go"
	"github.com/reconquest/karma-go"
	"github.com/reconquest/pkg/log"
)

var (
	version = "1.0"
	usage   = "sqltest " + version + `

Usage:
  sqltest [options] <in> <expected>
  sqltest -h | --help
  sqltest --version

Options:
  -d --db <uri>         PostgreSQL connection URI [default: postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable]
  --setup <command>     Command to run before test.
  --teardown <command>  Command to run after test.
  --no-rm               Do not remove output files.
  --approve <filter>    Approve results of testcases matching filter].
                         Example: --approve . to approve all testcases.
  -r --run <filter>     Run only testcases matching filter. [default: .]
  --debug               Enable debug logging.
  -h --help             Show this screen.
  --version             Show version.
`
)

type Arguments struct {
	ValueDirIn       string `docopt:"<in>"`
	ValueDirExpected string `docopt:"<expected>"`
	ValueDatabaseURI string `docopt:"--db"`
	ValueSetup       string `docopt:"--setup"`
	ValueTeardown    string `docopt:"--teardown"`

	FlagDoNotRemove bool `docopt:"--no-rm"`
	FlagDebug       bool `docopt:"--debug"`

	ValueApprove string `docopt:"--approve"`
	ValueRun     string `docopt:"--run"`
}

type Tester struct {
	args     Arguments
	db       *pgx.Conn
	dbConfig *pgx.ConnConfig

	dirOut string

	testcase struct {
		Testcase

		dbConfig *pgx.ConnConfig
	}
}

func main() {
	doc, err := docopt.ParseDoc(usage)
	if err != nil {
		panic(err)
	}

	var args Arguments
	err = doc.Bind(&args)
	if err != nil {
		panic(err)
	}

	if args.FlagDebug {
		log.SetLevel(log.LevelDebug)
	}

	defer os.Stdout.Sync()

	dbConfig, err := pgx.ParseConfig(args.ValueDatabaseURI)
	if err != nil {
		panic(err)
	}

	db, err := pgx.ConnectConfig(context.Background(), dbConfig)
	if err != nil {
		panic(err)
	}

	defer db.Close(context.Background())

	testcases, err := LoadTestcases(args.ValueDirIn, args.ValueDirExpected)
	if err != nil {
		panic(err)
	}

	testcases = filter(testcases, func(testcase Testcase) bool {
		return match(testcase.Name, args.ValueRun)
	})

	outputDirectory, err := os.MkdirTemp(
		os.TempDir(),
		"sqltest_"+time.Now().Format("20060102150405"),
	)
	if err != nil {
		panic(err)
	}

	defer func() {
		err := os.RemoveAll(outputDirectory)
		if err != nil {
			log.Errorf(err, "remove output directory")
		}
	}()

	allStartedAt := time.Now()

	failed := 0
	for _, testcase := range testcases {
		tester := Tester{
			args:     args,
			db:       db,
			dbConfig: dbConfig,
			dirOut:   outputDirectory,
		}

		startedAt := time.Now()

		err := tester.Run(testcase)

		took := time.Since(startedAt)

		if err != nil {
			log.Errorf(err, "FAIL %s (%v)", testcase.Name, took)

			failed++
		} else {
			log.Infof(nil, "PASS %s (%v)", testcase.Name, took)
		}
	}

	if failed > 0 {
		log.Fatalf(
			nil,
			"FAIL %d/%d (%.2f%%) testcases (%v)",
			failed,
			len(testcases),
			float64(failed)/float64(len(testcases))*100,
			time.Since(allStartedAt),
		)
	}

	log.Infof(
		nil,
		"PASS %d testcases (%v)",
		len(testcases),
		time.Since(allStartedAt),
	)
}

type Testcase struct {
	Name     string
	Filename string
}

func LoadTestcases(dirIn string, dirOut string) ([]Testcase, error) {
	entries, err := os.ReadDir(dirIn)
	if err != nil {
		return nil, err
	}

	testcases := make([]Testcase, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		testcases = append(testcases, Testcase{
			Name:     strings.TrimSuffix(entry.Name(), ".sql"),
			Filename: entry.Name(),
		})
	}

	return testcases, nil
}

func (tester *Tester) Run(testcase Testcase) error {
	tester.testcase.Testcase = testcase

	dbname := "sqltest_" + time.Now().Format("20060102150405")

	_, err := tester.db.Exec(context.Background(), "CREATE DATABASE "+dbname)
	if err != nil {
		return karma.Format(err, "create database")
	}

	defer func() {
		_, err := tester.db.Exec(context.Background(), "DROP DATABASE "+dbname)
		if err != nil {
			log.Errorf(err, "drop database")
		}
	}()

	tester.testcase.dbConfig = tester.dbConfig.Copy()
	tester.testcase.dbConfig.Database = dbname

	err = tester.runSetup()
	if err != nil {
		return karma.Format(err, "run setup")
	}

	err = tester.exec()
	if err != nil {
		return err
	}

	err = tester.runTeardown()
	if err != nil {
		return karma.Format(err, "run test")
	}

	return nil
}

func (tester *Tester) runSetup() error {
	if tester.args.ValueSetup == "" {
		return nil
	}

	return tester.runExternal(tester.args.ValueSetup)
}

func (tester *Tester) runTeardown() error {
	if tester.args.ValueTeardown == "" {
		return nil
	}

	return tester.runExternal(tester.args.ValueTeardown)
}

func (tester *Tester) runExternal(command string) error {
	cmd := exec.Command("sh", "-c", command)

	cmd.Env = append(
		os.Environ(),
		"SQLTEST_TESTCASE_DATABASE_NAME="+tester.testcase.dbConfig.Database,
		"SQLTEST_TESTCASE_DATABASE_URI="+updateDatabaseURI(
			tester.testcase.dbConfig.ConnString(),
			tester.testcase.dbConfig.Database,
		),
		"SQLTEST_TESTCASE_NAME="+tester.testcase.Name,
		"SQLTEST_TESTCASE_FILENAME="+tester.testcase.Filename,
		"SQLTEST_TESTCASE_DIR_IN="+tester.args.ValueDirIn,
		"SQLTEST_TESTCASE_DIR_EXPECTED="+tester.args.ValueDirExpected,
	)

	_, _, err := executil.Run(cmd)
	if err != nil {
		return karma.Format(err, "run external command")
	}

	return nil
}

func (tester *Tester) exec() error {
	args := []string{
		"psql",
		"-v",
		"ON_ERROR_STOP=1",
		"-f",
		filepath.Join(tester.args.ValueDirIn, tester.testcase.Filename),
		updateDatabaseURI(
			tester.testcase.dbConfig.ConnString(),
			tester.testcase.dbConfig.Database,
		),
	}

	log.Debugf(nil, "exec: %s", args)

	cmd := exec.Command(args[0], args[1:]...)

	actual := filepath.Join(tester.dirOut, tester.testcase.Filename)

	out, err := os.Create(actual)
	if err != nil {
		return karma.Format(err, "create actual file")
	}

	defer out.Close()

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return karma.Format(err, "get stdout pipe")
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return karma.Format(err, "get stderr pipe")
	}

	go io.Copy(os.Stderr, stderr)
	go io.Copy(out, stdout)

	err = cmd.Run()
	if err != nil {
		return err
	}

	err = out.Sync()
	if err != nil {
		return karma.Format(err, "sync actual file")
	}

	expected := filepath.Join(tester.args.ValueDirExpected, tester.testcase.Filename)

	err = ensureFileExists(expected)
	if err != nil {
		return karma.Format(err, "ensure expected file exists")
	}

	if tester.approved() {
		log.Infof(nil, "approve: %s", tester.testcase.Name)

		err = copyFile(actual, expected)
		if err != nil {
			return karma.Format(err, "copy actual to expected")
		}

		return nil
	}

	diff, err := runDiff(expected, actual)
	if err != nil {
		return err
	}

	if len(diff) > 0 {
		return karma.
			Describe("diff", "\n"+string(diff)).
			Format(nil, "diff is not empty")
	}

	return nil
}

func (tester *Tester) approved() bool {
	if tester.args.ValueApprove == "" {
		return false
	}

	pattern := regexp.MustCompile(tester.args.ValueApprove)

	return pattern.MatchString(tester.testcase.Name)
}

func runDiff(expected string, actual string) ([]byte, error) {
	cmd := exec.Command("diff", "-u", expected, actual)
	stdout, _, err := executil.Run(cmd)
	if err != nil {
		if len(stdout) == 0 {
			return nil, karma.Format(err, "diff command failed (no stdout)")
		}

		return stdout, nil
	}

	return nil, nil
}

func ensureFileExists(filename string) error {
	_, err := os.Stat(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			_, err := os.Create(filename)
			return err
		}

		return karma.Format(err, "stat file")
	}

	return nil
}

func updateDatabaseURI(originalURI string, dbname string) string {
	uri, err := url.Parse(originalURI)
	if err != nil {
		panic(err)
	}

	uri.Path = "/" + dbname

	return uri.String()
}

func copyFile(src string, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}

	err = out.Sync()
	if err != nil {
		return err
	}

	return nil
}

func filter[T comparable](values []T, predicate func(T) bool) []T {
	result := make([]T, 0)

	for _, value := range values {
		if predicate(value) {
			result = append(result, value)
		}
	}

	return result
}

func match(value string, pattern string) bool {
	re := regexp.MustCompile(pattern)

	return re.MatchString(value)
}
