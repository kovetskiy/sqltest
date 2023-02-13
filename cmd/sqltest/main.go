package main

import (
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
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
  --debug               Enable debug logging.
  -h --help             Show this screen.
  --version             Show version.
`
)

type Arguments struct {
	DirIn       string `docopt:"<in>"`
	DirExpected string `docopt:"<expected>"`
	DatabaseURI string `docopt:"--db"`
	Setup       string `docopt:"--setup"`
	Teardown    string `docopt:"--teardown"`
	DoNotRemove bool   `docopt:"--no-rm"`
	Debug       bool   `docopt:"--debug"`
}

type Tester struct {
	db       *pgx.Conn
	dbConfig *pgx.ConnConfig

	dirIn       string
	dirExpected string
	dirOut      string

	setup    string
	teardown string

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

	if args.Debug {
		log.SetLevel(log.LevelDebug)
	}

	defer os.Stdout.Sync()

	dbConfig, err := pgx.ParseConfig(args.DatabaseURI)
	if err != nil {
		panic(err)
	}

	db, err := pgx.ConnectConfig(context.Background(), dbConfig)
	if err != nil {
		panic(err)
	}

	defer db.Close(context.Background())

	testcases, err := LoadTestcases(args.DirIn, args.DirExpected)
	if err != nil {
		panic(err)
	}

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
			db:          db,
			dbConfig:    dbConfig,
			dirIn:       args.DirIn,
			dirExpected: args.DirExpected,
			dirOut:      outputDirectory,

			setup:    args.Setup,
			teardown: args.Teardown,
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
		return karma.Format(err, "exec testcase")
	}

	err = tester.runTeardown()
	if err != nil {
		return karma.Format(err, "run test")
	}

	return nil
}

func (tester *Tester) runSetup() error {
	if tester.setup == "" {
		return nil
	}

	return tester.runExternal(tester.setup)
}

func (tester *Tester) runTeardown() error {
	if tester.teardown == "" {
		return nil
	}

	return tester.runExternal(tester.teardown)
}

func (tester *Tester) runExternal(command string) error {
	cmd := exec.Command("sh", "-c", command)

	cmd.Env = append(
		os.Environ(),
		"SQLTEST_DATABASE_NAME="+tester.testcase.dbConfig.Database,
		"SQLTEST_DATABASE_URI="+updateDatabaseURI(
			tester.testcase.dbConfig.ConnString(),
			tester.testcase.dbConfig.Database,
		),
		"SQLTEST_TESTCASE_NAME="+tester.testcase.Name,
		"SQLTEST_TESTCASE_FILENAME="+tester.testcase.Filename,
		"SQLTEST_TESTCASE_DIR_IN="+tester.dirIn,
		"SQLTEST_TESTCASE_DIR_OUT="+tester.dirExpected,
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
		filepath.Join(tester.dirIn, tester.testcase.Filename),
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

	expected := filepath.Join(tester.dirExpected, tester.testcase.Filename)

	err = ensureFileExists(expected)
	if err != nil {
		return karma.Format(err, "ensure expected file exists")
	}

	err = runDiff(expected, actual)
	if err != nil {
		return err
	}

	return nil
}

func runDiff(expected string, actual string) error {
	cmd := exec.Command("diff", "-u", expected, actual)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
		return karma.Format(
			err,
			"diff failed: %s != %s",
			expected,
			actual,
		)
	}

	return nil
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
