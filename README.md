# sqltest

A tool to run unit tests on PostgreSQL sql files.

1. You have a bunch of SQL files with INSERT and SELECT in them. You also know what the expected output is.
    Put the source queries in a directory, and the expected output in another directory, use the same name for the files.
2. Run sqltest on the directory with the source queries and the directory with the expected output.
3. sqltest: create a temporary database
4. sqltest: run setup instructions (a command specified in --setup) while providing the database
   name as an environment variable.
5. sqltest: run the source queries in the source directory, and compare the output to the expected output.
    a. If different, print the diff and exit with an error code.
6. sqltest: run --teardown command while providing the database name as an environment variable.
7. sqltest: drop the temporary database.

<table border="0">
<tr>
<td style="vertical-align: top">
<b>Source</b>
</td>
<td style="vertical-align: top">
<b>Expected</b>
</td>
</tr>
<tr>
<td style="vertical-align: top">

```sql
SELECT 1 + 1 as data;

BEGIN;
SELECT 2+2 as data;
COMMIT;
```

</td>
<td>

```sql
 data 
------
    2
(1 row)

BEGIN
 data 
------
    4
(1 row)

COMMIT
```

</td>
</tr></table>

## Installation

```
go install github.com/kovetskiy/sqltest/cmd/sqltest@latest
```

## Usage

```
Usage:
  sqltest [options] <in> <expected>
  sqltest -h | --help
  sqltest --version

Options:
  -d --db <uri>         PostgreSQL connection URI [default: postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable]
  --setup <command>     Command to run before test.
  --teardown <command>  Command to run after test.
  --no-rm 			    Do not remove output files.
  -h --help             Show this screen.
  --version             Show version.
```

## Example

```
sqltest ./testdata/testcases ./testdata/expected -d 'postgres://a:b@host/dev?sslmode=disable'
2023-02-13 12:01:35.989 INFO  PASS simple-math (772.895681ms)
2023-02-13 12:01:35.989 INFO  PASS 1 testcases (772.933208ms)
```

## Environment variables:

Name                        | Description
---                         | ---
`SQLTEST_DATABASE_NAME`     | The name of the database used for the test
`SQLTEST_DATABASE_URI`      | The connection string used for the test
`SQLTEST_TESTCASE_NAME`     | The name of the test
`SQLTEST_TESTCASE_FILENAME` | The filename of the test
`SQLTEST_TESTCASE_DIR_IN`   | The directory where the test is located
`SQLTEST_TESTCASE_DIR_OUT`  | The directory where the expected output is located

## License

MIT
