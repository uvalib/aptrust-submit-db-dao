# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

A Go library (no executable) providing a Postgres data-access layer for the UVA APTrust submission system. The single package `uvaaptsdao` lives in the `uvaaptsdao/` subdirectory, which is also where `go.mod` lives (module `github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao`). Consumers (e.g. the tracksys importer and libra-open reconciliation tooling mentioned in commit history) import this package; the database schema itself is not defined in this repo.

## Commands

All Makefile targets `cd` into `uvaaptsdao/` first; if running `go` directly, do the same.

- `make build` / `make test` — run tests (`go test -v`). There are currently no `_test.go` files.
- `make test TEST=Name` — run a single test via `-run`.
- `make vet`, `make fmt`
- `make check` — installs and runs `staticcheck` (with `-checks all,-ST1000,-S1002,-ST1003,-ST1020,-ST1021,-ST1022`) and the `shadow` vet analyzer.
- `make dep` — `go get -u` + `go mod tidy`.

## Architecture

- `factory.go` — `NewDao(host, port, user, password, dbname)` opens and pings a `lib/pq` connection. `Dao` embeds `*sql.DB`, so methods call `dao.Query` / `dao.Prepare` directly.
- `dao-api.go` — public model structs, sentinel errors (`ErrSubmissionNotFound`, etc.), and submission/bag status string constants.
- `select.go`, `insert.go`, `update.go`, `delete.go` — `Dao` methods grouped by SQL verb.
- `helpers.go` — per-type row scanners (`xxxQueryResults` for single rows, `xxxListQueryResults` for slices), `execPrepared`, and `funcEntry`.

### Conventions to follow when adding methods

- Every public method starts with `funcExit := funcEntry("uvaaptsdao.MethodName"); defer funcExit()` (prints DEBUG entry/exit timing to stdout).
- Reads: `dao.Query(...)`, `defer rows.Close()`, then hand the rows to a helper scanner. Writes: `dao.Prepare(...)`, `defer stmt.Close()`, `execPrepared(stmt, ...)`.
- Scanner helpers return a wrapped sentinel error (`fmt.Errorf("%q: %w", ..., ErrXxxNotFound)`) when zero rows are found — list queries return an error, not an empty slice, on no results. Callers use `errors.Is`.
- The column order in the SELECT must exactly match the `rows.Scan` order in the helper; helpers are shared across several queries, so changing one affects all callers.
- Entities are keyed by string identifiers (submission identifier, client identifier, bag name) rather than numeric ids; bags are unique per (submission, bag_name).

### State model

Submission and bag status are **append-only history tables** (`submission_states`, `bag_states`). `UpdateSubmissionState` / `UpdateBagState` insert a new row rather than updating; the "current" state is the row with `MAX(id)` for that submission (or submission + bag_name). `AddSubmission` and `AddBag` automatically insert the initial `registered` state. `AddSubmissionState` / `AddBagState` insert with an explicit timestamp (used by importers backfilling history).

### Other tables

`files` (per-submission files with hash/size), `apt_files` (cache of what's already in APTrust, used for hash-conflict detection), `submission_conflicts` (links a new file to a conflicting file), `submission_failures`, `approvals`, `hash_allowlist`, `bag_allowlist`. Delete methods are per-table by submission; there is no single cascading delete helper, so callers remove related rows table by table.
