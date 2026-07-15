## DuckDB foundation ----------------------------------------------------------
##
## A thin, reusable layer that lets targets read parquet/csv straight from S3 and
## push joins + group-bys into DuckDB, so only small aggregated results ever cross
## into R. Replaces the paws-based `initialize_s3()` read pattern.
##
## Boundary (decided): DuckDB does IO + joins + aggregation; R keeps the stats
## (fixest, irlba, igraph, weighted moments finalisation stays in SQL here since
## it is exact and cheap). See notes/duckdb_migration.md.

#' Quote a value as a single-quoted SQL string literal (or SQL NULL).
sql_str <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x) || !nzchar(x)) return("NULL")
  paste0("'", gsub("'", "''", x), "'")
}

#' Build an s3:// URI from a bucket + key.
duck_s3_uri <- function(key, bucket = "scoavoux") {
  sprintf("s3://%s/%s", bucket, key)
}

#' Build a DuckDB list literal of s3 URIs, e.g. ['s3://b/a','s3://b/c'].
duck_uri_list <- function(keys, bucket = "scoavoux") {
  quoted <- vapply(duck_s3_uri(keys, bucket), sql_str, character(1))
  paste0("[", paste(quoted, collapse = ","), "]")
}

#' Attach S3 credentials to a live DuckDB connection.
#'
#' Reads the same environment variables as `initialize_s3()`. The endpoint is the
#' bare host (no scheme); URL_STYLE 'path' is required for non-AWS S3 (MinIO /
#' SSP Cloud). A fresh secret is created per connection, which is exactly what we
#' want given the weekly-expiring session token — every target run re-reads the
#' current token from the environment.
duck_attach_s3 <- function(con) {
  DBI::dbExecute(con, "INSTALL httpfs; LOAD httpfs;")
  # Only emit the parameters that are actually set, so permanent-credential
  # setups (no session token) or an unset region don't produce `X NULL`.
  flags <- c("TYPE s3", "PROVIDER config", "URL_STYLE 'path'", "USE_SSL true")
  creds <- c(
    KEY_ID        = Sys.getenv("AWS_ACCESS_KEY_ID"),
    SECRET        = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
    SESSION_TOKEN = Sys.getenv("AWS_SESSION_TOKEN"),
    REGION        = Sys.getenv("AWS_DEFAULT_REGION"),
    ENDPOINT      = Sys.getenv("AWS_S3_ENDPOINT"))       # bare host, e.g. minio.lab.sspcloud.fr
  creds <- creds[nzchar(creds)]
  kv <- paste(names(creds), vapply(unname(creds), sql_str, character(1)))
  DBI::dbExecute(con, sprintf(
    "CREATE OR REPLACE SECRET s3secret ( %s )", paste(c(flags, kv), collapse = ", ")))
  invisible(con)
}

#' Open a configured DuckDB connection.
#'
#' @param dbdir on-disk database file; a temp file by default. On-disk (rather
#'   than ':memory:') lets large joins spill instead of OOM-ing.
#' @param threads,memory_limit optional engine limits (e.g. 8, '24GB').
#' @param temp_directory where DuckDB spills; created if missing.
#' @param s3 whether to attach S3 credentials (set FALSE for local-only work).
duck_connect <- function(dbdir = tempfile(fileext = ".duckdb"),
                         threads = NULL,
                         memory_limit = NULL,
                         temp_directory = "data/temp/duck_spill",
                         s3 = TRUE) {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = dbdir)
  if (!is.null(threads))      DBI::dbExecute(con, sprintf("PRAGMA threads=%d", as.integer(threads)))
  if (!is.null(memory_limit)) DBI::dbExecute(con, sprintf("PRAGMA memory_limit=%s", sql_str(memory_limit)))
  if (!is.null(temp_directory)) {
    dir.create(temp_directory, showWarnings = FALSE, recursive = TRUE)
    DBI::dbExecute(con, sprintf("PRAGMA temp_directory=%s", sql_str(temp_directory)))
  }
  if (s3) duck_attach_s3(con)
  con
}

#' Run `fun(con, ...)` against a fresh connection, always disconnecting.
#'
#' This is how DuckDB lives inside a target: a live connection is not
#' serialisable and must not be cached as a target value.
with_duck <- function(fun, ...,
                      .dbdir = tempfile(fileext = ".duckdb"),
                      .s3 = TRUE, .threads = NULL, .memory_limit = NULL,
                      .temp_directory = "data/temp/duck_spill") {
  con <- duck_connect(dbdir = .dbdir, s3 = .s3, threads = .threads,
                      memory_limit = .memory_limit, temp_directory = .temp_directory)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  fun(con, ...)
}

## Generic readers (templates for migrating the remaining make_* imports) ------

#' Read parquet from S3 into R. `cols` NULL = all columns; `where` = raw SQL
#' predicate pushed to the scan. Prefer building larger queries with
#' read_parquet() directly when you can aggregate before collecting.
duck_read_parquet <- function(con, key, bucket = "scoavoux",
                              cols = NULL, where = NULL) {
  sel <- if (is.null(cols)) "*" else paste(cols, collapse = ", ")
  sql <- sprintf("SELECT %s FROM read_parquet(%s)", sel,
                 sql_str(duck_s3_uri(key, bucket)))
  if (!is.null(where)) sql <- paste(sql, "WHERE", where)
  DBI::dbGetQuery(con, sql)
}

#' Read a CSV from S3 into R (read_csv_auto). `cols` selects a subset.
duck_read_csv <- function(con, key, bucket = "scoavoux", cols = NULL) {
  sel <- if (is.null(cols)) "*" else paste(cols, collapse = ", ")
  DBI::dbGetQuery(con, sprintf(
    "SELECT %s FROM read_csv_auto(%s)", sel, sql_str(duck_s3_uri(key, bucket))))
}
