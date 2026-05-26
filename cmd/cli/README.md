<div align="center">
  <a href="../../README.md">DripSQL</a>
  /
  <a href="../../examples/README.md">Examples</a>
  /
  <a href="../bench/README.md">Benchmarks</a>
</div>

# DripSQL - CLI

Interactive shell and one-shot SQL runner for the embedded engine. The `-db` directory is created on first run.

```
go run ./cmd/cli -db <path> [-exec "<sql>"] [-format table|tsv|json]
```

## Flags

| Flag | Default | Description |
| --- | --- | --- |
| `-db` | required | Database directory |
| `-exec` | unset | Run one SQL string (may contain multiple `;`-separated statements) and exit |
| `-format` | `table` | Output format for query results, one of `table`, `tsv`, or `json` |

## Meta commands

Anything starting with `.` on a fresh prompt is a meta command instead of SQL.

| Command | Description |
| --- | --- |
| `.help` | Print the meta command list |
| `.quit`, `.exit` | Exit the shell |
| `.tables` | List catalog tables |
| `.schema NAME` | Print columns and nullability for one table |
| `.mode table\|tsv\|json` | Switch output format for subsequent queries |
| `.timer on\|off` | Toggle the per-statement `time` footer |

## Shell

The prompt switches from `dripsql>` to `   ...>` while a statement is still open and a top-level `;` triggers execution. Single-quoted literals including the `''` escape are respected so `;` inside `'a;b'` does not split. History is stored at `$XDG_CACHE_HOME/dripsql/shell-history` on Unix and `%LOCALAPPDATA%/dripsql/shell-history` on Windows.
