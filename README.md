[![Extension Test](https://github.com/isaacbrodsky/duckdb-zipfs/actions/workflows/MainDistributionPipeline.yml/badge.svg)](https://github.com/isaacbrodsky/duckdb-zipfs/actions/workflows/MainDistributionPipeline.yml)
[![DuckDB Version](https://img.shields.io/static/v1?label=duckdb&message=v1.5.5&color=blue)](https://github.com/duckdb/duckdb/releases/tag/v1.5.5)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

This is a [DuckDB](https://duckdb.org) extension that adds support for reading files from within [zip archives](https://en.wikipedia.org/wiki/ZIP_(file_format)) and other archive formats such as `tar`.


# Get started

Load from the [community extensions repository](https://community-extensions.duckdb.org/extensions/zipfs.html):
```SQL
INSTALL zipfs FROM community;
LOAD zipfs;
```

To read a file:
```SQL
SELECT * FROM 'zip://examples/a.zip/a.csv';
```

To read a file from azure blob storage (or other file system):
```SQL
SELECT * FROM 'zip://az://yourstorageaccount.blob.core.windows.net/yourcontainer/examples/a.zip/a.csv';
```

To read the table of contents of a zip file:
```SQL
SELECT * FROM archive_contents('examples/a.zip');
```

## File names

| URL quick reference | Description
| --- | ---
| `zip://a.zip/*.csv` | Local zip file named `a.zip`, containing csv files.
| `zip://http://example.com/a.zip/*.csv` | Web hosted zip file named `a.zip`, containing csv files.
| `archive://a.tar.gz!!*.csv` | Local archive file named `a.tar.gz`, containg csv files.
| `compressed://a.jsonl.bz2` | Local compressed ndjson file `a.jsonl.bz2`.

| Function | Description
| --- | ---
| `zip_contents` | Read the table of contents of a zip file
| `archive_contents` | Read the table of contents of an archive file

File names passed into the `zip://` URL scheme are expected to end with `.zip`, which indicates the end of the zip file name. The path after
that is taken to be the file path within the zip archive.

Globbing within the zip archive is supported, but see below for performance limitations. A glob query looks like:
```SQL
SELECT * FROM 'zip://examples/a.zip/*.csv';
```

Globbing for multiple zip files:
```SQL
SELECT * FROM 'zip://examples/*.zip/*.csv';
```

You may use options to turn this behavior off and instead choose some string to split on:
```SQL
SET zipfs_split = "!!";

SELECT * FROM 'zip://examples/a.zip!!b.csv';
```

Using `zipfs_split` also means you can read other archives supported by libarchive: (note different URL scheme, and libarchive is not available on Windows)
```SQL
SET zipfs_split = "!!";

SELECT * FROM 'archive://examples/a.tar.gz!!b.csv';
```

It is also possible to read from a variety of compressed file formats directly:
```SQL
SELECT * FROM read_json('compressed://examples/a.jsonl.bz2');
```

## Archive vs zip

This extension supports both zip files and archive files. The zip file support is using miniz, the archive file
support uses libarchive. libarchive supports a wider range of compression algorithms and container formats.
libarchive is not available on Windows and using them there will result in an error.

## Performance considerations

This extension is intended more for convience than high performance. It does not implement a file metadata cache as `tarfs` (on which this
extension is based) does. As such, operations which require the central directory (index) of the zip file, such as globbing files, must
reread the central directory multiple times, once for the glob and once for each file to open.

The selected file is **streamed**, not read entirely into memory. The decompression stream is kept open and read incrementally, so files
that are larger than memory when uncompressed can be read. Memory use per open file is bounded (a small fixed buffer) regardless of the
uncompressed size, so e.g. a 100 GB CSV inside a zip can be read on a machine with far less RAM.

This works best for sequential readers (`read_csv`, `read_json`/NDJSON), which read forward in a single pass — these are `O(n)` time with
bounded memory. Random access is still supported: forward seeks decompress-and-discard, while backward seeks restart the decompression
stream from the start of the entry. Formats that seek heavily (e.g. Parquet) therefore work but can be slow inside an archive. Because each
open file keeps a single decompression stream, reads on one handle are serialized and out-of-order/backward access re-decompresses from the
start, so reading an archived file is effectively single-threaded — extract large Parquet to plain storage first if you need parallel scans.

For archive entries that do not record their uncompressed size (raw `compressed://` gzip/bz2), the size is determined by a one-time
streaming pass that discards the data, which keeps memory bounded but decompresses the data twice (once to size, once to read). Entries in
`zip://` and `archive://` (tar etc.) record the size in the directory/header, so they are read in a single pass.

# Development

First, install vcpkg to `vcpkg`:

```sh
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

Then:

```sh
GEN=ninja make release
make test_release
```

# License

duckdb-zipfs Copyright 2025 Isaac Brodsky. Licensed under the [MIT License](./LICENSE).

[DuckDB](https://github.com/duckdb/duckdb) Copyright 2018-2022 Stichting DuckDB Foundation (MIT License)

[miniz](https://github.com/richgel999/miniz)
Copyright 2013-2014 RAD Game Tools and Valve Software
Copyright 2010-2014 Rich Geldreich and Tenacious Software LLC
(MIT License)

[DuckDB extension-template](https://github.com/duckdb/extension-template) Copyright 2018-2022 DuckDB Labs BV (MIT License)

[duckdb_tarfs](https://github.com/Maxxen/duckdb_tarfs) (MIT license)

[libarchive](https://github.com/libarchive/libarchive)
Copyright 2003-2018 Tim Kientzle
(varying licenses, see repo)
