# duckdb-zipfs

[![Extension Test](https://github.com/isaacbrodsky/duckdb-zipfs/actions/workflows/MainDistributionPipeline.yml/badge.svg)](https://github.com/isaacbrodsky/duckdb-zipfs/actions/workflows/MainDistributionPipeline.yml)
[![DuckDB Version](https://img.shields.io/static/v1?label=duckdb&message=v1.4.2&color=blue)](https://github.com/duckdb/duckdb/releases/tag/v1.4.2)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

This is a [DuckDB](https://duckdb.org) extension that adds support for reading files from within [zip archives](https://en.wikipedia.org/wiki/ZIP_(file_format)) and [bzip2-compressed files](https://en.wikipedia.org/wiki/Bzip2).

## Get started

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

### File names

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

SELECT * FROM 'zip://examples/a.zip!!b.csv'
```

## Bzip2 files

To read a bzip2-compressed file:

```SQL
SELECT * FROM read_csv('bz2://path/to/file.csv.bz2');
SELECT * FROM read_json('bz2://path/to/file.jsonl.bz2');
```

The `bzip2://` prefix also works:

```SQL
SELECT * FROM read_csv('bzip2://path/to/file.csv.bz2');
```

Globbing is supported:

```SQL
SELECT * FROM read_csv('bz2://data/*.csv.bz2');
```

Concatenated bzip2 streams (common in large compressed files) are handled automatically.

Note: bzip2 decompression is single-threaded (libbz2 limitation). For large files, consider using xz format instead which supports multi-threaded decompression.

### Performance considerations

This extension is intended more for convience than high performance. It does not implement a file metadata cache as `tarfs` (on which this
extension is based) does. As such, operations which require the central directory (index) of the zip file, such as globbing files, must
reread the central directory multiple times, once for the glob and once for each file to open.

The selected file will be read entirely into memory, not streamed. Therefore it cannot be used to read files which are larger than memory when uncompressed.

## Development

First, install vcpkg to `vcpkg`:

```sh
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

Then:

```sh
make -j 4 release
make test_release
```

## License

duckdb-zipfs Copyright 2025 Isaac Brodsky. Licensed under the [MIT License](./LICENSE).

[DuckDB](https://github.com/duckdb/duckdb) Copyright 2018-2022 Stichting DuckDB Foundation (MIT License)

[miniz](https://github.com/richgel999/miniz)
Copyright 2013-2014 RAD Game Tools and Valve Software
Copyright 2010-2014 Rich Geldreich and Tenacious Software LLC
(MIT License)

[DuckDB extension-template](https://github.com/duckdb/extension-template) Copyright 2018-2022 DuckDB Labs BV (MIT License)

[duckdb_tarfs](https://github.com/Maxxen/duckdb_tarfs) (MIT license)

[bzip2](https://sourceware.org/bzip2/) Copyright 1996-2019 Julian Seward (BSD-style license)
