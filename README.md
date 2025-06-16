[![Extension Test](https://github.com/isaacbrodsky/duckdb-zipfs/actions/workflows/MainDistributionPipeline.yml/badge.svg)](https://github.com/isaacbrodsky/duckdb-zipfs/actions/workflows/MainDistributionPipeline.yml)
[![DuckDB Version](https://img.shields.io/static/v1?label=duckdb&message=v1.3.1&color=blue)](https://github.com/duckdb/duckdb/releases/tag/v1.3.1)
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

## File names

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

Using `zipfs_split` also means you can read other archives supported by libarchive:
```SQL
SET zipfs_split = "!!";

SELECT * FROM 'zip://examples/a.tar.gz!!b.csv'
```

## Performance considerations

This extension is intended more for convience than high performance. It does not implement a file metadata cache as `tarfs` (on which this
extension is based) does. As such, operations which require the central directory (index) of the zip file, such as globbing files, must
reread the central directory multiple times, once for the glob and once for each file to open.

# Development

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

# License

duckdb-zipfs Copyright 2025 Isaac Brodsky. Licensed under the [MIT License](./LICENSE).

[DuckDB](https://github.com/duckdb/duckdb) Copyright 2018-2022 Stichting DuckDB Foundation (MIT License)

[libarchive](https://github.com/libarchive/libarchive)
Copyright 2003-2018 Tim Kientzle
(varying licenses, see repo)

[DuckDB extension-template](https://github.com/duckdb/extension-template) Copyright 2018-2022 DuckDB Labs BV (MIT License)

[duckdb_tarfs](https://github.com/Maxxen/duckdb_tarfs) (MIT license)

Thanks to [miniz](https://github.com/richgel999/miniz)
Copyright 2013-2014 RAD Game Tools and Valve Software
Copyright 2010-2014 Rich Geldreich and Tenacious Software LLC
(MIT License), which is no longer used in this extension.
