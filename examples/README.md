This directory contains example zip files to use with testing the extension.

## Contents listing

```
$ unzip -l a.zip
Archive:  a.zip
  Length      Date    Time    Name
---------  ---------- -----   ----
        0  03-29-2025 00:30   nested_dir/
       26  03-29-2025 00:35   nested_dir/some_file.jsonl
       12  01-17-2025 18:45   nested_dir/some_file.csv
       24  01-17-2025 15:22   a.csv
       26  03-29-2025 00:35   a.jsonl
       11  01-17-2025 18:45   b.csv
       26  03-29-2025 00:35   b.jsonl
---------                     -------
      125                     7 files
```

```
$ tar tvvzf a.tar.gz
drwxr-xr-x  0 isaac  staff       0 Mar 29 00:30 nested_dir/
-rw-r--r--  0 isaac  staff      26 Mar 29 00:35 nested_dir/some_file.jsonl
-rw-r--r--  0 isaac  staff      12 Jan 17 18:45 nested_dir/some_file.csv
-rw-r--r--  0 isaac  staff      24 Jan 17 15:22 a.csv
-rw-r--r--  0 isaac  staff      26 Mar 29 00:35 a.jsonl
-rw-r--r--  0 isaac  staff      11 Jan 17 18:45 b.csv
-rw-r--r--  0 isaac  staff      26 Mar 29 00:35 b.jsonl
Archive Format: POSIX ustar format,  Compression: gzip
```

```
$ unzip -l b.zip
Archive:  b.zip
  Length      Date    Time    Name
---------  ---------- -----   ----
        0  03-29-2025 00:30   nested_dir/
       26  03-29-2025 00:35   nested_dir/some_file.jsonl
       12  01-17-2025 18:45   nested_dir/some_file.csv
       24  01-17-2025 15:22   a.csv
       26  03-29-2025 00:35   a.jsonl
       11  01-17-2025 18:45   b.csv
       26  03-29-2025 00:35   b.jsonl
---------                     -------
      125                     7 files
```

```
$ unzip -l csv_only.zip
Archive:  csv_only.zip
  Length      Date    Time    Name
---------  ---------- -----   ----
        0  03-29-2025 00:30   nested_dir/
       12  01-17-2025 18:45   nested_dir/some_file.csv
       24  01-17-2025 15:22   a.csv
       11  01-17-2025 18:45   b.csv
---------                     -------
       47                     4 files
```

```
$ unzip -l empty.zip
Archive:  empty.zip
warning [empty.zip]:  zipfile is empty
```

```
$ unzip -l csv_gz.zip
Archive:  csv_gz.zip
  Length      Date    Time    Name
---------  ---------- -----   ----
       50  01-17-2025 15:22   a.csv.gz
       37  01-17-2025 18:45   b.csv.gz
---------                     -------
       87                     2 files
```

`examples/big/big.zip` was generated with:
```py
import zipfile
with zipfile.ZipFile('big.zip', 'w') as zf:
    for i in range(2500):
        zf.writestr(f'big_dir/{i}.txt', b'hello')
```

Password is `password`
```
$ unzip -l passworded.zip
Archive:  passworded.zip
  Length      Date    Time    Name
---------  ---------- -----   ----
       14  09-19-2026 20:20   secret_file.csv
---------                     -------
       14                     1 file
```

```
% 7zz l encrypted_header.7z -psecret123

7-Zip (z) 26.03 (arm64) : Copyright (c) 1999-2026 Igor Pavlov : 2026-09-03
 64-bit arm_v:8.5-A locale=en_US.UTF-8 Threads:10 OPEN_MAX:4096, ASM

Scanning the drive for archives:
1 file, 240 bytes (1 KiB)

Listing archive: encrypted_header.7z

--
Path = encrypted_header.7z
Type = 7z
Physical Size = 240
Headers Size = 224
Method = LZMA2:12 7zAES
Solid = -
Blocks = 1

   Date      Time    Attr         Size   Compressed  Name
------------------- ----- ------------ ------------  ------------------------
2026-09-20 14:35:33 ....A           12           16  file_name_secret.csv
------------------- ----- ------------ ------------  ------------------------
2026-09-20 14:35:33                 12           16  1 files
```
