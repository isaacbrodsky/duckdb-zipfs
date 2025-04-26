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
