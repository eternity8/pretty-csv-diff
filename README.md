# Pretty CSV Diff

`pretty-csv-diff` is a Python utility that compares two CSV files for differences. It highlights these differences by colorizing them and aligning the columns for easy visual comparison. This tool is particularly useful for data analysts and developers who work with large datasets and need to quickly identify changes between two versions of a CSV file.

## Features

- Color-coded output for easy spotting of differences.
- Column alignment for improved readability.
- Command-line interface for easy integration with other tools or scripts.
- Supports specifying multiple primary keys for comparison.

## Installation

To install `pretty-csv-diff`, clone the repository and run the setup script:

```bash
git clone https://github.com/eternity8/pretty-csv-diff.git
cd pretty-csv-diff
pip install .
```

## Usage

The basic command-line syntax for `pretty-csv-diff` is:

```bash
pretty-csv-diff [options] path1 path2 [primary_key ...]
```

- `path1` and `path2` are the file paths to the two CSV files you want to compare.
- `primary_key` is either the name or the index of the primary key column(s) in the CSV files. You can specify multiple columns for composite keys.

### Example

To compare two CSV files `old.csv` and `new.csv` using the column `id` as the primary key:

```bash
pretty-csv-diff old.csv new.csv id
```

This will output the differences between the two files in your terminal, with `<` indicating rows that are only in `old.csv` and `>` indicating rows that are only in `new.csv`. Rows present in both files but with differences will be shown side by side for comparison.

## Command-Line Example Screenshot

![](screenshot.png)

## Command-Line Usage

```
usage: pretty-csv-diff [-h] path path pk [pk ...]

positional arguments:
  path        paths to the two csv files to be compared
  pk          name or index of primary key column. multiple columns are
              allowed

optional arguments:
  -h, --help  show this help message and exit
```

## Contributing

Contributions to `pretty-csv-diff` are welcome.

## License

`pretty-csv-diff` is released under the Apache License, Version 2.0 (January 2004). See the [LICENSE](<[https://link-to-your-license](https://www.apache.org/licenses/LICENSE-2.0)https://www.apache.org/licenses/LICENSE-2.0>) file for more details.
