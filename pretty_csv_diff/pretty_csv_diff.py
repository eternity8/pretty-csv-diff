import csv
import itertools
import io
from itertools import zip_longest
from functools import partial


class PrettyCsvDiff:
    def __init__(self, path, pk, encoding=None, **fmtparams):
        # The constructor initializes the object with file paths, primary keys, encoding, and CSV format parameters.

        self._pks = pk  # List of primary keys (columns used to uniquely identify rows).
        self._encoding = encoding  # Encoding of the CSV files.
        self._fmtparams = {
            k: v for k, v in fmtparams.items() if v is not None
        }  # CSV format parameters.

        # Reading data from the two CSV files specified in the path.
        self._data_a, self._meta_a = self._read(path[0])
        self._data_b, self._meta_b = self._read(path[1])
        self._maxnumcols = max(self._meta_a["_numcols"], self._meta_b["_numcols"])
        self._minnumcols = min(self._meta_a["_numcols"], self._meta_b["_numcols"])
        self._maxlen = [
            max(pair)
            for pair in zip_longest(
                self._meta_a["_maxlen"], self._meta_b["_maxlen"], fillvalue=0
            )
        ]

    def _get_pk(self, row, meta):
        # Extracts the primary key values from a given row.
        return [row[k] for k in meta["_pks"]]

    def _read(self, path):
        meta = {}
        # Reads a CSV file and organizes its data.
        with open(path, "r", encoding=self._encoding, newline="") as fp:
            # Automatically detect the CSV dialect if not provided in the format parameters.
            if "dialect" not in self._fmtparams:
                sample = "".join(next(fp) for _ in range(3))
                self._fmtparams["dialect"] = csv.Sniffer().sniff(sample)
                fp = itertools.chain(io.StringIO(sample), fp)

            reader = csv.reader(fp, **self._fmtparams)

            # Reading the header and setting up column widths and primary key indices.
            header_line = next(reader)

            meta["_header"] = header_line
            meta["_maxlen"] = list(map(len, header_line))
            meta["_numcols"] = len(header_line)
            meta["_pks"] = [header_line.index(pk) for pk in self._pks]

            data = []
            for row in reader:
                data.append(row)
                meta["_maxlen"] = [
                    max(pair) for pair in zip(map(len, row), meta["_maxlen"])
                ]
            get_pk = partial(self._get_pk, meta=meta)
            data.sort(key=get_pk)
            return data, meta

    def _formatted(self, prefix, row, diff=None):
        # Formats a row for output, applying color based on differences and primary key highlighting.
        # Note: Color codes are used for terminal output.

        BOLD = "\x1b[1m"
        RED = "\x1b[41m"
        GREEN = "\x1b[42m"
        RESET = "\x1b[0m"

        def colorize(k):
            # Colorizes and pads the elements of a row for display.
            sgr = ""
            if prefix in ("<", ">") and (not diff or diff[k]):
                sgr += RED if prefix == "<" else GREEN
            if k in self._pks:
                sgr += BOLD
            padding = " " * (self._maxlen[k] - len(row[k]))
            return sgr + row[k] + padding + (RESET if sgr else "")

        return (prefix,) + tuple(colorize(k) for k in range(len(row)))

    def do(self):
        # Main method to compare and output differences between two CSV files.

        i = 0
        j = 0
        previous = None

        all_rows_a = [self._meta_a["_header"]] + self._data_a
        all_rows_b = [self._meta_b["_header"]] + self._data_b

        # Ensure header is always printed
        if self._meta_a["_header"] == self._meta_b["_header"]:
            yield self._formatted(" ", self._meta_b["_header"])

        while i < len(all_rows_a) or j < len(all_rows_b):
            row_a = all_rows_a[i]
            row_b = all_rows_b[j]
            # Iterating through both datasets to compare rows.
            pk_a = (
                self._get_pk(row_a, meta=self._meta_a)
                if i < len(all_rows_a)
                else [AlwaysGreater()]
            )
            pk_b = (
                self._get_pk(row_b, meta=self._meta_b)
                if j < len(all_rows_b)
                else [AlwaysGreater()]
            )

            next_a = pk_a < pk_b
            next_b = pk_a > pk_b
            next_ab = pk_a == pk_b

            if next_ab:
                diff = [a != b for a, b in zip_longest(row_a, row_b)]
            else:
                diff = None

            diff_ab = diff and any(diff)

            current = (next_a, next_b)
            if (next_a or next_b) and previous != current or diff_ab:
                yield self._formatted(" ", ["-" * n for n in self._maxlen])
                previous = current

            if next_a or next_ab:
                if next_a or diff_ab:
                    yield self._formatted("<", row_a, diff)
                i += 1

            if next_b or next_ab:
                if next_b or diff_ab:
                    yield self._formatted(">", row_b, diff)
                j += 1


class AlwaysGreater:
    # Represents a value that is always greater than any other non-similar object.
    def __lt__(self, other):
        return False

    def __gt__(self, other):
        return not isinstance(other, AlwaysGreater)

    def __eq__(self, other):
        return False
