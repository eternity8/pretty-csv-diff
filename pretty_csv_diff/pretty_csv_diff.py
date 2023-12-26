import csv
import itertools
import io

class PrettyCsvDiff:
    def __init__(self, path, pk, encoding=None, **fmtparams):
        # The constructor initializes the object with file paths, primary keys, encoding, and CSV format parameters.
        
        self._header = None  # Will store the CSV header.
        self._maxlen = None  # Will store the maximum length of each column for formatting.
        self._pks = pk  # List of primary keys (columns used to uniquely identify rows).
        self._encoding = encoding  # Encoding of the CSV files.
        self._fmtparams = {k: v for k, v in fmtparams.items() if v is not None}  # CSV format parameters.

        # Reading data from the two CSV files specified in the path.
        self._data_a = self._read(path[0])
        self._data_b = self._read(path[1])

    def _read(self, path):
        # Reads a CSV file and organizes its data.
        with open(path, 'r', encoding=self._encoding, newline='') as fp:
            # Automatically detect the CSV dialect if not provided in the format parameters.
            if 'dialect' not in self._fmtparams:
                sample = ''.join(next(fp) for _ in range(3))
                self._fmtparams['dialect'] = csv.Sniffer().sniff(sample)
                fp = itertools.chain(io.StringIO(sample), fp)

            reader = csv.reader(fp, **self._fmtparams)

            # Reading the header and setting up column widths and primary key indices.
            if self._header is None:
                self._header = next(reader)
                self._maxlen = list(map(len, self._header))
                self._pks = [int(pk) if pk.isdecimal() else self._header.index(pk) for pk in self._pks]
            else:
                next(reader)

            data = []
            for row in reader:
                data.append(row)
                self._maxlen = [max(pair) for pair in zip(map(len, row), self._maxlen)]

            # Sorting the data based on the primary key.
            data.sort(key=self._get_pk)
            return data

    def _get_pk(self, row):
        # Extracts the primary key values from a given row.
        return [int(row[k]) if row[k].isdecimal() else row[k] for k in self._pks]

    def _formatted(self, prefix, row, diff=None):
        # Formats a row for output, applying color based on differences and primary key highlighting.
        # Note: Color codes are used for terminal output.

        BOLD = '\x1b[1m'
        RED = '\x1b[41m'
        GREEN = '\x1b[42m'
        RESET = '\x1b[0m'

        def colorize(k):
            # Colorizes and pads the elements of a row for display.
            sgr = ''
            if prefix in ('<', '>') and (not diff or diff[k]):
                sgr += RED if prefix == '<' else GREEN
            if k in self._pks:
                sgr += BOLD
            padding = ' ' * (self._maxlen[k] - len(row[k]))
            return sgr + row[k] + padding + (RESET if sgr else '')

        return (prefix,) + tuple(colorize(k) for k in range(len(row)))

    def do(self):
        # Main method to compare and output differences between two CSV files.
        yield self._formatted(' ', self._header)

        i = 0
        j = 0
        previous = None

        while i < len(self._data_a) or j < len(self._data_b):
            # Iterating through both datasets to compare rows.
            pk_a = self._get_pk(self._data_a[i]) if i < len(self._data_a) else [AlwaysGreater()]
            pk_b = self._get_pk(self._data_b[j]) if j < len(self._data_b) else [AlwaysGreater()]

            next_a = pk_a < pk_b
            next_b = pk_a > pk_b
            next_ab = pk_a == pk_b

            diff = [a != b for a, b in zip(self._data_a[i], self._data_b[j])] if next_ab else None
            diff_ab = diff and any(diff)

            current = (next_a, next_b)
            if (next_a or next_b) and previous != current or diff_ab:
                yield self._formatted(' ', ['-' * n for n in self._maxlen])
                previous = current

            if next_a or next_ab:
                if next_a or diff_ab:
                    yield self._formatted('<', self._data_a[i], diff)
                i += 1

            if next_b or next_ab:
                if next_b or diff_ab:
                    yield self._formatted('>', self._data_b[j], diff)
                j += 1

class AlwaysGreater:
    # Represents a value that is always greater than any other non-similar object.
    def __lt__(self, other):
        return False

    def __gt__(self, other):
        return not isinstance(other, AlwaysGreater)

    def __eq__(self, other):
        return False
