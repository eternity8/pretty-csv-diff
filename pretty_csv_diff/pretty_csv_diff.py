import csv
import itertools
import io

class PrettyCsvDiff:
    def __init__(self, path, pk, encoding=None, **fmtparams):
        # Initializing the object with paths to CSV files, primary keys, encoding, and format parameters.
        self._header = None
        self._maxlen = None
        self._pks = pk
        self._encoding = encoding
        self._fmtparams = {k: v for k, v in fmtparams.items() if v is not None}

        # Reading the data from the two provided file paths.
        self._data_a = self._read(path[0])
        self._data_b = self._read(path[1])

    def _read(self, path):
        # Private method to read a CSV file and organize its data.
        with open(path, 'r', encoding=self._encoding, newline='') as fp:
            # Detecting the CSV dialect if not provided.
            if 'dialect' not in self._fmtparams:
                sample = ''.join(next(fp) for _ in range(3))
                self._fmtparams['dialect'] = csv.Sniffer().sniff(sample)

                # Using itertools.chain to handle pipe inputs, as seek is not possible.
                fp = itertools.chain(io.StringIO(sample), fp)

            reader = csv.reader(fp, **self._fmtparams)

            # Reading the header and initializing column widths and primary keys.
            if self._header is None:
                self._header = next(reader)
                self._maxlen = list(map(len, self._header))
                self._pks = [int(pk) if pk.isdecimal() else self._header.index(pk) for pk in self._pks]
            else:
                next(reader)

            data = []
            for row in reader:
                # Storing each row and updating column widths.
                data.append(row)
                self._maxlen = [max(pair) for pair in zip(map(len, row), self._maxlen)]

        # Sorting the data based on the primary key.
        data.sort(key=self._get_pk)
        return data

    def _get_pk(self, row):
        # Extracting primary key values from a row.
        return [int(row[k]) if row[k].isdecimal() else row[k] for k in self._pks]

    def _formatted(self, prefix, row, diff=None):
        # Formatting a row for output with optional coloring for differences.
        BOLD = '\x1b[1m'
        RED = '\x1b[41m'
        GREEN = '\x1b[42m'
        RESET = '\x1b[0m'

        def colorize(k):
            # Applying color based on differences and primary key highlighting.
            sgr = ''
            if prefix in ('<', '>') and (not diff or diff[k]):
                sgr += RED if prefix == '<' else GREEN
            if k in self._pks:
                sgr += BOLD
            padding = ' ' * (self._maxlen[k] - len(row[k]))
            return sgr + row[k] + padding + (RESET if sgr else '')

        return (prefix,) + tuple(colorize(k) for k in range(len(row)))


    def do(self):
        # The main method to compare and yield differences between the two CSV files.
        yield self._formatted(' ', self._header)

        i = 0
        j = 0
        previous = None

        while i < len(self._data_a) or j < len(self._data_b):
            # Handling row comparisons and determining whether to advance in either file.
            pk_a = self._get_pk(self._data_a[i]) if i < len(self._data_a) else [AlwaysGreater()]
            pk_b = self._get_pk(self._data_b[j]) if j < len(self._data_b) else [AlwaysGreater()]

            next_a = pk_a < pk_b
            next_b = pk_a > pk_b
            next_ab = pk_a == pk_b

            # Determining the differences between rows.
            diff = [a != b for a, b in zip(self._data_a[i], self._data_b[j])] if next_ab else None
            diff_ab = diff and any(diff)

            current = (next_a, next_b)
            if (next_a or next_b) and previous != current or diff_ab:
                # Yielding a separator row when switching between files or encountering differences.
                yield self._formatted(' ', ['-' * n for n in self._maxlen])
                previous = current

            # Outputting the differences.
            if next_a or next_ab:
                if next_a or diff_ab:
                    yield self._formatted('<', self._data_a[i], diff)
                i += 1

            if next_b or next_ab:
                if next_b or diff_ab:
                    yield self._formatted('>', self._data_b[j], diff)
                j += 1


class AlwaysGreater:
    # Utility class to represent a value always greater than any non-similar object.
    def __lt__(self, other):
        return False

    def __gt__(self, other):
        if isinstance(other, AlwaysGreater):
            return False
        return True

    def __eq__(self, other):
        return False
