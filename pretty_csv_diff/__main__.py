# Importing the argparse module to parse command-line arguments.
import argparse

# Importing the PrettyCsvDiff class from the pretty_csv_diff module.
from .pretty_csv_diff import PrettyCsvDiff

def main():
    # Creating a parser object to handle command-line arguments.
    parser = argparse.ArgumentParser(epilog='https://github.com/telnet23/pretty-csv-diff')
    # Adding an argument for the paths to the two CSV files that will be compared.
    parser.add_argument('path', nargs=2, help='paths to the two csv files to be compared')
    # Adding an argument for the primary key column(s), allowing multiple columns.
    parser.add_argument('pk', nargs='+', help='name or index of primary key column. multiple columns are allowed')
    # Optional argument to specify CSV encoding.
    parser.add_argument('--encoding', help='override csv encoding. determined from locale by default')
    # Optional argument to specify CSV delimiter.
    parser.add_argument('--delimiter', help='override csv delimiter. determined heuristically by default')
    # Parsing the arguments provided on the command line.
    args = vars(parser.parse_args())

    # Creating an instance of PrettyCsvDiff with the parsed arguments and executing its comparison functionality.
    for formatted_row in PrettyCsvDiff(**args).do():
        # Printing each formatted row of the comparison result.
        print('  '.join(formatted_row))

# This condition ensures that the main function is executed only when the script is run directly.
if __name__ == '__main__':
    main()
