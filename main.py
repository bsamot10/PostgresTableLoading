from utils import get_connection, ConfigParser
from table_loading import TableLoader
import warnings
warnings.filterwarnings("ignore")


def main():

    # parse command line arguments
    config_parser = ConfigParser()
    args = config_parser.set_arguments()
    print(args)

    # get info from arguments
    path_file = args.path_file
    sheet_name = args.sheet_name
    table_name = args.table_name
    varchar_only = args.varchar_only
    if not varchar_only:
        exit(print("\nExit message: 'varchar_only' argument -> Only varchar datatype is currently supported.\n"))

    # create a connection to the database
    connection = get_connection()

    # initialize TableLoader and implement table loading process
    table_loader = TableLoader(connection, path_file, sheet_name, table_name, varchar_only)
    table_loader.implement_table_loading()


if __name__ == '__main__':
    main()