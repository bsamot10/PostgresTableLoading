import pandas as pd
import warnings
warnings.filterwarnings("ignore")


class SchemaLoader:

    def __init__(self, connection, path_file, sheet_name, table_name, varchar_only):
        self.connection = connection
        self.cursor = connection.cursor()
        self.path_file = path_file
        self.sheet_name = sheet_name
        self.table_name = table_name
        self.varchar_only = varchar_only

        self.df = self.read_file()
        self.escape_strings = [' ', '.', '/', "group", "Group", "GROUP"]

    def read_file(self):
        if self.path_file.endswith("csv"):
            return pd.read_csv(fr"{self.path_file}")
        elif self.path_file.endswith("xlsx"):
            if self.sheet_name:
                return pd.read_excel(fr"{self.path_file}", dtype=str, sheet_name=self.sheet_name)
            else:
                return pd.read_excel(fr"{self.path_file}", dtype=str)
        else:
            exit(print("\nExit message: file type -> Only 'csv' and 'xlsx' files can be read.\n"))

    def set_columns(self):
        columns = {}
        for col_name in self.df.columns:
            if any([col_name.strip().__contains__(esc_str) for esc_str in self.escape_strings]):
                columns[col_name.strip()] = "varchar"
            else:
                columns[col_name.strip().lower()] = "varchar"
        return columns

    def add_columns(self):
        columns = self.set_columns()
        for col_name, data_type in columns.items():
            if any([col_name.__contains__(esc_str) for esc_str in self.escape_strings]):
                self.cursor.execute(f'ALTER TABLE {self.table_name} add COLUMN IF NOT EXISTS "{col_name}" {data_type}')
            else:
                self.cursor.execute(f'ALTER TABLE {self.table_name} add COLUMN IF NOT EXISTS {col_name} {data_type}')
        self.connection.commit()
