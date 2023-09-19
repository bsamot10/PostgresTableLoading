from table_schema import SchemaLoader
from psycopg2 import sql
import warnings
warnings.filterwarnings("ignore")


class TableLoader(SchemaLoader):

    def __init__(self, connection, path_file, sheet_name, table_name, varchar_only):
        super().__init__(connection, path_file, sheet_name, table_name, varchar_only)
        self.connection = connection
        self.table_name = table_name

    def implement_table_loading(self):
        sql_insert_table = self.get_sql_insert_query()
        self.load_table(sql_insert_table)

    def get_sql_insert_query(self):
        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {self.table_name} ()")
        columns = self.set_columns().keys()
        self.add_columns()
        sql_insert = sql.SQL("INSERT INTO {} ({}) VALUES ({})") \
                        .format(sql.SQL(self.table_name),
                                sql.SQL(', ').join(map(sql.Identifier, columns)),
                                sql.SQL(', ').join(sql.Placeholder() * len(columns)))

        return sql_insert

    def load_table(self, sql_insert):
        number_of_rows = len(self.df)
        for i in range(number_of_rows):
            data_insert = [str(value) for value in self.df.iloc[i]]
            self.cursor.execute(sql_insert.as_string(self.connection), (data_insert))
            self.connection.commit()


