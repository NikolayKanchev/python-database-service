class Table:
    def __init__(self, table_name, row_index=0):
        self.__table_name = table_name
        self.__row_index = row_index
        self.__columns = []
        self.__rows = []
        self.__allowed_column_types = ["int", "float", "str", "bool"]

    def get_name(self):

        return self.__table_name

    def set_name(self, new_name):

        self.__table_name = new_name

    def add_column(self, column):

        self.__columns.append(column)

    def delete_column(self, column):

        self.__columns.remove(column)

    def get_columns_names(self):

        str_columns_names = ""

        for c in self.__columns:

            str_columns_names += c.get_name() + " "

        return str_columns_names

    def get_column(self, column_name):

        for column in self.__columns:

            if column.get_name() == column_name:

                return column

        return None

    def get_columns(self):

        return self.__columns

    def get_allowed_column_types(self):

        return self.__allowed_column_types

    def insert_row(self, row):

        self.__rows.append(row)

        self.__row_index += 1

    def get_row_index(self):

        return self.__row_index

    def get_rows(self):

        return self.__rows

    def delete_all_rows(self):

        self.__rows = []

    def delete_row(self, row):

        self.__rows.remove(row)


