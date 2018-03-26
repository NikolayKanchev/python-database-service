
class User:
    def __init__(self, username, password, address, list_tables):
        self.__username = username
        self.__password = password
        self.__address = address
        self.__list_tables = list_tables

    def get_pass(self):
        return self.__password

    def get_username(self):
        return self.__username

    def get_address(self):
        return self.__address

    def append_table(self, table):
        if table not in self.__list_tables:
            self.__list_tables.append(table)
            return True
        else:
            return False

    def get_list_tables(self):
        return self.__list_tables

    def get_table(self, table_name):
        for table in self.__list_tables:
            if table.get_name() == table_name:
                return table

    def set_address(self, address):
        self.__address = address


class Table:
    def __init__(self, table_name):
        self.__table_name = table_name
        self.__columns = [[]]
        self.__types = [int]
        self.__names = [id]
        self.__last_row_index = 0

    def get_name(self):
        return self.__table_name

    def get_column_index_by_name(self, name):
        index = 0
        for n in self.__names:
            if n == name:
                break
            index += 1

        if index > self.__names.__len__():
            return -99999

        return index

    def add_column(self, name, column_type):
        self.__names.append(name)
        self.__types.append(column_type)
        self.__columns.append([])

    def delete_column(self, name):
        index = self.get_column_index_by_name(name)
        self.__names.pop(index)
        self.__types.pop(index)
        self.__columns.pop(index)

    def get_list_columns(self):
        return self.__names

    def get_column_type(self, name):
        index = self.get_column_index_by_name(name)
        return self.__types[index]

    def insert_row(self,  columns_names, data):
        list_names = []
        list_data = []

        if columns_names.startswith("(") and columns_names.endswith(")"):
            str_names = columns_names[1:-1]
            list_names = str_names.split(",")
        else:
            return False, "Error - missing '( )'"

        if data.startswith("(") and data.endswith(")"):
            str_data = data[1:-1]
            list_data = str_data.split(",")
        else:
            return False, "Error - missing '( )'"

        if list_names.__len__() == 0 or list_data.__len__() == 0:
            return False, "Error - empty Arguments !!!"

        if list_names.__len__() != list_data.__len__():
            return False, "Error - there should be data for each column name !!!"

        for i, name in enumerate(list_names):
            column_index = self.get_column_index_by_name(name)
            column = self.__columns[column_index]
            column_type = self.__types[column_index]
            data = list_data[i]
            data_type = type(list_data[i])

            if column_index == -99999:
                return False, "Error - the column '" + name + "' doesn't match any of the table columns !!!"

            if data_type == column_type:
                column.append(data)
                self.__last_row_index += 1
            else:
                return False, "Error - the type of the data doesn't match the required column type !!!"

    def delete_row(self, row_id):
        if row_id > self.__columns.__len__():
            return "Error - invalid row number"

        for column in self.__columns:
            column.pop(row_id)












