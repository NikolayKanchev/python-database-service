class Database:
    def __init__(self, database_name, list_tables):
        self.__database_name = database_name
        self.__list_tables = list_tables

    def get_name(self):

        return self.__database_name

    def rename(self, new_db_name):

        self.__database_name = new_db_name

    def append_table(self, table):

        if table not in self.__list_tables:

            self.__list_tables.append(table)

            return True

        else:
            return False

    def drop_table(self, table):

        if table in self.__list_tables:

            self.__list_tables.remove(table)

    def get_list_tables(self):

        return self.__list_tables

    def get_table(self, table_name):

        for table in self.__list_tables:

            if table.get_name() == table_name:

                return table

        return None



