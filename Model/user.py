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
