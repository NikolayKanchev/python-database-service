
class User:
    def __init__(self, username, password, address, list_databases):
        self.__username = username
        self.__password = password
        self.__address = address
        self.__list_databases = list_databases

    def get_pass(self):

        return self.__password

    def get_username(self):

        return self.__username

    def set_address(self, address):

        self.__address = address

    def get_address(self):

        return self.__address

    def create_database(self, database):

        if database not in self.__list_databases:

            self.__list_databases.append(database)

            return True

        else:
            return False

    def get_list_databases(self):

        return self.__list_databases

    def get_database(self, database_name):

        for database in self.__list_databases:

            if database.get_name() == database_name:

                return database

        return None

    def drop_database(self, database):

        if database in self.__list_databases:

            self.__list_databases.remove(database)

            return True

        else:
            return False

