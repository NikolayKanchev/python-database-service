
class Column:

    def __init__(self, name, column_type, is_primary, is_foreign):
        self.__name = name
        self.__column_type = column_type
        self.__is_primary = is_primary
        self.__is_foreign = is_foreign

    def set_name(self, column_name):
        self.__name = column_name

    def set_type(self, column_type):
        self.__column_type = column_type

    def set_is_primary(self, is_primary):
        self.__is_primary = is_primary

    def set_is_foreign(self, is_foreign):
        self.__is_foreign = is_foreign

    def get_name(self):
        return self.__name

    def get_type(self):
        return self.__column_type

    def get_is_primary(self):
        return self.__is_primary

    def get_is_foreign(self):
        return self.__is_foreign

