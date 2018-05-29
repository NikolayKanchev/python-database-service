
class RowElement:

    def __init__(self, column_name, data):
        self.__column_name = column_name
        self.__data = data

    def get_column_name(self):

        return self.__column_name

    def set_data(self, data):

        self.__data = data

    def get_data(self):

        return self.__data
