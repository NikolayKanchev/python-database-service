class Row:

    def __init__(self, index):
        self.__index = index
        self.__elements = []

    def get_element(self, column_name):

        for element in self.__elements:

            if element.get_column_name() == column_name:

                return element

        return None

    def edit_element(self, column_name, new_data):

        for element in self.__elements:

            if element.get_column_name() == column_name:

                element.set_data(new_data)

                return True

        return False

    def get_index(self):

        return self.__index

    def get_elements(self):

        return self.__elements

    def insert(self, element):

        self.__elements.append(element)
