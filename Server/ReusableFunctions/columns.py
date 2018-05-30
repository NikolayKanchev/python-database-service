from pythonDatabase.Model.column import Column
from pythonDatabase.Server.ReusableFunctions.send_receive import *


def add_column(table, con):

    column_name = send_receive(" Enter column name: ", con)

    user_choice = send_receive(" Choose a column type: \n"
                               "1) String \n"
                               "2) Integer \n"
                               "3) Float \n"
                               "4) Boolean \n", con)

    while True:

        if user_choice == '1':

            column_type = 'str'

            break

        if user_choice == '2':

            column_type = 'int'

            break

        if user_choice == '3':

            column_type = 'float'

            break

        if user_choice == '4':

            column_type = 'bool'

            break

        user_choice = send_receive(" Wrong input, try again with number from 1 to 4", con)

    new_column = Column(column_name, False, False, column_type)

    table.add_column(new_column)


def get_list_columns(con, table):

    columns = table.get_columns()

    arr_table = [["COLUMNS"]]

    for c in columns:
        arr_table.append([c.get_name()])

    send(con, f"{arr_table}")

    return str(arr_table[1:])


