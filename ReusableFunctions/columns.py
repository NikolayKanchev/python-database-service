from pythonDatabase.ReusableFunctions.send_receive import *


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

    table.add_column(column_name, column_type)


def get_list_columns(addr, table_name, list_users):

    user = get_user(addr, list_users)

    table = user.get_table(table_name)

    columns_names = table.get_columns_names()

    str_columns = " Columns in " + table_name + " :\n"

    for column_name in columns_names:

        column_type = table.get_column_type(column_name)

        str_columns += str(column_type) + "\t" + str(column_name) + "\n"

    return str_columns, columns_names

