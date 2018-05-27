from pythonDatabase.Model.column import Column
from pythonDatabase.Model.database import Database
from pythonDatabase.Model.row import Row
from pythonDatabase.Model.row_element import RowElement
from pythonDatabase.Model.table import Table
from pythonDatabase.ReusableFunctions.send_receive import *


def on_sql_create_database(sql_elements, addr, con, list_users):

    # It checks whether there are enough arguments or too many in the SQL
    if len(sql_elements) == 3:

        database_name = sql_elements[2]

        user = get_user(addr, list_users)

        database = user.get_database(database_name)

        # It checks whether the database exist or not
        if database is not None:

            send(con, f"A database with name: '{database_name}' exist!")

            return

        new_database = Database(database_name, [])

        user.create_database(new_database)

        save_list_users(list_users)

        send(con, f"Database: '{database_name}' was created successfully !")

    else:
        send(con, "ERROR - You are missing database name or you have to many values for the name !")


def on_sql_create_table(sql_elements, addr, con, list_users):

    user = get_user(addr, list_users)

    # It checks whether there are enough arguments or too many in the SQL
    if len(sql_elements) >= 3:

        try:
            database_name, table_name = sql_elements[2].split(".")

        except ValueError as e:

            send(con, "You are missing '.'")

        database = user.get_database(database_name)

        if database is not None:

            table = database.get_table(table_name)

            if table is None:

                if len(sql_elements) > 3:

                    if sql_elements[3][0] == "(" and sql_elements[-1][-1] == ")":

                        columns_elements = []

                        for element in sql_elements[3:]:

                            element_list = list(element)

                            for e in element_list:

                                if e == "(" or e == ")" or e == " " or e == ",":

                                    element_list.remove(e)

                            new_element = "".join(element_list)

                            columns_elements.append(new_element)

                        columns_names = []
                        columns_types = []

                        # it creates a table
                        new_table = Table(table_name)

                        # It checks whether the item is a column name or type and adds them in different arrays
                        for i, item in enumerate(columns_elements):

                            if i % 2 == 0:
                                columns_names.append(item)
                            else:
                                columns_types.append(item)

                        # It checks whether there is a column type for each column name
                        if len(columns_names) != len(columns_types):

                            send(con, f"ERROR - Not enough arguments for columns !")

                            return

                        # It checks whether a column type is allowed or not
                        for i, column_type in enumerate(columns_types):

                            if column_type not in new_table.get_allowed_column_types():

                                send(con, f"Error - Incorrect column column_type '{column_type}' ")

                                return

                            else:
                                # It adds the columns to the table

                                new_column = Column(columns_names[i], columns_types[i], False, False)

                                new_table.add_column(new_column)

                                # new_table.add_column(columns_names[i], columns_types[i])

                        database.append_table(new_table)

                        save_list_users(list_users)

                        send(con, f"A table with name: '{table_name}' and columns: {sql_elements[3:]}"
                                  f"was successfully created in database: '{database_name}' !")

                        return

                    else:
                        send(con, "ERROR, Wrong syntax. You are missing '(' or ')'")

                # It creates a table only with name
                else:

                    table = Table(table_name)

                    database.append_table(table)

                    save_list_users(list_users)

                    send(con, f"A table with name: '{table_name}' "
                              f"was successfully created in database: '{database_name}' !")

                    return

            else:

                send(con, f"A table with name: '{table_name}' already exist !")

                return

        else:

            send(con, f"Wrong database name: '{database_name}'")

    else:
        send(con, "ERROR - Missing table name!")


def on_sql_drop_database(sql_elements, addr, con, list_users):

    if len(sql_elements) == 3:

        user = get_user(addr, list_users)

        database_name = sql_elements[2]

        database = user.get_database(database_name)

        if database is None:

            send(con, f"ERROR - database with name: '{database_name}' doesn't exist")

        else:

            user.drop_database(database)

            send(con, f"Database '{database_name}' was deleted !")

            save_list_users(list_users)

    else:
        send(con, "Missing database name or too many values !")


def on_sql_drop_table(sql_elements, addr, con, list_users):

    user = get_user(addr, list_users)

    if len(sql_elements) == 3:

        try:
            database_name, table_name = sql_elements[2].split(".")

            database = user.get_database(database_name)

            if database is not None:

                table = database.get_table(table_name)

                if table is not None:

                    database.drop_table(table)

                    save_list_users(list_users)

                    send(con, f"A table with name: '{table_name}' "
                              f"was successfully deleted from database: '{database_name}' !")
                    return

                else:

                    send(con, f"A table with name: '{table_name}' doesn't exist !")

                    return

            else:

                send(con, f"Wrong database name: '{database_name}'")

        except ValueError as e:

            send(con, "You are missing '.'")

    else:
        send(con, "Missing table name or too many values")


def on_sql_rename_database(sql_elements, addr, con, list_users):

    if len(sql_elements) > 4:

        database_name = sql_elements[2]

        user = get_user(addr, list_users)

        database = user.get_database(database_name)

        if database is not None:

            if len(sql_elements) > 4 and sql_elements[3] == "TO":

                if len(sql_elements) == 5:

                    new_db_name = sql_elements[4]

                    database.set_name(new_db_name)

                    save_list_users(list_users)

                    send(con, f"Database: '{database_name}' was renamed successfully to '{new_db_name}'!")

                else:
                    send(con, f"ERROR - Missing new name or too many arguments for it !")

            else:
                send(con, f"ERROR - Check your syntax near'{sql_elements[3]}' !")

        else:
            send(con, f"ERROR - A database with name: '{database_name}' doesn't exist!")

    else:
        send(con, "ERROR - Missing arguments !")


def on_sql_rename_table(sql_elements, addr, con, list_users):

    if len(sql_elements) > 3:

        try:
            database_name, table_name = sql_elements[2].split(".")

            user = get_user(addr, list_users)

            database = user.get_database(database_name)

            if database is not None:

                if len(sql_elements) > 4 and sql_elements[3] == "TO":

                    if len(sql_elements) == 5:

                        try:
                            new_db_name, new_table_name = sql_elements[4].split(".")

                            if database_name == new_db_name:

                                if table_name != new_table_name:

                                    table = database.get_table(table_name)

                                    if table is not None:

                                        table.set_name(new_table_name)

                                        save_list_users(list_users)

                                        send(con, f"Table: '{database_name}.{table_name}' was renamed "
                                                  f"to {new_db_name}.{new_table_name} successfully !")

                                    else:
                                        send(con, f"A table with name: '{table_name}' doesn't exist")

                                else:
                                    send(con, "The new name is the same as the old one")

                            else:
                                send(con, "Database name is not the same. The table wasn't renamed !")

                        except ValueError as e:

                            send(con, "You are missing '.'")

                    else:
                        send(con, f"ERROR - Missing new name or too many arguments for it !")

                else:
                    send(con, f"Check your syntax near'{sql_elements[3]}' !")

            else:
                send(con, f"A database with name: '{database_name}' doesn't exist!")

        except ValueError as e:

            send(con, "You are missing '.'")

    else:
        send(con, "Missing arguments !")


def on_sql_add_column(sql_elements, addr, con, list_users):

    if len(sql_elements) == 6:

        if sql_elements[3] == "ADD":

            try:
                database_name, table_name = sql_elements[2].split(".")

                user = get_user(addr, list_users)

                database = user.get_database(database_name)

                if database is not None:

                    table = database.get_table(table_name)

                    if table is not None:

                        column_name = sql_elements[4]
                        column_type = sql_elements[5]

                        if column_type not in table.get_allowed_column_types():

                            send(con, f"ERROR - Wrong value for column type: {column_type}")

                        else:

                            column = table.get_column(column_name)

                            if column is None:

                                new_column = Column(column_name, column_type, False, False)

                                table.add_column(new_column)

                                save_list_users(list_users)

                                send(con, f"Column with name '{column_name}' "
                                          f"was added successfully to table '{table_name}'")

                            else:
                                send(con, f"A column '{column_name}' already exist !")

                    else:
                        send(con, f"A table with name: '{table_name}' doesn't exist")

                else:
                    send(con, f"A database with name: '{database_name}' doesn't exist")

            except ValueError as e:

                send(con, "You are missing '.'")

                return

        else:
            send(con, f"ERROR - Wrong syntax near '{sql_elements[3]}'")

    else:
        send(con, "ERROR - Too many or not enough arguments !")


def on_sql_drop_column(sql_elements, addr, con, list_users):

    if len(sql_elements) == 6:

        if sql_elements[3] == "DROP" and sql_elements[4] == "COLUMN":

            try:
                database_name, table_name = sql_elements[2].split(".")

                user = get_user(addr, list_users)

                database = user.get_database(database_name)

                if database is not None:

                    table = database.get_table(table_name)

                    if table is not None:

                        column_name = sql_elements[5]

                        column = table.get_column(column_name)

                        if column is not None:

                            table.delete_column(column)

                            save_list_users(list_users)

                            send(con, f"Column with name '{column_name}' "
                                      f"was successfully deleted from table '{table_name}'")

                        else:
                            send(con, f"A column '{column_name}' doesn't exist !")

                    else:
                        send(con, f"A table with name: '{table_name}' doesn't exist")

                else:
                    send(con, f"A database with name: '{database_name}' doesn't exist")

            except ValueError as e:

                send(con, "You are missing '.'")

                return

        else:
            send(con, f"ERROR - Wrong syntax near '{sql_elements[3]}' or '{sql_elements[4]}'")

    else:
        send(con, "ERROR - Too many or not enough arguments !")


def on_sql_change_column_type(sql_elements, addr, con, list_users):

    if len(sql_elements) == 7:

        if sql_elements[3] == "ALTER" and sql_elements[4] == "COLUMN":

            try:
                database_name, table_name = sql_elements[2].split(".")

                user = get_user(addr, list_users)

                database = user.get_database(database_name)

                if database is not None:

                    table = database.get_table(table_name)

                    if table is not None:

                        column_name = sql_elements[5]
                        new_column_type = sql_elements[6]

                        if new_column_type not in table.get_allowed_column_types():

                            send(con, f"ERROR - Wrong value for column type: {new_column_type}")

                        else:
                            column = table.get_column(column_name)

                            if column is not None:

                                old_column_type = column.get_type()

                                if old_column_type != new_column_type:

                                    column.set_type(new_column_type)

                                    save_list_users(list_users)

                                    send(con, f"The type of the column '{column_name}' "
                                              f"\n\twas successfully changed to '{new_column_type}'")

                                else:
                                    send(con, f"ERROR - The previous type is the same as the new type. "
                                              f"\n\t\tThey are both '{new_column_type}'")

                            else:
                                send(con, f"ERROR - A column '{column_name}' doesn't exist !")

                    else:
                        send(con, f"ERROR - A table with name: '{table_name}' doesn't exist")

                else:
                    send(con, f"ERROR - A database with name: '{database_name}' doesn't exist")

            except ValueError as e:

                send(con, "ERROR - You are missing '.'")

                return

        else:
            send(con, f"ERROR - Wrong syntax near '{sql_elements[3]}' or '{sql_elements[4]}'")

    else:
        send(con, "ERROR - Too many or not enough arguments !")


def on_sql_insert(sql_elements, addr, con, list_users):

    if len(sql_elements) > 5:

        try:
            database_name, table_name = sql_elements[2].split(".")

        except ValueError:

            send(con, "ERROR - Wrong syntax, missing '.'")

        user = get_user(addr, list_users)

        database = user.get_database(database_name)

        if database is not None:

            table = database.get_table(table_name)

            if table is not None:

                if sql_elements[3] == "VALUES":

                    values = sql_elements[4:]

                    if values[0][0] == "(" and values[-1][-1] == ")":

                        columns_values = []

                        for value in values:

                            value_list = list(value)

                            for c in value_list:

                                if c == "(" or c == ")" or c == " " or c == ",":

                                    value_list.remove(c)

                            new_value = "".join(value_list)

                            columns_values.append(new_value)

                        columns = table.get_columns()

                        if len(columns_values) == len(columns):

                            row_index = table.get_row_index()

                            new_row = Row(row_index)

                            for i, column in enumerate(columns):

                                valid_value = None

                                column_type = column.get_type()

                                column_name = column.get_name()

                                column_value = columns_values[i]

                                try:
                                    if column_type == "int":

                                        valid_value = int(column_value)

                                    if column_type == "float":

                                        valid_value = float(column_value)

                                    if column_type == "str":

                                        valid_value = str(column_value)

                                    if column_type == "bool":

                                        if column_value == 'True' or column_value == 'False':

                                            valid_value = bool(column_value)

                                            print(f"'{column_value}'")

                                except ValueError:

                                    send(con, f"ERROR - Wrong value for column '{column_name}' "
                                              f"It should be '{column_type}'. Got '{type(column_value)}'")

                                if valid_value is not None:

                                    new_row_element = RowElement(column_name, column_value)

                                    new_row.insert(new_row_element)

                                else:
                                    send(con, f"ERROR - Wrong value for column '{column_name}' "
                                              f"It should be '{column_type}'. Got '{type(column_value)}'")

                            if len(new_row.get_elements()) == len(columns):

                                table.insert_row(new_row)

                                save_list_users(list_users)

                                send(con, f"A new row was successfully inserted "
                                          f"\n\t\tto the table '{table_name}'")

                        else:
                            send(con, "ERROR - The number of the values doesn't match the number of the columns !")

                    else:
                        send(con, "ERROR - Wrong syntax : missing '(' or ')' !")

                else:
                    send(con, f"ERROR - Wrong syntax near '{sql_elements[3]}'")

            else:
                send(con, f"ERROR - Table with name '{table_name}' \n\t\t"
                          f"doesn't exist in database '{database_name}'")

        else:
            send(con, f"ERROR - Database with name '{database_name}' doesn't exist")

    else:
        send(con, "ERROR - 'Not enough arguments !'")


def on_sql_select(sql_elements, addr, con, list_users):

    if len(sql_elements) >= 4:

        if sql_elements[1] == "*":

            if sql_elements[2] == "FROM":

                database_name = None
                table_name = None

                try:
                    database_name, table_name = sql_elements[3].split(".")

                except ValueError as e:

                    send(con, "You are missing '.'")

                user = get_user(addr, list_users)

                database = user.get_database(database_name)

                if database is not None:

                    table = database.get_table(table_name)

                    if table is not None:

                        table_rows = table.get_rows()

                        array_table = []

                        column_names = table.get_columns_names().split()

                        array_table.append(column_names)

                        for row in table_rows:

                            row_elements = row.get_elements()

                            row_data = []

                            for element in row_elements:

                                element_dict = element.get_data()

                                row_data.append(element_dict)

                            array_table.append(row_data)

                        send(con, f"{array_table}")

                    else:
                        send(con, f"ERROR - Table '{table_name}' doesn't exist in database '{database_name}' !")

                else:
                    send(con, f"ERROR - Database '{database_name}' doesn't exist !")

            else:
                send(con, f"ERROR - Check your syntax near {sql_elements[2]}")
        else:
            # columns names
            pass



    else:
        send(con, "ERROR - 'Not enough arguments !'")


def on_sql_delete(sql_elements, addr, con, list_users):
    send(con, f"***{sql_elements[0]}***{sql_elements[1]}***{sql_elements[2]}***")

