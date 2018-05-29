from pythonDatabase.Model.column import Column
from pythonDatabase.Model.database import Database
from pythonDatabase.Model.row import Row
from pythonDatabase.Model.row_element import RowElement
from pythonDatabase.Model.table import Table
from pythonDatabase.ReusableFunctions.send_receive import *
from collections import Counter


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

        database_name = None
        table_name = None

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


def select_where(con, conditions_str, rows, columns_names):

    array_table = [columns_names]
    conditions = []
    sql_words = []

    for element in conditions_str:

        if element != "AND" and element != "OR":

            conditions.append(element)

        else:
            sql_words.append(element)

    if len(conditions) - 1 == len(sql_words):

        if len(conditions) > 2 and "AND" in sql_words and "OR" in sql_words:

            send(con, f"ERROR - Can't find the logic of your statement\n"
                      f"\t\tYou have both 'AND' and 'OR' ")
        else:

            selected_rows = dict()

            for condition in conditions:

                column_name, value = condition.split("=")

                for row in rows:

                    row_elements = row.get_elements()

                    for el in row_elements:

                        if el.get_column_name() == column_name and el.get_data() == value:

                            if row not in selected_rows.keys():

                                selected_rows[row] = 1

                            else:
                                new_value = selected_rows.get(row) + 1

                                selected_rows.update({row: new_value})

            # for row in rows:

            for key in selected_rows:

                if len(sql_words) > 0:

                    if sql_words[0] == "AND":

                        if selected_rows[key] == len(conditions):

                            row_elements = key.get_elements()

                            row_data = []

                            for element in row_elements:

                                if element.get_column_name().upper() in columns_names:

                                    element_data = element.get_data()

                                    row_data.append(element_data)

                            array_table.append(row_data)

                    else:

                        if key in rows:

                            row_elements = key.get_elements()

                            row_data = []

                            for element in row_elements:

                                if element.get_column_name().upper() in columns_names:

                                    element_data = element.get_data()

                                    row_data.append(element_data)

                            array_table.append(row_data)

                else:

                    if key in rows:

                        row_elements = key.get_elements()

                        row_data = []

                        for element in row_elements:

                            if element.get_column_name().upper() in columns_names:

                                element_data = element.get_data()

                                row_data.append(element_data)

                        array_table.append(row_data)

            send(con, f"{array_table}")

    else:
        send(con, "ERROR - Missing arguments. \n"
                  "Not enough or too many SQL words between the conditions")


def on_sql_select(sql_elements, addr, con, list_users):

    if len(sql_elements) >= 4:

        user = get_user(addr, list_users)

        # region Selecting all from the table
        if sql_elements[1] == "*":

            if sql_elements[2] == "FROM":

                database_name = None
                table_name = None

                try:
                    database_name, table_name = sql_elements[3].split(".")

                except ValueError as e:

                    send(con, "You are missing '.'")

                database = user.get_database(database_name)

                if database is not None:

                    table = database.get_table(table_name)

                    if table is not None:

                        table_rows = table.get_rows()

                        array_table = []

                        column_names = table.get_columns_names().upper().split()

                        array_table.append(column_names)

                        """ For each row gets the data and appends it to a new array 'row_data'
                            The 'row data' is appended to the 'array_table'
                            The 'array_table is being send to the user '"""
                        for row in table_rows:

                            row_elements = row.get_elements()

                            row_data = []

                            for element in row_elements:

                                element_data = element.get_data()

                                row_data.append(element_data)

                            array_table.append(row_data)

                        if len(sql_elements) > 4:

                            if sql_elements[4] == "WHERE":

                                if len(sql_elements) > 5:

                                    conditions_str = sql_elements[5:]

                                    select_where(con, conditions_str, table_rows, column_names)

                                else:
                                    send(con, "ERROR - 'Not enough arguments after 'WHERE' !'")

                            else:
                                send(con, f"ERROR - Wrong syntax near '{sql_elements[3]}', expected 'WHERE'")

                        else:
                            send(con, f"{array_table}")

                    else:
                        send(con, f"ERROR - Table '{table_name}' doesn't exist in database '{database_name}' !")

                else:
                    send(con, f"ERROR - Database '{database_name}' doesn't exist !")

            else:
                send(con, f"ERROR - Check your syntax near {sql_elements[2]}")
        # endregion

        # region Selecting some columns from the table
        else:

            # column names from the SQL statement
            column_names = []

            for element in sql_elements[1:]:

                if element == "FROM":

                    break

                else:

                    element = element.replace(",", "")

                    column_names.append(element)

            # Make a set to prevent the user from typing the same column name 2 times
            column_names = set(column_names)

            # Used to reach the next element of the SQL statement
            next_index = len(column_names) + 1

            if sql_elements[next_index] == "FROM":

                next_index += 1

                # It checks whether the SQL statement length is the same as the index of the next element
                # prevents from accessing an element which doesn't exist
                if len(sql_elements) - 1 >= next_index:

                    database_name = None
                    table_name = None

                    try:
                        database_name, table_name = sql_elements[next_index].split(".")

                    except ValueError:

                        send(con, "ERROR - You are missing '.'")

                        return

                    database = user.get_database(database_name)

                    if database is not None:

                        table = database.get_table(table_name)

                        if table is not None:

                            columns = table.get_columns()

                            table_col_names = []

                            for c in columns:

                                table_col_names.append(c.get_name())

                            # It checks whether the column names from the SQL statement exist in the table or not
                            for name in column_names:

                                if name not in table_col_names:

                                    send(con, f"ERROR - The column '{name}' "
                                              f"doesn't exist in table '{database_name}.{table_name}'")

                                    return

                            table_rows = table.get_rows()

                            # this is what it will be send to the user
                            # It will hold the column names and the data
                            array_table = []

                            arr_column_names = []

                            # It capitalize the column names for better visibility in the Text Table
                            for name in column_names:

                                arr_column_names.append(name.upper())

                            array_table.append(arr_column_names)

                            """ It loop tru all table rows.
                                For each row loops tru all row elements
                                It checks whether the column name of each element
                                is in the list of desired columns or not
                                Then appends the element data to an array 'row_data'
                                And appends the 'row_data' to the 'array_table'"""
                            for row in table_rows:

                                row_elements = row.get_elements()

                                row_data = []

                                for element in row_elements:

                                    element_col_name = element.get_column_name()

                                    if element_col_name.upper() in arr_column_names:

                                        element_data = element.get_data()

                                        row_data.append(element_data)

                                if len(row_data) > 0:

                                    array_table.append(row_data)

                            if len(sql_elements) - 1 == next_index:

                                send(con, f"{array_table}")

                            else:

                                next_index += 1

                                if sql_elements[next_index] == "WHERE":

                                    if len(sql_elements) - 1 > next_index:

                                        next_index += 1

                                        conditions_str = sql_elements[next_index:]

                                        list_columns_names = []

                                        for name in reversed(list(column_names)):

                                            upper_name = name.upper()

                                            list_columns_names.append(upper_name)

                                        select_where(con, conditions_str, table_rows, list_columns_names)

                                    else:
                                        send(con, "ERROR - 'Not enough arguments after 'WHERE' !'")

                                else:
                                    send(con, f"ERROR - Wrong syntax near '{sql_elements[3]}', expected 'WHERE'")

                        else:
                            send(con, f"ERROR - Table '{table_name}' doesn't exist in database '{database_name}' !")

                    else:
                        send(con, f"ERROR - Database '{database_name}' doesn't exist !")

                else:
                    send(con, "ERROR - Missing arguments !")
            else:
                send(con, f"ERROR - Wrong syntax near '{sql_elements[next_index]}' !")
        # endregion

    else:
        send(con, "ERROR - 'Not enough arguments !'")


def on_sql_delete(sql_elements, addr, con, list_users):

    if len(sql_elements) >= 4:

        user = get_user(addr, list_users)

        # region DELETE * FROM table_name
        if sql_elements[1] == "*":

            if sql_elements[2] == "FROM":

                database_name = None
                table_name = None

                try:
                    database_name, table_name = sql_elements[3].split(".")

                except ValueError:

                    send(con, "ERROR - You are missing '.'")

                database = user.get_database(database_name)

                if database is not None:

                    table = database.get_table(table_name)

                    if table is not None:

                        table.delete_all_rows()

                        save_list_users(list_users)

                        send(con, f"All data in '{database_name} have been successfully deleted'")

                    else:
                        send(con, f"ERROR - Table '{table_name}' doesn't exist in database '{database_name}' !")

                else:
                    send(con, f"ERROR - Database '{database_name}' doesn't exist !")

            else:
                send(con, f"ERROR - Wrong syntax near '{sql_elements[2]}' !")
        # endregion

        # region DELETE FROM table_name WHERE condition
        elif sql_elements[1] == "FROM":

            database_name = None
            table_name = None

            try:
                database_name, table_name = sql_elements[2].split(".")

            except ValueError:

                send(con, "ERROR - You are missing '.'")

            database = user.get_database(database_name)

            if database is not None:

                table = database.get_table(table_name)

                if table is not None:

                    if sql_elements[3] == "WHERE":

                        conditions = []
                        sql_words = []

                        for element in sql_elements[4:]:

                            if element != "AND" and element != "OR":

                                conditions.append(element)

                            else:
                                sql_words.append(element)

                        if len(conditions) - 1 == len(sql_words):

                            if len(conditions) > 2 and "AND" in sql_words and "OR" in sql_words:

                                send(con, f"ERROR - Can't find the logic of your statement\n"
                                          f"\t\tYou have both 'AND' and 'OR' ")
                            else:

                                table_rows = table.get_rows()

                                rows_to_delete = dict()

                                for row in table_rows:

                                    row_elements = row.get_elements()

                                    for condition in conditions:

                                        column_name, value = condition.split("=")

                                        for el in row_elements:

                                            if el.get_column_name() == column_name and el.get_data() == value:

                                                if row not in rows_to_delete.keys():

                                                    rows_to_delete[row] = 1
                                                else:
                                                    new_value = rows_to_delete.get(row) + 1

                                                    rows_to_delete.update({row: new_value})

                                for key in rows_to_delete:

                                    if len(sql_words) > 0:

                                        if sql_words[0] == "AND":

                                            if rows_to_delete[key] == len(conditions):

                                                table.delete_row(key)

                                                send(con, "A row was deleted successfully !")

                                        else:

                                            if key in table_rows:

                                                table.delete_row(key)

                                                send(con, "A row was deleted successfully !")

                                    else:

                                        if key in table_rows:

                                            table.delete_row(key)

                                            send(con, "A row was deleted successfully !")

                            # save_list_users(list_users)

                        else:
                            send(con, "ERROR - Missing arguments. \n"
                                      "Not enough or too many SQL words between the conditions")

                    else:
                        send(con, f"ERROR - Check your syntax near '{sql_elements[3]}'. Expected 'WHERE' !")

                else:
                    send(con, f"ERROR - Table '{table_name}' doesn't exist in database '{database_name}' !")

            else:
                send(con, f"ERROR - Database '{database_name}' doesn't exist !")
        # endregion

        else:
            send(con, f"ERROR - Wrong syntax near '{sql_elements[1]}' !")
    else:
        send(con, "ERROR - not enough arguments !")

