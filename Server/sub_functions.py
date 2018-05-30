import pickle

from Model.column import Column
from Model.table import Table


def hash_password(password):
    hashed_pass = ""
    counter = 0
    salt = "The best hashing ever !!!"

    for symbol in password:
        hashed_pass = hashed_pass + chr(ord(symbol) + ord(str(len(password))) + 1122)
        hashed_pass = hashed_pass + salt[counter]
        counter = counter + 1
    return hashed_pass


# region Users
def load_list_users():

    try:
        with open("users.dat", "rb") as f:

            list_users = pickle.load(f)

    except Exception:

        list_users = []

    return list_users


def save_list_users(list_users):

    with open("users.dat", "wb") as f:

        pickle.dump(list_users, f)


def get_user(addr, list_users):

    for user in list_users:

        if addr == user.get_address():

            return user


def validate_user(list_users, username, password, addr):

    value_to_return = False

    for user in list_users:

        if user.get_username() == username and user.get_pass() == hash_password(password):

            user.set_address(addr)

            value_to_return = True

            break

    return value_to_return


def validate_user_exist(sql_elements, addr, con, list_users):
    username = ""
    password = ""

    if sql_elements[1]:

        username_elements = sql_elements[1].split("=")

        if username_elements[0] == "USERNAME" and username_elements[1]:

            username = username_elements[1]

        else:
            send_receive(f"Check your syntax near {sql_elements[1]}", con)

            return

    if sql_elements[2]:

        password_elements = sql_elements[2].split("=")

        if password_elements[0] == "PASSWORD":

            password = password_elements[1]

        else:
            send_receive(f"Check your syntax near {sql_elements[2]}", con)

            return

    valid_user = validate_user(list_users, username, password, addr)

    return valid_user, username, password
# endregion


# region Tables
def create_table(user_input, user):
    new_table = Table(user_input)
    user.append_table(new_table)


def delete_table(user_tables, user_choice):
    for table in user_tables:
        if table.get_name() == user_choice:
            user_tables.remove(table)
# endregion


# region Columns
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
# endregion


# region Send - Receive
def receive(con):

    b_array = con.recv(1024)

    received_str = b_array.decode("UTF-8")  # str = str(b_array, "UTF-8") - this works too

    return received_str


def send(conn, message):

    conn.send(str.encode(message))


def send_receive(message, conn):

    send(conn, message)

    return receive(conn)


def send_list_tables(addr, con, list_users):

    user = get_user(addr, list_users)

    user_tables = user.get_list_tables()

    str_tables_choice = " Choose a table:\n"

    for table in user_tables:

        str_tables_choice += str(table.get_name()) + "\n"

    user_choice = send_receive(str_tables_choice + "\n\nOr type 'b' to go back: ", con)

    # validates the user input
    while user_choice not in str_tables_choice and user_choice != 'b':
        user_choice = send_receive("Wrong input, table: '" + user_choice + "' doesn't exist\n Try again: ", con)

    return user_tables, user_choice
# endregion


def remove_special_char(sql_string):

    str_to_return = sql_string.replace('"', '')

    str_to_return = str_to_return.replace('\\', '')

    return str_to_return

