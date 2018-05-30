from pythonDatabase.Server.ReusableFunctions.users import *


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
