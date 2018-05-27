from pythonDatabase.Model.user import *
from socket import *
from _thread import *

from pythonDatabase.ReusableFunctions.others import *
from pythonDatabase.ReusableFunctions.tables import *
from pythonDatabase.ReusableFunctions.columns import *
from pythonDatabase.Server.sql_functions import *

HOST, PORT = '', 8000

s = socket(AF_INET, SOCK_STREAM)
s.bind((HOST, PORT))
s.listen(5)
list_users = []


# It gets the user input for the table name,
# It validates the input
# It creates a new table and appends it to the user tables
# saves the lists of users
def on_create_table(addr, con):

    while True:

        user_input = send_receive(" Enter table name: ", con)

        print(user_input)

        if user_input == "":

            con.send(str.encode(" Wrong input! Try again !!!"))

        else:

            user = get_user(addr, list_users)

            # region old code
            #      -------------- creates a file with this name --------------
            #
            # print("databases/" + user.get_username() + "/" + user_input + ".txt")
            #
            # if not os.path.exists("databases/" + user.get_username() + "/" + user_input + ".txt"):
            #     with open("databases/" + user.get_username() + "/" + user_input + ".txt", "w"):
            #         pass/
            # endregion

            # It creates a table and appends it to the user.tables
            create_table(user_input, user)

            save_list_users(list_users)

            main_functionality(addr, con)

            break


def on_drop_table(addr, con):

    """ It sends the list of the users tables and returns it back,
        as well as the chosen of the user table """
    user_tables, user_choice = send_list_tables(addr, con, list_users)

    # region old code
    # user = get_user(addr, list_users)
    # if os.path.exists("databases/" + user.get_username() + "/" + user_choice + ".txt"):
    #     if os.path.isfile("databases/" + user.get_username() + "/" + user_choice + ".txt"):
    #         os.remove("databases/" + user.get_username() + "/" + user_choice + ".txt")
    # endregion

    delete_table(user_tables, user_choice)

    main_functionality(addr, con)


def on_add_column(addr, con, table_name):

    user = get_user(addr, list_users)

    table = user.get_table(table_name)

    add_column(table, con)

    save_list_users(list_users)

    on_manage_existing_table(addr, con)


def on_remove_column(addr, con, table_name):

    str_columns, columns_names = get_list_columns(addr, table_name, list_users)

    user_choice = send_receive(str_columns + "\nChoose the column that you want to delete"
                                             "\nOR type 'b' and press enter to go back: ", con)

    while True:

        if user_choice == "b":
            break

        if user_choice in columns_names:
            break

        user_choice = send_receive("Wrong input \n Try again: "
                                   + user_choice, con)

    user = get_user(addr, list_users)

    table = user.get_table(table_name)

    table.delete_column(user_choice)

    save_list_users(list_users)

    on_manage_existing_table(addr, con)


def on_getting_columns(addr, con, table_name):

    str_columns, columns_names = get_list_columns(addr, table_name, list_users)

    user_choice = send_receive(str_columns + "\n\nType 'b' and press enter to go back: ", con)

    while user_choice != 'b':
        user_choice = send_receive("Wrong input \n Type 'b' and press enter to go back: ", con)

    on_manage_existing_table(addr, con)

    return user_choice


def on_insert_row(addr, con, table_name):
    pass


def on_remove_row(addr, con, table_name):
    pass


def on_get_row(addr, con, table_name):
    pass


def on_manage_existing_table(addr, con):

    while True:
        user_tables, table_name = send_list_tables(addr, con, list_users)

        if table_name == 'b':
            main_functionality(addr, con)
            break

        user_choice = send_receive(" Choose an option:\n"
                                   "1) Add a column\n"
                                   "2) Remove column\n"
                                   "3) Get columns\n"
                                   "4) Insert a row\n"
                                   "5) Remove a row\n"
                                   "6) Get a row\n"
                                   "'b' - to go back \n", con)

        if user_choice == '1':
            on_add_column(addr, con, table_name)
            break

        elif user_choice == '2':
            on_remove_column(addr, con, table_name)
            break

        elif user_choice == '3':
            on_getting_columns(addr, con, table_name)
            break

        elif user_choice == '4':
            on_insert_row(addr, con, table_name)
            break

        elif user_choice == '5':
            on_remove_row(addr, con, table_name)
            break

        elif user_choice == '6':
            on_get_row(addr, con, table_name)
            break

        elif user_choice == 'b':
            on_manage_existing_table(addr, con)
            break

        else:
            print(" Wrong input from " + str(addr))
            con.send(str.encode(" Wrong input. Try again !!!"))


def main_functionality(client_address, con):

    while True:
        user_choice = send_receive(" Choose an option:\n"
                                   "1) Create table\n"
                                   "2) Drop table\n"
                                   "3) Manage existing table\n"
                                   "'out' - to log out \n", con)

        if user_choice == '1':
            on_create_table(client_address, con)
            break

        elif user_choice == '2':
            on_drop_table(client_address, con)
            break

        elif user_choice == '3':
            on_manage_existing_table(client_address, con)
            break

        elif user_choice == 'out':
            menu(client_address, con)
            break

        else:
            print(" Wrong input from " + str(client_address))
            con.send(str.encode(" Wrong input. Try again !!!"))


# region Login - validates the username and password
def on_login(addr, con):
    message = ""

    while True:

        print("<<< Login >>> Waiting for username from " + str(addr) + "......")

        username = send_receive(" " + message + "< Login > Username: ", con)

        print("<<< Login >>> From " + str(addr) + " received " + username)

        print("<<< Login >>> Waiting for password from " + str(addr) + "......")

        password = send_receive("< Login > Password: ", con)

        print("<<< Login >>> From " + str(addr) + " received " + password)

        valid_user = validate_user(list_users, username, password, addr)

        if valid_user:

            print("---------------------------------------------------------------------")

            main_functionality(addr, con)

            break

        else:
            message = "Invalid username or password. Try again: \n"

# endregion


# region Register
#     Checks weather the user exist or not.
#     Hashes the password before saving.
#     Creates a new User object.
#     Adds it to the list_users.
#     Saves the list_users in the file users.dat
def on_register(addr, conn):
    message = ""

    while True:
        user_exist = False

        print("<<< Register >>> Waiting for username from " + str(addr) + "......")
        username = send_receive(message + "< Register > Username:", conn)
        print("<<< Register >>> From " + str(addr) + " received " + username)

        for user in list_users:
            if user.get_username() == username:
                message = " The username already exist. Try again !!!\n<MyDB"
                user_exist = True

        if not user_exist:
            print("<<< Register >>> Waiting for password from " + str(addr) + "......")
            password = send_receive("< Register > Password: ", conn)
            print("<<< Register >>> From " + str(addr) + " received " + password)

            hashed_pass = hash_password(password)

            new_user = User(username, hashed_pass, addr, [])

            list_users.append(new_user)

            save_list_users(list_users)

            # #Creates directory with the same name as the username
            #
            # if not os.path.exists("databases/" + username):
            #     os.mkdir("databases/" + username)

            print("<<< Register >>> " + str(addr) + "Was registered successfully !!! \n"
                    "---------------------------------------------------------------------")

            on_login(addr, conn)
            break


def menu(address, con):
    user_choice = send_receive(" Choose an option:\n"
                               "1) Login\n"
                               "2) Register\n"
                               "'quit' - to exit \n", con)
    print("<<< MyDB >>> Waiting for client choice - 1) Login - 2) Register - 3) Quit Register ......")

    if user_choice == '1':
        on_login(address, con)

    elif user_choice == '2':
        on_register(address, con)

    elif user_choice == 'quit':
        pass # -------------------------------- How to exit? -------------------------------------

    else:
        menu(address, con)
# endregion


# region SQL - mode
def on_sql_register(sql_elements, addr, con):

    valid_user, username, password = validate_user_exist(sql_elements, addr, con, list_users)

    if valid_user:

        send(con, "A user with that username already exists ! Try with another username !")

        return False

    else:
        hashed_pass = hash_password(password)

        new_user = User(username, hashed_pass, addr, [])

        list_users.append(new_user)

        save_list_users(list_users)

        send(con, "Successful registration! You are now logged in as well!")

        return True


def on_sql_login(sql_elements, addr, con):

    valid_user, username, password = validate_user_exist(sql_elements, addr, con, list_users)

    if not valid_user:

        send(con, "ERROR - Not valid username or password ! \n \t\t Check them and try again !")

    else:

        send(con, "You are logged in !")

        return True


def on_sql_mode(addr, con):
    logged_in = False

    while True:

        sql_string = send_receive("< SQL-mode >", con)

        sql_string = remove_special_char(sql_string)

        sql_elements = sql_string.split()

        try:
            if not logged_in:

                if sql_elements[0] == "REGISTER":

                    logged_in = on_sql_register(sql_elements, addr, con)

                elif sql_elements[0] == "LOGIN":

                    logged_in = on_sql_login(sql_elements, addr, con)

                else:
                    send(con, "You need to login or register first !")

            else:

                if sql_elements[0] == "REGISTER" or sql_elements[0] == "LOGIN":

                    send(con, "You you are logged in, so you can't log in or register again!")

                elif sql_elements[0] == "CREATE":

                    if sql_elements[1] and sql_elements[1] == "DATABASE":

                        on_sql_create_database(sql_elements, addr, con, list_users)

                    elif sql_elements[1] and sql_elements[1] == "TABLE":

                        on_sql_create_table(sql_elements, addr, con, list_users)

                    else:

                        send(con, f"Check your syntax near '{sql_elements[0]} {sql_elements[1]}'")

                        continue

                elif sql_elements[0] == "DROP":

                    if sql_elements[1] and sql_elements[1] == "DATABASE":

                        on_sql_drop_database(sql_elements, addr, con, list_users)

                    elif sql_elements[1] and sql_elements[1] == "TABLE":

                        on_sql_drop_table(sql_elements, addr, con, list_users)

                    else:

                        send(con, f"Check your syntax near '{sql_elements[0]} {sql_elements[1]}'")

                        continue

                elif sql_elements[0] == "RENAME":

                    if sql_elements[1] and sql_elements[1] == "DATABASE":

                        on_sql_rename_database(sql_elements, addr, con, list_users)

                    elif sql_elements[1] and sql_elements[1] == "TABLE":

                        on_sql_rename_table(sql_elements, addr, con, list_users)

                    else:

                        send(con, f"Check your syntax near '{sql_elements[0]} {sql_elements[1]}'")

                        continue

                elif sql_elements[0] == "ALTER" and sql_elements[1] == "TABLE":

                    if sql_elements[3] == "ADD":

                        on_sql_add_column(sql_elements, addr, con, list_users)

                    elif sql_elements[3] == "DROP" and sql_elements[4] == "COLUMN":

                        on_sql_drop_column(sql_elements, addr, con, list_users)

                    elif sql_elements[3] == "ALTER" and sql_elements[4] == "COLUMN":

                        on_sql_change_column_type(sql_elements, addr, con, list_users)

                    else:

                        send(con, f"Check your syntax near '{sql_elements[0]} {sql_elements[1]}'")

                        continue

                elif sql_elements[0] == "INSERT" and sql_elements[1] == "INTO":

                    on_sql_insert(sql_elements, addr, con, list_users)

                elif sql_elements[0] == "SELECT":

                    on_sql_select(sql_elements, addr, con, list_users)

                elif sql_elements[0] == "DELETE":

                    on_sql_delete(sql_elements, addr, con, list_users)

                else:

                    send(con, f"Check your syntax near '{sql_elements[0]}'")

                    continue

        except IndexError as ie:

            send(con, "ERROR - Not enough arguments !")



# endregion


# region new Thread - Menu or Sql mode
def new_thread(adr, connection):
    while True:
        message = receive(connection)
        print("Received from " + str(adr) + ": ", message)
        if message == "1":
            menu(adr, connection)
            break
        elif message == "2":
            on_sql_mode(adr, connection)
            break
        elif message == "quit":
            exit()
# endregion


# region Accepts the request and starts a new thread for each connection
def accept_connection():
    print("MyDB server is listening on port: ", PORT)

    while True:
        conn, adr = s.accept()

        print(adr, "successfully connected to the server !!!")

        start_new_thread(new_thread, (adr, conn, ))
# endregion


if __name__ == "__main__":

    list_users = load_list_users()

    accept_connection()


