from _thread import start_new_thread

from Model.user import *
from socket import *
from Server.sql_functions import *

HOST, PORT = '', 8000

# It creates a stream socket in the Internet domain
s = socket(AF_INET, SOCK_STREAM)
s.bind((HOST, PORT))
s.listen(5)

list_users = []


# region MENU - mode
# It gets the user input for the table name,
# It validates the input
# It creates a new table and appends it to the user tables
# saves the lists of users
def on_create_table(addr, con, database):

    tables = database.get_list_tables()

    tables_names = ""

    for table in tables:

        tables_names += table.get_name() + " "

    while True:

        user_input = send_receive(" Enter table name: ", con)

        print(user_input)

        if user_input == "":

            send(con, " Wrong input! Try again !!!")

        elif user_input in tables_names:

            send(con, f"ERROR - Table '{user_input}' already exist !")

        else:

            # It creates a table and appends it to the database
            new_table = Table(user_input)

            database.append_table(new_table)

            save_list_users(list_users)

            on_manage_database(addr, con)

            break


def on_drop_table(addr, con, database):

    tables = database.get_list_tables()

    arr_table = [["TABLES"]]

    for t in tables:
        arr_table.append([t.get_name()])

    while True:

        table_name = send_receive(str(arr_table), con)

        if table_name == 'b':

            break

        if table_name not in str(arr_table[1:]):

            send(con, f"ERROR - Table '{table_name} doesn't exist in database '{database.get_name()}'")

        else:

            delete_table(tables, table_name)

            save_list_users(list_users)

            break

    on_manage_database(addr, con)


def on_add_column(addr, con, table, database):

    add_column(table, con)

    save_list_users(list_users)

    on_manage_existing_table(addr, con, database)


def on_remove_column(addr, con, table, database):

    columns_names = get_list_columns(con, table)

    str_to_send = "Choose a column to delete or enter 'b' to go back"

    while True:

        user_choice = send_receive(str_to_send, con)

        if user_choice == "b":

            break

        if user_choice in columns_names:

            column = table.get_column(user_choice)

            table.delete_column(column)

            save_list_users(list_users)

            on_manage_existing_table(addr, con, database)

            break

        str_to_send = "Wrong input !!!  Try again: "


def on_getting_columns(addr, con, table, database):

    get_list_columns(con, table)

    while True:

        user_choice = send_receive("Enter 'b' to go back", con)

        if user_choice == "b":

            on_manage_existing_table(addr, con, database)

            break


def on_insert_row(addr, con, table_name, database):
    pass


def on_remove_row(addr, con, table_name, database):
    pass


def on_get_row(addr, con, table_name, database):
    pass


def on_manage_existing_table(addr, con, database):

    tables = database.get_list_tables()

    arr_table = [["TABLES"]]

    for t in tables:

        arr_table.append([t.get_name()])

    while True:

        table_name = send_receive(str(arr_table), con)

        if table_name == 'b':

            main_functionality(addr, con)

            break

        if table_name in str(arr_table[1:]):

            break

        else:
            send(con, f"Table '{table_name}' doesn't exist ! Choose an existing one !")

    while True:

        user_choice = send_receive(" Choose an option:\n"
                                   "1) Add a column\n"
                                   "2) Remove column\n"
                                   "3) Get columns\n"
                                   "4) Insert a row\n"
                                   "5) Remove a row\n"
                                   "6) Get a row\n"
                                   "'b' - to go back \n", con)

        table = database.get_table(table_name)

        if user_choice == '1':

            on_add_column(addr, con, table, database)

            break

        elif user_choice == '2':

            on_remove_column(addr, con, table, database)

            break

        elif user_choice == '3':

            on_getting_columns(addr, con, table, database)

            break

        elif user_choice == '4':

            on_insert_row(addr, con, table_name, database)

            break

        elif user_choice == '5':

            on_remove_row(addr, con, table_name, database)

            break

        elif user_choice == '6':

            on_get_row(addr, con, table_name, database)

            break

        elif user_choice == 'b':

            on_manage_existing_table(addr, con, database)

            break

        else:
            print(" Wrong input from " + str(addr))

            con.send(str.encode(" Wrong input. Try again !!!"))


def choose_database(addr, con):

    user = get_user(addr, list_users)

    user_databases = user.get_list_databases()

    arr_table = [["DATABASES"]]

    for d in user_databases:

        arr_table.append([d.get_name()])

    while True:

        user_choice = send_receive(f"{arr_table}", con)

        if user_choice in str(arr_table[1:]):

            break

        if user_choice == "b":

            main_functionality(addr, con)

            break

        else:

            arr_table = f"Error - Database {user_choice} doesn't exist !"

    database = user.get_database(user_choice)

    return database


def on_manage_database(addr, con):

    database = choose_database(addr, con)

    while True:
        user_choice = send_receive(" Choose an option:\n"
                                   "1) Create table\n"
                                   "2) Drop table\n"
                                   "3) Manage existing table\n"
                                   "'b' - to go back \n", con)

        if user_choice == '1':

            on_create_table(addr, con, database)

            break

        elif user_choice == '2':

            on_drop_table(addr, con, database)

            break

        elif user_choice == '3':

            on_manage_existing_table(addr, con, database)

            break

        elif user_choice == 'b':

            main_functionality(addr, con)

            break

        else:
            print(" Wrong input from " + str(addr))

            con.send(str.encode(" Wrong input. Try again !!!"))
    pass


def on_create_database(addr, con):

    user = get_user(addr, list_users)

    databases = user.get_list_databases()

    while True:

        database_name = send_receive("Type database name: ", con)

        database = user.get_database(database_name)

        if database not in databases:

            new_database = Database(database_name, [])

            user.create_database(new_database)

            save_list_users(list_users)

            main_functionality(addr, con)

            break

        else:
            send(con, f"Database '{database_name}' already exist! ")


def on_drop_database(addr, con):

    database = choose_database(addr, con)

    user = get_user(addr, list_users)

    user.drop_database(database)

    save_list_users(list_users)

    main_functionality(addr, con)


def main_functionality(client_address, con):

    while True:
        user_choice = send_receive(" Choose an option:\n"
                                   "1) Manage database\n"
                                   "2) Create database\n"
                                   "3) Drop database\n"
                                   "'out' - to log out \n", con)

        if user_choice == '1':

            on_manage_database(client_address, con)

            break

        elif user_choice == '2':

            on_create_database(client_address, con)

            break

        elif user_choice == '3':

            on_drop_database(client_address, con)

            break

        elif user_choice == 'out':

            menu(client_address, con)

            break

        else:
            print(" Wrong input from " + str(client_address))
            con.send(str.encode(" Wrong input. Try again !!!"))

# endregion


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
                               "2) Register\n", con)
    print("<<< MyDB >>> Waiting for client choice - 1) Login - 2) Register ")

    if user_choice == '1':
        on_login(address, con)

    elif user_choice == '2':
        on_register(address, con)

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

        except IndexError:

            send(con, "ERROR - Not enough arguments !")
# endregion


# region new Thread - Menu or Sql mode
def new_thread(adr, connection):

    while True:

        message = receive(connection)

        print("Received from " + str(adr) + ": ", message)

        if message == "menu":

            menu(adr, connection)

            break

        elif message == "sql":

            on_sql_mode(adr, connection)

            break

# endregion


# region Accepts the request and starts a new thread for each connection
def accept_connection():

    print("MyDB server is listening on port: ", PORT)

    while True:

        conn, adr = s.accept()

        print(adr, " Successfully connected to the server !!! ")

        start_new_thread(new_thread, (adr, conn, ))
        # threading.Thread(target=new_thread, args=(adr, conn, ), daemon=True)
# endregion


if __name__ == "__main__":

    list_users = load_list_users()

    accept_connection()


