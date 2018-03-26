from pythonDatabase.objectsDB import *
from socket import *
from _thread import *
from pythonDatabase.myFunctions import *
import os

HOST, PORT = '', 8000

s = socket(AF_INET, SOCK_STREAM)
s.bind((HOST, PORT))
s.listen(5)
list_users = []

#region main functionality for the Menu


def get_user(addr):
    for user in list_users:
        if addr == user.get_address():
            return user


def on_create_table(addr, con):
    while True:
        user_input = send_receive("Enter table name: >", con)
        print(user_input)

        if user_input == "":
            con.send(str.encode("> Wrong input! Try again !!!"))
        else:
            user = get_user(addr)

            # #     -------------- creates a file with this name --------------
            #
            # print("databases/" + user.get_username() + "/" + user_input + ".txt")
            #
            # if not os.path.exists("databases/" + user.get_username() + "/" + user_input + ".txt"):
            #     with open("databases/" + user.get_username() + "/" + user_input + ".txt", "w"):
            #         pass/

            new_table = Table(user_input)
            user.append_table(new_table)
            # -------------------------------------------- To check --------------------------------
            save_list_users(list_users)
            main_functionality(addr, con)
            break


def on_drop_table(addr, con):
    user_tables, user_choice = send_list_tables(addr, con)

    user = get_user(addr)

    # if os.path.exists("databases/" + user.get_username() + "/" + user_choice + ".txt"):
    #     if os.path.isfile("databases/" + user.get_username() + "/" + user_choice + ".txt"):
    #         os.remove("databases/" + user.get_username() + "/" + user_choice + ".txt")

    for table in user_tables:
        if table.get_name() == user_choice:
            user_tables.remove(table)

    main_functionality(addr, con)


def send_list_tables(addr, con):
    user = get_user(addr)
    user_tables = user.get_list_tables()
    # files = os.listdir("databases/" + user.get_username())

    str_tables_choice = " >Choose a table:\n"

    for table in user_tables:
        str_tables_choice += str(table.get_name()) + "\n"

    # for file in files:
    #     table = file[:-4]
    #     user_tables.append(table)
    #     str_tables_choice += table + "\n"

    user_choice = send_receive(str_tables_choice, con)
    return user_tables, user_choice


def add_column(addr, con, table_name):
    user = get_user(addr)
    table = user.get_table(table_name)
    column_name = send_receive("> Enter column name: ", con)
    column_type = ""
    user_choise = send_receive("> Choose a column type: 'n"
                               "1) String"
                               "2) Integer"
                               "3) Float"
                               "4) Boolean", con)

    while True:
        user_choise = send_receive("> Wrong input, try again with number from 1 to 4", con)

        if user_choise == '1':
            column_type = 'str'
            break
        if user_choise == '2':
            column_type = 'int'
            break
        if user_choise == '3':
            column_type = 'float'
            break
        if user_choise == '4':
            column_type = 'bool'
            break

    table.add_column(column_name, column_type)
    save_list_users(list_users)
    on_manage_existing_table(addr, con)


def remove_column(addr, con, table_name):
    pass


def get_list_columns(addr, con, table_name):
    user = get_user(addr)
    table = user.get_table(table_name)
    columns_names = table.get_list_columns()
    str_columns = "Columns in " + table_name + " :\n"

    for column_name in columns_names:
        column_type = table.get_column_type(column_name)
        str_columns += str(column_type) + "\t" + str(column_name) + "\n"

    user_choice = send_receive("> " + str_columns, con)
    on_manage_existing_table(addr, con)
    return user_choice


def insert_row(addr, con, table_name):
    pass


def remove_row(addr, con, table_name):
    pass


def get_row(addr, con, table_name):
    pass


def on_manage_existing_table(addr, con):

    while True:
        user_tables, table_name = send_list_tables(addr, con)

        user_choice = send_receive("> Choose an option:\n"
                                   "1) Add a column\n"
                                   "2) Remove column\n"
                                   "3) Get columns\n"
                                   "4) Insert a row\n"
                                   "5) Remove a row\n"
                                   "6) Get a row\n"
                                   "'out' - to log out \n", con)

        if user_choice == '1':
            add_column(addr, con, table_name)
            break
        elif user_choice == '2':
            remove_column(addr, con, table_name)
            break
        elif user_choice == '3':
            get_list_columns(addr, con, table_name)
            break
        elif user_choice == '4':
            insert_row(addr, con, table_name)
            break
        elif user_choice == '5':
            remove_row(addr, con, table_name)
            break
        elif user_choice == '6':
            get_row(addr, con, table_name)
            break
        elif user_choice == 'out':
            break
        else:
            print(" Wrong input from " + str(addr))
            con.send(str.encode("> Wrong input. Try again !!!"))


def main_functionality(client_address, con):
    while True:
        user_choice = send_receive("> Choose an option:\n"
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
            con.send(str.encode("> Wrong input. Try again !!!"))
#endregion


#region Login - validates the username and password
def on_login(addr, con):
    message = ""

    valid_user = False
    while True:
        print("<<< Login >>> Waiting for username from " + str(addr) + "......")
        username = send_receive(">" + message + "<-Login -Username:> ", con)
        print("<<< Login >>> From " + str(addr) + " received " + username)

        print("<<< Login >>> Waiting for password from " + str(addr) + "......")
        password = send_receive("-Login -Password:> ", con)
        print("<<< Login >>> From " + str(addr) + " received " + password)

        for user in list_users:
            if user.get_username() == username and user.get_pass() == hash_password(password):
                user.set_address(addr)
                valid_user = True

        if valid_user:
            print("---------------------------------------------------------------------")
            main_functionality(addr, con)
            break
        else:
            message = "Invalid username or password. Try again: \n"

#endregion


''' Checks wether the user exist or not.
    Hashes the password before saving.
    Creates a new User object.
    Adds it to the list_users.
    Saves the list_users in the file users.dat
    '''
#region Register


def on_register(addr, conn):
    message = ""

    while True:
        user_exist = False

        print("<<< Register >>> Waiting for username from " + str(addr) + "......")
        username = send_receive(message + "-Register -Username:> ", conn)
        print("<<< Register >>> From " + str(addr) + " received " + username)

        for user in list_users:
            if user.get_username() == username:
                message = "> The username already exist. Try again !!!\n<MyDB"
                user_exist = True

        if not user_exist:
            print("<<< Register >>> Waiting for password from " + str(addr) + "......")
            password = send_receive("-Register -Password:> ", conn)
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
    user_choice = send_receive("> Choose an option:\n"
                               "1) Login\n"
                               "2) Register\n"
                               "'quit' - to exit \n", con)
    print("<<< MyDB >>> Waiting for client choice - 1) Login - 2) - 3) Quit Register ......")

    if user_choice == '1':
        on_login(address, con)

    elif user_choice == '2':
        on_register(address, con)

    elif user_choice == 'quit':
        pass # -------------------------------- How to exit? -------------------------------------

    else:
        menu(address, con)
#endregion

#region SQL


def sql(adr, con):
    sql_code = receive(con)
    print("---------" + sql_code + "----------")
#endregion


#region The user chooses to use a menu or SQL commands


def new_thread(adr, connection):
    while True:
        message = receive(connection)
        print("Received from " + str(adr) + ": ", message)
        if message == "1":
            menu(adr, connection)
            break
        elif message == "2":
            sql(adr, connection)
            break
        elif message == "quit":
            exit()
#endregion

#region Accepts the request and starts a new thread for each connection


def accept_connection():
    print("MyDB server is listening on port: ", PORT)

    while True:
        conn, adr = s.accept()

        print(adr, "successfully connected to the server !!!")

        start_new_thread(new_thread, (adr, conn, ))
#endregion


if __name__ == "__main__":

    list_users = load_list_users()

    accept_connection()


