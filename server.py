from pythonExamProject_database.objectsDB import *
from socket import *
from _thread import *
from pythonExamProject_database.myFunctions import *
import os

HOST, PORT = '', 8000

s = socket(AF_INET, SOCK_STREAM)
s.bind((HOST, PORT))
s.listen(5)
list_users = []

#region main functionality for the Menu


def get_user_tables(addr):
    for user in list_users:
        if addr == user.get_address():
            return user.get_list_tables(), user


def on_create_table(addr, con):
    while True:
        user_input = send_receive("Enter table name: >", con)
        print(user_input)

        if user_input == "":
            con.send(str.encode("> Wrong input! Try again !!!"))
        else:
            user_tables, user = get_user_tables(addr)
            print("---------------------------------------------------------------------------")
            break


def on_drop_table(addr, con):
    pass


def on_manage_existing_table(addr, con):

    # list_tables_for_user()
    # Add  columns
    # Remove columns
    # Insert rows
    # Delete row
    # Extract
    user_choice = send_receive("> Choose the table you want to manage:\n", con)


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

    valid_user = False
    while True:
        print("<<< Login >>> Waiting for username from " + str(addr) + "......")
        username = send_receive("-Login -Username:> ", con)
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
                message = "> The username exist already. Try again !!!\n<MyDB"
                user_exist = True

        if not user_exist:
            print("<<< Register >>> Waiting for password from " + str(addr) + "......")
            password = send_receive("-Register -Password:> ", conn)
            print("<<< Register >>> From " + str(addr) + " received " + password)

            hashed_pass = hash_password(password)

            new_user = User(username, hashed_pass, addr, [])

            list_users.append(new_user)

            save_list_users(list_users)

            #Creates directory with the same name as the username

            if not os.path.exists("databases/" + username):
                os.mkdir("databases/" + username)

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
        reply = receive(connection)
        print("Received: ", reply)
        if reply == "1":
            menu(adr, connection)
            break
        elif reply == "2":
            sql(adr, connection)
            break
        elif reply == "quit":
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


