from pythonDatabase.Server.ReusableFunctions.send_receive import *
from pythonDatabase.Server.ReusableFunctions.pass_hashing import *
import pickle


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


