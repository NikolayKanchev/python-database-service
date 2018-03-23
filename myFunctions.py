import pickle


def load_list_users():
    try:
        with open("users.dat", "rb") as f:
            list_users = pickle.load(f)
    except:
        list_users = []
    return list_users


def save_list_users(list_users):
    with open("users.dat", "wb") as f:
        pickle.dump(list_users, f)


#region Receive and send functions
def receive(con):
    b_array = con.recv(1024)
    received_str = b_array.decode("UTF-8")  # str = str(b_array, "UTF-8") - this works too
    return received_str


def send_receive(message, conn):
    conn.send(str.encode(message))
    return receive(conn)
#endregion


def hash_password(password):
    hashed_pass = ""
    counter = 0
    salt = "The best hashing ever !!!"

    for symbol in password:
        hashed_pass = hashed_pass + chr(ord(symbol) + ord(str(len(password))) + 1122)
        hashed_pass = hashed_pass + salt[counter]
        counter = counter + 1
    return hashed_pass
