from pythonDatabase.Model.table import Table
from pythonDatabase.ReusableFunctions.send_receive import send_receive


def create_table(user_input, user):
    new_table = Table(user_input)
    user.append_table(new_table)


def delete_table(user_tables, user_choice):
    for table in user_tables:
        if table.get_name() == user_choice:
            user_tables.remove(table)

