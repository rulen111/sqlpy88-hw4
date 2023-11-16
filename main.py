# SQLPY-88 Homework #4. Working with PostgreSQL in Python. Student Akhmarov Ruslan

# Imports
import psycopg2


# Defining functions for DB usage
def create_tables(cur):
    """
    Creates two tables with pre-specified structure
    :param cur: psycopg2 cursor object
    :return: None
    """
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clients(
        client_id SERIAL PRIMARY KEY,
        first_name VARCHAR(40) NOT NULL,
        last_name VARCHAR(40) NOT NULL,
        email VARCHAR(60) NOT NULL UNIQUE CHECK(email LIKE '%@%')
    );
    
    CREATE TABLE IF NOT EXISTS phones(
        phone BIGINT PRIMARY KEY CHECK(phone BETWEEN 70000000000 AND 80000000000),
        client_id INTEGER NOT NULL REFERENCES clients(client_id)
    );
    """)


def drop_tables(cur):
    """
    Deletes both tables created with create_tables(cur)
    :param cur: psycopg2 cursor object
    :return: None
    """
    cur.execute("""
    DROP TABLE phones;
    DROP TABLE clients;
    """)


def new_client(cur, first_name, last_name, email, phones=None):
    """
    Creates a new entry with given attributes
    :param cur: psycopg2 cursor object
    :param first_name: entry's first_name (string, not null)
    :param last_name: entry's last_name (string, not null)
    :param email: entry's email (string, unique, not null, like "%@%")
    :param phones: list of phone numbers (integer) to insert
    :return: None
    """
    cur.execute("""
    INSERT INTO clients VALUES
    (DEFAULT, %s, %s, %s)
    RETURNING client_id;
    """, (first_name, last_name, email))
    client_id = cur.fetchone()[0]

    if phones:
        for phone in phones:
            cur.execute("""
            INSERT INTO phones VALUES
            (%s, %s);
            """, (phone, client_id))


def add_phone(cur, phone, client_id):
    """
    Adds specified phone number to DB
    :param cur: psycopg2 cursor object
    :param phone: phone number (integer)
    :param client_id: corresponding client id (integer)
    :return: None
    """
    cur.execute("""
    INSERT INTO phones VALUES
    (%s, %s);
    """, (phone, client_id))


def update_info(cur, client_id, first_name, last_name, email, phones=None):
    """
    Updates all columns for the specified entry with given parameters
    :param cur: psycopg2 cursor object
    :param client_id: client id number (integer)
    :param first_name: entry's first_name (string, not null)
    :param last_name: entry's last_name (string, not null)
    :param email: entry's email (string, unique, not null, like "%@%")
    :param phones: list of phone numbers (integer)
    :return: None
    """
    cur.execute("""
    UPDATE clients SET
    first_name = %s,
    last_name = %s,
    email = %s
    WHERE client_id = %s;
    """, (first_name, last_name, email, client_id))

    if phones:
        for phone in phones:
            cur.execute("""
            UPDATE phones SET
            phone = %s
            WHERE client_id = %s;
            """, (phone, client_id))


def del_phone(cur, phone):
    """
    Deletes phone number from the table
    :param cur: psycopg2 cursor object
    :param phone: phone number to delete (integer)
    :return: None
    """
    cur.execute("""
    DELETE FROM phones WHERE phone = %s;
    """, (phone,))


def del_client(cur, client_id):
    """
    Deletes entry from the table
    :param cur: psycopg2 cursor object
    :param client_id: id number of the entry to delete
    :return: None
    """
    cur.execute("""
    DELETE FROM clients WHERE client_id = %s;
    """, (client_id,))


def get_client(cur, client_id=None, first_name="%", last_name="%", email="%", phone=None):
    """
    Used to get entries with specified values. You can pass any single parameter or a combination,
    returns all entries if nothing is passed (except :param cur).
    :param cur: psycopg2 cursor object
    :param client_id: entry's id number to search for (integer, optional)
    :param first_name: entry's first_name (string, optional) to search for
    :param last_name: entry's last_name (string, optional) to search for
    :param email: entry's email (string, unique, optional, like "%@%") to search for
    :param phone: phone number (integer, optional) to search for
    :return: list with tuple structured entries
    """
    if phone:
        cur.execute("""
        SELECT * FROM clients c
        LEFT JOIN phones p 
        ON c.client_id = p.client_id
        WHERE p.phone = %s;
        """, (phone,))
        return cur.fetchall()
    else:
        cur.execute("""
        SELECT * FROM clients c
        LEFT JOIN phones p 
        ON c.client_id = p.client_id
        WHERE ((%s IS NULL) OR (c.client_id = %s))
        AND c.first_name LIKE %s
        AND c.last_name LIKE %s
        AND c.email LIKE %s;
        """, (client_id, client_id, first_name, last_name, email))
        return cur.fetchall()


# Trying out the functions defined above
if __name__ == "__main__":
    with psycopg2.connect(database="netology_hw4", user="postgres", password="postgres") as conn:
        with conn.cursor() as cur:
            # Deleting tables
            drop_tables(cur)
            conn.commit()

            # Creating tables
            create_tables(cur)
            conn.commit()

            # Creating client entries
            new_client(cur, "Ruslan", "Akhmarov", "ari@gmail.com")
            new_client(cur, "Ruslan", "Imposter", "arimp@gmail.com")
            new_client(cur, "Vasya", "Pupkin", "vasyap@ya.ru", [79999999999])
            new_client(cur, "Sanya", "Sanyich", "sasanya@mail.ru", [79999999997, 79999999998])

            # Adding phone numbers
            add_phone(cur, 79999999991, 1)

            # Updating an entry
            update_info(cur, 3, "Vanya", "Dupkin", "vanyad@ya.ru")

            # Deleting entries
            del_phone(cur, 79999999999)
            del_client(cur, 2)

            # Searching for entries
            print(get_client(cur, client_id=None, first_name="%", last_name="Sanyich", email="%", phone=None))
            print(get_client(cur, client_id=None, first_name="%", last_name="%", email="%", phone=79999999998))
    conn.close()
