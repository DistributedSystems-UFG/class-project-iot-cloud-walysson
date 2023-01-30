import sqlite3
import datetime

def create_column_user():
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS users (identifier INTEGER, name TEXT, password TEXT)")
    conn.commit()
    conn.close()

def add_admin_user(name, password):
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute("INSERT INTO users VALUES (1, ?,?)", (name, password))
    conn.commit()
    conn.close()

def create_column_devices():
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS devices (identifier INTEGER, name TEXT, type TEXT, state TEXT)")
    conn.commit()
    conn.close()

def create_initial_devices():
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute("INSERT INTO devices VALUES (1, 'Temperature Sensor', 'temperature', 'void')")
    c.execute("INSERT INTO devices VALUES (2, 'Light Sensor', 'light', 'void')")
    c.execute("INSERT INTO devices VALUES (3, 'Red LED', 'led', '0')")
    c.execute("INSERT INTO devices VALUES (4, 'Green LED', 'led', '0')")
    conn.commit()
    conn.close()

def create_user_devices_values_table():
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS user_devices_values (user INTEGER, device INTEGER, value TEXT, created_at TEXT)")
    conn.commit()
    conn.close()

def insert_user_device_value(user, device, value):
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute("INSERT INTO user_devices_values VALUES (?,?,?,?)", (user, device, value, datetime.datetime.now()))
    conn.commit()
    conn.close()

def count_devices():
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM devices")
    count = c.fetchone()[0]
    conn.close()
    return count

def verify_login(name, password):
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE name=? AND password=?", (name, password))
    if c.fetchone():
        return True
    else:
        return False

def run():
    create_column_user()
    if not verify_login('admin', 'admin'):
        add_admin_user('admin', 'admin')
    create_column_devices()
    if count_devices() == 0:
        create_initial_devices()
    create_user_devices_values_table()