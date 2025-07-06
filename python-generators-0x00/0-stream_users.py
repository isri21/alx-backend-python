import mysql.connector

def stream_users():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="ALX_prodev"
        )
        cursor = connection.cursor()
        cursor.execute("SELECT user_id, name, email, age FROM user_data")
        
        for row in cursor:
            yield {
                'user_id': row[0],
                'name': row[1],
                'email': row[2],
                'age': int(row[3])  # Convert DECIMAL to int for consistency with output
            }
        
        cursor.close()
        connection.close()
    except mysql.connector.Error as err:
        print(f"Error streaming users: {err}")