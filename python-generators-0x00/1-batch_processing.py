import mysql.connector

def stream_users_in_batches(batch_size):
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="ALX_prodev"
        )
        cursor = connection.cursor()
        cursor.execute("SELECT user_id, name, email, age FROM user_data")
        
        batch = []
        for row in cursor:  # Loop 1: Fetch rows from cursor
            batch.append({
                'user_id': row[0],
                'name': row[1],
                'email': row[2],
                'age': int(row[3])
            })
            if len(batch) == batch_size:
                yield batch
                batch = []
        
        if batch:  # Yield any remaining rows
            yield batch
            
        cursor.close()
        connection.close()
    except mysql.connector.Error as err:
        print(f"Error streaming users: {err}")

def batch_processing(batch_size):
    for batch in stream_users_in_batches(batch_size):  # Loop 2: Iterate over batches
        for user in batch:  # Loop 3: Process users in batch
            if user['age'] > 25:
                yield user