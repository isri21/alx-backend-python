import mysql.connector

def stream_user_ages():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="ALX_prodev"
        )
        cursor = connection.cursor()
        cursor.execute("SELECT age FROM user_data")
        
        for age in cursor:  # Loop 1: Yield each age
            yield float(age[0])
        
        cursor.close()
        connection.close()
    except mysql.connector.Error as err:
        print(f"Error streaming ages: {err}")

def calculate_average_age():
    total_age = 0
    count = 0
    for age in stream_user_ages():  # Loop 2: Process ages
        total_age += age
        count += 1
    
    if count == 0:
        return 0
    average = total_age / count
    print(f"Average age of users: {average:.2f}")
    return average

if __name__ == "__main__":
    calculate_average_age()