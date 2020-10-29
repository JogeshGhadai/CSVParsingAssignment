import csv
import mysql.connector as mysql


def main():
    
    #import pdb;pdb.set_trace()
    all_data = None
    with open("./Test - Parse Sheet.csv", "r") as csvfile:
        all_data = csv.reader(csvfile)
        all_data = [data for data in all_data]
    
    print([data[1] for data in all_data])
    db = mysql.connect(
        host = "localhost",
        user = "jogeshg",
        passwd = "abcd1234",
        database = "assignmentdb" 
    )
    cursor = db.cursor()
    cursor1 = db.cursor()
    cursor2 = db.cursor()
    cursor.execute("SELECT * FROM test_table;")
    records = cursor.fetchall()
    print(records)
    query = "INSERT INTO test_table (test_str) VALUES (%s);"
    values = [(all_data[3][1],),(all_data[5][2],),(all_data[5][9],)]
    cursor1.executemany(query, values)
    db.commit()
    cursor2.execute("SELECT * FROM test_table;")
    records = cursor.fetchall()
    print(records)



if __name__ == "__main__":
    main()
