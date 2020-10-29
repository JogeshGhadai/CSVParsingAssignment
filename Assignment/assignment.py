import csv
import mysql.connector as mysql


def main():

    all_data = {}
    try:
        with open("./EmpDetails.csv", "r") as csvfile:
            temp_data = [data for data in csv.reader(csvfile)]
            all_data["EmpDetails"] = {"headers": temp_data[:1],
                                      "data": temp_data[1:]}
        with open("./EmpSkills.csv", "r") as csvfile:
            temp_data = [data for data in csv.reader(csvfile)]
            all_data["EmpSkills"] = {"headers": temp_data[:1],
                                     "data": temp_data[1:]}
        with open("./EmpStackDetails.csv", "r") as csvfile:
            temp_data = [data for data in csv.reader(csvfile)]
            all_data["EmpStackDetails"] = {"headers": temp_data[:1],
                                           "data": temp_data[1:]}
    except Exception as e:
        print "Error Occured While Reading the CSV files: "+e.message
    
    try:
        db = mysql.connect(
            host = "localhost",
            user = "jogeshg",
            passwd = "abcd1234",
            database = "assignmentdb"
        )

        empdetails_insert_query = "INSERT INTO Employee (EmpId, FirstName, \
                                                         LastName, CreatedById, \
                                                         LastUpdatedById, CreatedBy, \
                                                         LastUpdatedBy) \
                                                 VALUES (%s, %s, %s, %s, %s, %s, %s);"
        empdetails_fetch_query = "SELECT * FROM Employee;"

        empskills_insert_query = "INSERT INTO EmployeeSkills (SkillName, EmpId) \
                                                      VALUES (%s, %s);"
        empskills_fetch_query = "SELECT * FROM EmployeeSkills;"
    
        empstackdetails_insert_query = "INSERT INTO StackData (StackId, StackNickName, EmpId) \
                                                       VALUES (%s, %s, %s);"
        empstackdetails_fetch_query = "SELECT * FROM StackData;"

        empdetails_insert_cursor = db.cursor()
        empdetails_fetch_cursor = db.cursor()

        empskills_insert_cursor = db.cursor()
        empskills_fetch_cursor = db.cursor()

        empstackdetails_insert_cursor = db.cursor()
        empstackdetails_fetch_cursor = db.cursor()
        
        #import pdb;pdb.set_trace()
        empdetails_insert_cursor.executemany(empdetails_insert_query, 
                                             all_data["EmpDetails"]["data"])
        empskills_insert_cursor.executemany(empskills_insert_query, 
                                            all_data["EmpSkills"]["data"])
        empstackdetails_insert_cursor.executemany(empstackdetails_insert_query,
                                                  all_data["EmpStackDetails"]["data"])

        db.commit()
        print "Data Saved to DB Successfully!"

    except Exception as e:
        print "Error Occured While Saving Data into DB: "+e.message
    
    finally:
        empdetails_insert_cursor.close()
        empskills_insert_cursor.close()
        empstackdetails_insert_cursor.close()
        db.close()


if __name__ == "__main__":
    main()
