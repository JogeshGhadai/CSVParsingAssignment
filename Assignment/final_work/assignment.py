import csv
import mysql.connector as mysql


def main():
    #import pdb;pdb.set_trace()
    all_data = fetct_all_csv_data()
    insert_all_data_to_tables(all_data)
    all_fetched_data = fetch_saved_data_from_db()
    write_fetched_data_to_csv(all_data, all_fetched_data)


def write_fetched_data_to_csv(headers_data, all_data):
    try:

        with open("./outputs/EmpDetails_Output.csv", "w") as out_file:
            writer = csv.writer(out_file)
            writer.writerows(headers_data["EmpDetails"]["headers"])
            writer.writerows(all_data["EmpDetails"])
        
        with open("./outputs/EmpSkills_Output.csv", "w") as out_file:
            writer = csv.writer(out_file)
            writer.writerows(headers_data["EmpSkills"]["headers"])
            writer.writerows(all_data["EmpSkills"])
        
        with open("./outputs/EmpStackDetails_Output.csv", "w") as out_file:
            writer = csv.writer(out_file)
            writer.writerows(headers_data["EmpStackDetails"]["headers"])
            writer.writerows(all_data["EmpStackDetails"])

        print "Data written successfully to CSV."

    except Exception as e:
        print "Error while writing data to CSV: "+e.message


def fetct_all_csv_data():
    all_data = {}
    try:
        with open("./inputs/EmpDetails.csv", "r") as csvfile:
            temp_data = [data for data in csv.reader(csvfile)]
            all_data["EmpDetails"] = {"headers": temp_data[:1],
                                      "data": temp_data[1:]}
        with open("./inputs/EmpSkills.csv", "r") as csvfile:
            temp_data = [data for data in csv.reader(csvfile)]
            all_data["EmpSkills"] = {"headers": temp_data[:1],
                                     "data": temp_data[1:]}
        with open("./inputs/EmpStackDetails.csv", "r") as csvfile:
            temp_data = [data for data in csv.reader(csvfile)]
            all_data["EmpStackDetails"] = {"headers": temp_data[:1],
                                           "data": temp_data[1:]}
        print "All Data Fetched from CSVs Successfully."
    except Exception as e:
        all_data["EmpDetails"] = {"headers":[],"data":[]}
        all_data["EmpSkills"] = {"headers":[],"data":[]}
        all_data["EmpStackDetails"] = {"headers":[],"data":[]}
        print "Error Occured While Reading the CSV files: "+e.message
    finally:
        return all_data


def get_db_connection():
    try:
        db = mysql.connect(
            host = "localhost",
            user = "jogeshg",
            passwd = "abcd1234",
            database = "assignmentdb"
        )
    except:
        db = None
        print "Error While Generating the DB Connection."
    finally:
        return db


def insert_all_data_to_tables(all_data):

    try:
        db = get_db_connection()
        empdetails_insert_query = "INSERT INTO Employee (EmpId, FirstName, \
                                                         LastName, CreatedById, \
                                                         LastUpdatedById, CreatedBy, \
                                                         LastUpdatedBy) \
                                                 VALUES (%s, %s, %s, %s, %s, %s, %s);"

        empskills_insert_query = "INSERT INTO EmployeeSkills (SkillName, EmpId) \
                                                      VALUES (%s, %s);"
    
        empstackdetails_insert_query = "INSERT INTO StackData (StackId, StackNickName, EmpId) \
                                                       VALUES (%s, %s, %s);"

        empdetails_insert_cursor = db.cursor()
        empskills_insert_cursor = db.cursor()
        empstackdetails_insert_cursor = db.cursor()
        
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


def fetch_saved_data_from_db():

    all_fetched_data = {}
    try:
        db = get_db_connection()
        
        empdetails_fetch_query = "SELECT * FROM Employee;"
        empskills_fetch_query = "SELECT SkillName,EmpId FROM EmployeeSkills;"
        empstackdetails_fetch_query = "SELECT * FROM StackData;"
        
        empdetails_fetch_cursor = db.cursor()
        empskills_fetch_cursor = db.cursor()
        empstackdetails_cursor = db.cursor()
        
        empdetails_fetch_cursor.execute(empdetails_fetch_query) 
        empdetails_records = empdetails_fetch_cursor.fetchall()
        final_empdetails_data = []
        for rec in empdetails_records:
            temp = []
            for aa in rec:
                temp.append(aa.encode("utf-8"))
            final_empdetails_data.append(tuple(temp))
        all_fetched_data["EmpDetails"] = final_empdetails_data
        
        empskills_fetch_cursor.execute(empskills_fetch_query)
        empskills_records = empskills_fetch_cursor.fetchall()
        final_empskills_data = []
        for rec in empskills_records:
            temp = []
            for aa in rec:
                temp.append(aa.encode("utf-8"))
            final_empskills_data.append(tuple(temp))
        all_fetched_data["EmpSkills"] = final_empskills_data
        
        empstackdetails_cursor.execute(empstackdetails_fetch_query)
        empstackdetail_records = empstackdetails_cursor.fetchall()
        final_empstackdetails_data = []
        for rec in empstackdetail_records:
            temp = []
            for aa in rec:
                temp.append(aa.encode("utf-8"))
            final_empstackdetails_data.append(tuple(temp))
        all_fetched_data["EmpStackDetails"] = final_empstackdetails_data
        
        print "All Data fetched from DB Successfully!"

    except Exception as e:
        all_fetched_data["EmpDetails"] = []
        all_fetched_data["EmpSkills"] = []
        all_fetched_data["EmpStackDetails"] = []
        print "Error While fetching data from DB: "+e.message

    finally:
        empdetails_fetch_cursor.close()
        empskills_fetch_cursor.close()
        empstackdetails_cursor.close()
        db.close()
        return all_fetched_data


if __name__ == "__main__":
    main()
