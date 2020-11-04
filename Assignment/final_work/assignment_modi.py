import csv
import yaml
import logging
import mysql.connector as mysql


def main():
    try:
        all_data = fetct_all_csv_data()
        db_connection = get_db_connection()
        db_cur = db_connection.cursor()
        insert_all_data_to_tables(all_data, db_cur)
        db_connection.commit()
        all_fetched_data = fetch_saved_data_from_db(db_cur)
        write_fetched_data_to_csv(all_data, all_fetched_data)
    except Exception as e:
        logging.error("Error Occured in main flow: "+e.message)
    finally:
        if db_cur is not None:
            db_cur.close()
        if db_connection is not None:
            db_connection.close()


def fetct_all_csv_data():
    """
    It fetches the data present in Inputs folder CSV Files 
    and returnsthem in a dictionary format.
    
    Returns
    -------
    dict
    """
    
    all_data = {}
    
    try:
        with open("./inputs/EmpDetails.csv", "r") as csvfile:
            temp_data = [data for data in csv.reader(csvfile)]
            all_data["EmpDetails"] = {"headers": temp_data[:1],
                                      "data": temp_data[1:]}
        with open("./inputs/EmpSkills.csv", "r") as csvfile:
            temp_data = [data for data in csv.reader(csvfile)]
            final_data = []
            for skill_name,emp_id in temp_data[1:]:
                final_data.append(tuple([lower_and_remove_punctuation(skill_name), emp_id]))
            all_data["EmpSkills"] = {"headers": temp_data[:1],
                                     "data": final_data}
        with open("./inputs/EmpStackDetails.csv", "r") as csvfile:
            temp_data = [data for data in csv.reader(csvfile)]
            all_data["EmpStackDetails"] = {"headers": temp_data[:1],
                                           "data": temp_data[1:]}
        logging.info("All Data Fetched from CSVs Successfully.")
    
    except Exception as e:
        all_data["EmpDetails"] = {"headers":[],"data":[]}
        all_data["EmpSkills"] = {"headers":[],"data":[]}
        all_data["EmpStackDetails"] = {"headers":[],"data":[]}
        logging.error("Error Occured While Reading the CSV files: "+e.message)
    
    finally:
        return all_data


def lower_and_remove_punctuation(word):
    """
    It Removes Punctuations and Lowers the given word.
    
    Parameters
    ----------
    word : str
        The word to be modified

    Returns
    -------
    str
    """

    res = ""
    punctuations = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''
    
    for le in word:
        if le not in punctuations:
            res += le
    return res.lower()


def get_db_connection():
    """
    It Creates a MySQL DB Connection and returns the db connection object.
    
    Returns
    -------
    mysql.connect
    """

    try:
        with open("./config.yaml") as f:
            mysql_creds = yaml.load(f, Loader=yaml.FullLoader)["mysql"]

        db = mysql.connect(
            host = mysql_creds["host"],
            user = mysql_creds["user"],
            passwd = mysql_creds["passwd"],
            database = mysql_creds["db"]
        )
    
    except:
        db = None
        logging.error("Error While Generating the DB Connection.")
    
    finally:
        return db


def insert_all_data_to_tables(all_data, db_cursor):
    """
    It inserts the data present in the dictionary to the 
    respective MySQL Tables.
    
    Parameters
    ----------
    all_data : dict
        The dictionary which is used to write data to MySQL DB.
    """

    try:
        empdetails_insert_query = "INSERT INTO Employee (EmpId, FirstName, \
                                                         LastName, CreatedById, \
                                                         LastUpdatedById, CreatedBy, \
                                                         LastUpdatedBy) \
                                                 VALUES (%s, %s, %s, %s, %s, %s, %s);"

        empskills_insert_query = "INSERT INTO EmployeeSkills (SkillName, EmpId) \
                                                      VALUES (%s, %s);"
    
        empstackdetails_insert_query = "INSERT INTO StackData (StackId, StackNickName, EmpId) \
                                                       VALUES (%s, %s, %s);"

        db_cursor.executemany(empdetails_insert_query, 
                              all_data["EmpDetails"]["data"])
        db_cursor.executemany(empskills_insert_query, 
                              all_data["EmpSkills"]["data"])
        db_cursor.executemany(empstackdetails_insert_query,
                              all_data["EmpStackDetails"]["data"])

        logging.info("Data Saved to DB Successfully!")

    except Exception as e:
        logging.error("Error Occured While Saving Data into DB: "+e.message)
    

def fetch_saved_data_from_db(db_cursor):
    """
    It fetched the data saved in the MySQL DB and returns 
    in the form of a Dictionary.

    Returns
    -------
    dict
    """

    all_fetched_data = {}
    
    try:
        empdetails_fetch_query = "SELECT \
                                    EmpId, \
                                    FirstName, \
                                    LastName, \
                                    CreatedById, \
                                    LastUpdatedById, \
                                    CreatedBy, \
                                    LastUpdatedBy \
                                  FROM \
                                  Employee;"
        empskills_fetch_query = "SELECT \
                                   SkillName, \
                                   EmpId \
                                 FROM \
                                 EmployeeSkills;"
        empstackdetails_fetch_query = "SELECT \
                                         StackId, \
                                         EmpId, \
                                         StackNickName \
                                       FROM \
                                       StackData;"
        
        db_cursor.execute(empdetails_fetch_query) 
        empdetails_records = db_cursor.fetchall()
        final_empdetails_data = []
        
        for rec in empdetails_records:
            temp = []
            for aa in rec:
                temp.append(aa.encode("utf-8"))
            final_empdetails_data.append(tuple(temp))
        all_fetched_data["EmpDetails"] = final_empdetails_data
        
        db_cursor.execute(empskills_fetch_query)
        empskills_records = db_cursor.fetchall()
        final_empskills_data = []
        
        for rec in empskills_records:
            temp = []
            for aa in rec:
                temp.append(aa.encode("utf-8"))
            final_empskills_data.append(tuple(temp))
        all_fetched_data["EmpSkills"] = final_empskills_data
        
        db_cursor.execute(empstackdetails_fetch_query)
        empstackdetail_records = db_cursor.fetchall()
        final_empstackdetails_data = []
        
        for rec in empstackdetail_records:
            temp = []
            for aa in rec:
                temp.append(aa.encode("utf-8"))
            final_empstackdetails_data.append(tuple(temp))
        all_fetched_data["EmpStackDetails"] = final_empstackdetails_data
        
        logging.info("All Data fetched from DB Successfully!")

    except Exception as e:
        all_fetched_data["EmpDetails"] = []
        all_fetched_data["EmpSkills"] = []
        all_fetched_data["EmpStackDetails"] = []
        logging.error("Error While fetching data from DB: "+e.message)

    finally:
        return all_fetched_data


def write_fetched_data_to_csv(headers_data, all_data):
    """
    It writes the Data to CSV files.

    Parameters
    ----------
    headers_data : dict
        It contains the header names required for writing in CSVs.
    all_data : dict
        It contains all the data except the Headers of for the CSV.
    """
    
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

        logging.info("Data written successfully to CSV.")

    except Exception as e:
        logging.error("Error while writing data to CSV: "+e.message)


if __name__ == "__main__":
    main()
