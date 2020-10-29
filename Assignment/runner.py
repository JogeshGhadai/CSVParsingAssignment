import csv
import mysql.connector as mysql


def main():
    
    # import pdb;pdb.set_trace()
    all_data = None
    with open("./Test - Parse Sheet.csv", "r") as csvfile:
        all_data = csv.reader(csvfile)
        all_data = [data for data in all_data]
    #print(all_data)
    #print "################"
    headers = all_data[:1]
    vals = all_data[1:]
    print(headers)
    print(vals)
    #for data in all_data:
    #    print(get_or_str(data[1]))
    #with open("./test_out.csv", 'w') as csvfile:
    #    dataWriter = csv.writer(csvfile)
    #    dataWriter.writerow(all_data)
    db = mysql.connect(
        host = "localhost",
        user = "jogeshg",
        passwd = "abcd1234",
        database = "assignmentdb" 
    )
    cursor = db.cursor()
    cursor1 = db.cursor()
    cursor2 = db.cursor()
    cursor.execute("SELECT * FROM AllData;")
    records = cursor.fetchall()
    #print [rec[0] for rec in records]
    print("#####################")
    #for tup in all_data[1:]:
    print("$$$$$$$$$$$$$")
    #vals = [[i.encode("utf-8") for i in x] for x in vals]
    #import pdb;pdb.set_trace()
    #final_vals = []
    #for rec in vals:
    #    tt = []
    #    for aa in rec:
    #        tt.append(aa.encode("utf-8"))
    #    final_vals.append(tuple(tt))
    #print final_vals
    query = "INSERT INTO AllData (EmpId, \
                                  FirstName, \
                                  LastName, \
                                  Skill1, \
                                  Skill2, \
                                  Skill3, \
                                  Skill4, \
                                  Skill5, \
                                  StackId, \
                                  StackNickName, \
                                  CreatedById, \
                                  LastUpdatedById, \
                                  CreatedBy, \
                                  LastUpdatedBy) \
                                  VALUES (%s, %s, %s, %s, \
                                          %s, %s, %s, %s, \
                                          %s, %s, %s, %s, \
                                          %s, %s);"

        #query = "INSERT INTO AllData VALUES ();"
    #print "##################"
    #print all_data[2][1]
    #print all_data[4][2]
    #print all_data[4][9]
    #print get_or_str(all_data[3][1])
    #print get_or_str(all_data[5][2])
    #print get_or_str(all_data[5][9])
    #values = [("BB",get_or_str(all_data[3][1]),),("BB",get_or_str(all_data[5][2]),),("BB",get_or_str(all_data[5][9]),), ("BB",get_or_str(all_data[4][9]),)]
        #values = [tuple([get_or_str(y) for y in x]) for x in all_data[1:]]
    #import pdb;pdb.set_trace()
        #values = tup[:-6]+[int(tup[-6])]+[tup[-5]]+[int(a) for a in tup[-4:-2]]+tup[-2:]
    #values = final_vals
    values = vals
    print(values)
    #import pdb;pdb.set_trace()
    #values = [(get_or_str(all_data[2][1]),),(get_or_str(all_data[4][2]),),(get_or_str(all_data[4][9]),)]
    cursor1.executemany(query, values)
    print("##Inserted :",values)
    db.commit()
    cursor2.execute("SELECT * FROM AllData;")
    records = cursor.fetchall()
    print("##########")
    #for record in records:
    #    print([r.decode("utf-8") for r in record])
    #records = [[x.decode('utf-8') for x in r] for r in records]
    print(records)
    print("@@@@@@@@@@@@@")
    final_records = []
    for rec in records:
        tt = []
        for aa in rec:
            tt.append(aa.encode("utf-8"))
        final_records.append(tuple(tt))
    print "Final: ", final_records
    with open("./test_out.csv", "w") as out_file:
        writer = csv.writer(out_file)
        writer.writerows(headers)
        writer.writerows(final_records)
        
    #print records
    


def get_or_str(st):
    res = ""
    try:
        res = str(st.encode('utf-8'))
    except:
        res = str(st)
    return res


if __name__ == "__main__":
    main()
