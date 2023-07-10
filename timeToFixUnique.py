import os
import sqlite3
from contextlib import closing

lastCommit = 0
lastTimestamp = 0
lastUser = 0
#list of (error, errcode) from one commit
errorList = []
#recompilation of the database where errors are stored in errorList per commit
#(errorList, timestamp, commit num, errorcode)
newDB = []

#dictionary of fixes (error msg) : (errcode, time, user, commitnum)
#temp fix dict
session = {}
#list to be put into the db (user, errormsg, errorcode, time, commitnum)
fixes = []

#dictionary of average time to fix error code : (num errors, total time, avg time)
avgDb = {}
#listified version
avgDbList = []

with closing(sqlite3.connect('commitErrors-final.db')) as connection:
    with closing(connection.cursor()) as cursor:
        db = cursor.execute("SELECT error, errcode, timestamp, commitNumber, user FROM commits").fetchall()
        
        #recompile the database so that all errors are in one row per commit
        for row in db:
            if row[3] == lastCommit:
                errorList.append((row[0], row[1]))
            else:
                #create entry
                newDB.append((errorList.copy(), lastTimestamp, lastCommit, lastUser))
                #reset and add next error
                lastTimestamp = row[2]
                lastCommit = row[3]
                lastUser = row[4]
                errorList.clear()
                errorList.append((row[0], row[1]))
        
        #account for last entry
        newDB.append((errorList, lastTimestamp, lastCommit, lastUser))

        #fill list of resolution sessions
        for i in range(len(newDB)):
            if i < len(newDB)-1:
                row = newDB[i]
                interval = newDB[i+1][1] - newDB[i][1]

                #reset fix session on new user
                if row[2] == 0:
                    session.clear()

                #check if errors have been fixed -> add to db
                for key in session.copy():
                    if key not in list(zip(*row[0]))[0]:
                        fixes.append((session[key][2], key, session[key][0], session[key][1], session[key][3]))
                        session.pop(key)
                
                #clear session if above time limit
                if interval > 1500:
                    session.clear()
                #looks for new errors -> add to queue
                else:
                    #weighted time allocation
                    timeAdd = interval / len(row[0])
                    for error in row[0]:
                        errmsg = error[0]
                        if errmsg != "No Error":
                            #create new entry if not in session
                            if session == {} or errmsg not in session:
                                session[errmsg] = (error[1], timeAdd, row[3], row[2])
                            #update time in session
                            else:
                                session[errmsg] = (session[errmsg][0], session[errmsg][1] + timeAdd, session[errmsg][2], session[errmsg][3])

        cursor.execute("DROP TABLE weightedTimeToFix")
        cursor.execute("CREATE TABLE weightedTimeToFix (user TEXT, error TEXT, errCode TEXT, time DOUBLE, commitNum INTEGER)")
        for row in fixes:
                cursor.executemany("INSERT INTO weightedTimeToFix VALUES (?, ?, ?, ?, ?)", (row,))  
        connection.commit()

        #calculate average time to fix
        for row in fixes:
            #update count, total time
            errcode = row[2]
            if errcode in avgDb:
                avgDb[errcode] = (avgDb[errcode][0] + 1, avgDb[errcode][1] + row[3])
            #create new entry
            else:
                avgDb[errcode] = (1, row[3])

        #calc average + convert dict into list for easy adding to table
        for errcode in avgDb:
            entry = avgDb[errcode]
            avgDb[errcode] = (entry[0], entry[1], entry[1]/entry[0])
            avgDbList.append((errcode, *avgDb[errcode]))

        cursor.execute("DROP TABLE averageFixPerCode")
        cursor.execute("CREATE TABLE averageFixPerCode (errcode TEXT, count INTEGER, total time INTEGER, average time DOUBLE)")
        for row in avgDbList:
                cursor.executemany("INSERT INTO averageFixPerCode VALUES (?, ?, ?, ?)", (row,)) 
        connection.commit()
