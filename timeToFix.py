import os
import sqlite3
from contextlib import closing

#access the db, for each commit number, record the error in a list
#when the error is gone in a commit, find the difference in timestamps
#how to address multiple of the same error? I'm just gonna record unique errors

lastCommit = None
lastTimestamp = None
#list of errors codes from one commit
errorList = []
#recompilation of the database where errors are stored in errorList per commit
#(errorList, timestamp, commit num)
reformedDb = []

#"queue" of errors that have yet to be resolved
fixQueue = []
#database of (error code, time to fix)
fixesDb = []

#dictionary of average time to fix error code : (num errors, total time, avg time)
avgDb = {}
#listified version
avgDbList = []

with closing(sqlite3.connect('commitErrors-final.db')) as connection:
    with closing(connection.cursor()) as cursor:
        db = cursor.execute("SELECT errcode, timestamp, commitNumber FROM commits").fetchall()

        #recompile the database so that all errors are in one row per commit
        for row in db:
            if row[2] == lastCommit:
                errorList.append(row[0])
            else:
                #create entry
                reformedDb.append((errorList.copy(), lastTimestamp, lastCommit))
                #reset and add next error
                lastTimestamp = row[1]
                lastCommit = row[2]
                errorList.clear()
                errorList.append(row[0])
        
        #account for last entry
        reformedDb.append((errorList, lastTimestamp, lastCommit))

        #fill new database
        for row in reformedDb:
            #reset queue on new user
            if row[2] == 0:
                fixQueue.clear()

            #check if errors have been fixed -> add to db
            for item in fixQueue.copy():
                if item[0] not in row[0]:
                    fixesDb.append((item[0], row[1] - item[1]))
                    fixQueue.remove(item)
                    
            #looks for new errors -> add to queue
            for error in row[0]:
                if error != None:
                    if fixQueue == [] or error not in list(zip(*fixQueue))[0]:
                        fixQueue.append((error, row[1]))

        #remove syntax errors and time outliers
        for row in fixesDb.copy():
            if row[1] > 1000:
                fixesDb.remove(row)
            
        cursor.execute("DROP TABLE timeToFixDistribution")
        cursor.execute("CREATE TABLE timeToFixDistribution (errcode TEXT, time INTEGER)")
        for row in fixesDb:
                cursor.executemany("INSERT INTO timeToFixDistribution VALUES (?, ?)", (row,))  
        connection.commit()

        #calculate average time to fix
        for row in fixesDb:
            #update count, total time, average time
            if row[0] in avgDb:
                avgDb[row[0]] = (avgDb[row[0]][0]+1, avgDb[row[0]][1]+row[1], avgDb[row[0]][0]+1/avgDb[row[0]][1]+row[1])
            else:
                avgDb[row[0]] = (1, row[1], row[1])

        #convert dict into list for easy adding to table
        for key in avgDb:
            avgDbList.append((key, *avgDb[key]))


        cursor.execute("DROP TABLE timeToFixTotals")
        cursor.execute("CREATE TABLE timeToFixTotals (errcode TEXT, count INTEGER, total time INTEGER, average time DOUBLE)")
        for row in avgDbList:
                cursor.executemany("INSERT INTO timeToFixTotals VALUES (?, ?, ?, ?)", (row,))  
        connection.commit()
