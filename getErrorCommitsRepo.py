import os
import git
import json
import sqlite3
from contextlib import closing

PROJECT = 'p1'

#builds and returns data rows (list of tuples) to insert in table
def buildCommit():
    data = []
    errorNum = 0
    repo.git.checkout(id, force=True)

    #get the errorviz version if it exists
    if '.errorviz-version' in os.listdir():
        with open('.errorviz-version') as file:
            errorvizVer = file.readline()
    else:
        errorvizVer = None

    #get rust version
    with open('rustc.version') as file:
        rustVer = file.readline().strip()

    #append 'mod repl_helper;' to lib.rs
    #special case for 407952
    if commitNum > 2028 and str(user) == '407952':
        with open('src/lib.rs', 'a') as file:
            file.write('\nmod repl_helper;')

    #run build and get json with first error
    os.system('cargo build --message-format=json > errorReport')
    with open('errorReport') as file:
        for line in file:
            if ('"reason":"compiler-message"' in line) and ('"level":"error"' in line) and not('aborting due to' in line and 'previous error' in line):
                #get error description from json
                errorJson = json.loads(line)
                completeMsg = json.dumps(errorJson["message"])
                error = errorJson["message"]["message"]

                #check if line number provided
                if errorJson["message"]["spans"] != None and errorJson["message"]["spans"] != []:
                    lineStart = errorJson["message"]["spans"][0]["line_start"]
                    lineEnd = errorJson["message"]["spans"][0]["line_end"]
                else:
                    lineStart = None
                    lineEnd = None

                #syntax errors don't have error codes
                if errorJson["message"]["code"] != None:
                    errCode = errorJson["message"]["code"]["code"]
                else:
                    errCode = None

                #append row to data list
                data.append((user, rustVer, errorvizVer, id, parent, commitNum, errorNum, completeMsg, error, errCode, lineStart, lineEnd, interval, timestamp))
                errorNum += 1

    #if no errors in message report, data contains no error
    if errorNum == 0:
        data.append((user, rustVer, errorvizVer, id, parent, commitNum, None, 'No Error', None, None, None, None, interval, timestamp))

    #remove report and return data    
    os.remove('errorReport')
    return data




#delete old database
if 'commitErrors.db' in os.listdir():
    os.remove('commitErrors.db')

#create db with headers
with closing(sqlite3.connect('commitErrors.db')) as connection:
    with closing(connection.cursor()) as cursor:
        cursor.execute("create table commits (user text, rustVer text, errorvizVer text, commitID hexadecimal, parentID hexadecimal, commitNumber integer, errorNum integer, completeMsg text, error text, errcode text, lineStart integer, lineEnd integer, interval integer, timestamp integer)")

        #iterate each user
        os.chdir('users')
        if '.DS_Store' in os.listdir():  
            os.remove('.DS_Store')
        users = os.listdir()
        for user in users:
            os.chdir(user)

            #iterate each commit in project
            repo = git.Repo(PROJECT)
            os.chdir(PROJECT)
            prevstamp = 0
            interval = 0
            commitNum = 0
            for commit in repo.iter_commits(reverse=True):

                #get timestamp and interval
                timestamp = commit.authored_datetime.timestamp()
                if prevstamp != 0:
                    interval = timestamp - prevstamp
                prevstamp = timestamp

                #checkout the commit and build using function, get errors
                id = commit.hexsha
                if commitNum != 0:
                    parent = commit.parents[0].hexsha
                else:
                    parent = None
                data = buildCommit()

                #insert data into db
                for row in data:
                    cursor.executemany("insert into commits values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (row,))
                connection.commit()

                #increment commit number
                commitNum += 1
                print(commitNum)
            #leave project and user folder
            os.chdir('../../')