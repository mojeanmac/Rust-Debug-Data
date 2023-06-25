import os
import git
import json
import sqlite3
from contextlib import closing

PROJECT = 'p1'

#builds and returns data row (tuple) to insert in table
def buildCommit(id, parent, commitNum, interval, timestamp):
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

    #run build and get json with first error
    os.system('cargo build --message-format=json > errorReport')
    firstErr = None
    with open('errorReport') as file:
        for line in file:
            if '"reason":"compiler-message"' in line and '"level":"error"' in line:
                firstErr = line
                break
    
    #get error code and desc from json
    if firstErr != None:
        firstErrJSON = json.loads(firstErr)
        if firstErrJSON["message"] != None:
            error = firstErrJSON["message"]["message"]
            if firstErrJSON["message"]["code"] != None:
                errCode = firstErrJSON["message"]["code"]["code"]
            else:
                errCode = None
        else:
            error = None
            errCode = None
    else:
        error = "No Error"
        errCode = "No Error"

    #return data
    return (user, id, parent, commitNum, rustVer, errorvizVer, error, errCode, interval, timestamp)

#delete old database
os.system("rm commitErrors.db")

#create db with headers
with closing(sqlite3.connect('commitErrors.db')) as connection:
    with closing(connection.cursor()) as cursor:
        cursor.execute("create table commits (user text, commitID hexadecimal, parentID hexadecimal, commitNumber integer, rustVer text, errorvizVer text, error text, errcode text, interval integer, timestamp integer)")

        #iterate each user
        os.chdir('users')
        os.system('rm .DS_Store')
        users = os.listdir()
        for user in users:
            os.chdir(user)

            #iterate each commit in project
            repo = git.Repo(PROJECT)
            os.chdir(PROJECT)
            prevstamp = 0
            interval = 0
            commitNum = 0
            for commit in repo.iter_commits('main', reverse=True):

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
                data = buildCommit(id, parent, commitNum, interval, timestamp)

                #insert data into db
                cursor.executemany("insert into commits values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (data,))
                connection.commit()

                #increment commit number
                commitNum += 1
                print(commitNum)
            #leave project and user folder
            os.chdir('../../')