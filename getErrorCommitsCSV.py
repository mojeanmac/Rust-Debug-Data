import json
import sqlite3
import pandas as pd
import numpy as np
import os
from contextlib import closing

FILENAME = "csv/10.4.23data.csv"
IGNOREIDS = ["05b58d80f0317560861afb75e6b4ed62"]

with open(FILENAME, 'r') as file:
    df = csvFile = pd.read_csv(file)
    data = df["Data"]
    survey = {}
    headers = []
    for log in data:
        lines = log.split('\n')
        if lines[0] not in headers:
            headers.append(lines[0])
            headerjson = json.loads(lines[0])
            uuid = headerjson["uuid"]
            if uuid in IGNOREIDS:
                continue
            for line in lines:
                if "survey" in line:
                    survey[uuid] = line


#delete old database
if 'survey.db' in os.listdir():
    os.remove('survey.db')

with closing(sqlite3.connect('survey.db')) as connection:
    with closing(connection.cursor()) as cursor:
        cursor.execute("create table survey (uuid text, codingExp integer, rustExp text, linesOfCode text, java text, javascript text, c text, python text, rustBook boolean, youtube boolean, stackOverflow boolean, other boolean, industry text, gender text, age text, education integer)")

        for uuid, response in survey.items():
            resp = json.loads(response)["survey"]
            codingExp = resp["experience"]["codingExp"]
            rustExp = resp["experience"]["rustExp"]
            match rustExp:
                case "0":
                    rustExp = "1 month"
                case "1":
                    rustExp = "1 year"
                case "2":
                    rustExp = "2 years"
                case "3":
                    rustExp = "3 years"
                case "4":
                    rustExp = "5+ years"
            linesOfCode = resp["experience"]["linesOfCode"]
            match linesOfCode:
                case "0":
                    linesOfCode = "<100"
                case "1":
                    linesOfCode = "100-1000"
                case "2":
                    linesOfCode = "1000-10000"
                case "3":
                    linesOfCode = "10000-100000"
                case "4":
                    linesOfCode = ">100000"
            java = resp["languages"]["java"]
            javascript = resp["languages"]["javascript"]
            c = resp["languages"]["c"]
            python = resp["languages"]["python"]
            rustBook = resp["resources"]["rustBook"]
            youtube = resp["resources"]["youtube"]
            stackOverflow = resp["resources"]["stackOverflow"]
            other = resp["resources"]["other"]
            industry = resp["demographics"]["industry"]
            gender = resp["demographics"]["gender"]
            match gender:
                case "0":
                    gender = "woman"
                case "1":
                    gender = "man"
                case "2":
                    gender = "non-binary"
                case "3":
                    gender = "prefer not to answer"
            age = resp["demographics"]["age"]
            match age:
                case "0":
                    age = "18-22"
                case "1":
                    age = "23-30"
                case "2":
                    age = "31-40"
                case "3":
                    age = "41-50"
                case "4":
                    age = "51-60"
                case "5":
                    age = "60+"
            degree = resp["demographics"]["degree"]

            tuple = (uuid, codingExp, rustExp, linesOfCode, java, javascript, c, python, 
                     rustBook, youtube, stackOverflow, other, industry, gender, age, degree)

            cursor.executemany("insert into survey values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (tuple,))
        connection.commit()



# get column
# for each line in column ('\n' separated)
# check for uuid and survey data to export into new file
# check for studyenabled/disabled in extension reload = revis enabled/disabled
# recover LATEST log


# for error database:
# uuid | buildNum | seconds since last | revis used | error code | error msg hash 
# | filehash | source | start line | end line | range