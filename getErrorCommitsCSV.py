import json
import sqlite3
import csv

FILENAME = "test_query_data.csv"

with open(FILENAME, 'r') as file:
    csvFile = csv.reader(file)
    for lines in file:
        break
        # get column
        # for each line in column ('\n' separated)
        # will double-quotes effect json read? hopefully not
        # check for uuid and survey data to export into new file
        # recover LATEST log



# for error database:
# uuid | buildNum | seconds since last | revis used | error code | error msg hash 
# | filehash | source | start line | end line | range

# for survey database:
# uuid | codingExp | rustExp | linesOfCode | java | javascript | c | python
#  | rustBook | youtube | stackOverflow | other | industry | gender | age | degree