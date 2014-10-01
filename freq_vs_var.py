#!/usr/local/python

from SECRET import get_user
from SECRET import get_pw
from SECRET import get_db
from SECRET import get_host
import mysql.connector

# ---
# database connection
# ---

connection = mysql.connector.connect(user=get_user(), database=get_db(), host=get_host(), password=get_pw())

# ---
# frequencys
# ---

cursor_freq = connection.cursor()
query_freq =  "SELECT p.publication_id as pid, cv.attribute_value as cva \
FROM publication p \
JOIN publication_has_attribute pha ON p.publication_id = pha.publication_id \
JOIN attribute ON pha.attribute_id = attribute.attribute_id \
JOIN controlled_vocabulary cv ON cv.controlled_vocabulary_id = attribute.attribute_value \
WHERE attribute.attribute_key = 'frequency';"

cursor_freq.execute(query_freq)

frequencies = {}

for (pid, cva) in cursor_freq:
    if cva in frequencies:
        frequencies[cva].append(pid)
    else:
        frequencies[cva] = [pid]


# ---
# variables
# ---

cursor_var = connection.cursor()
query_var = "SELECT p.publication_id as pid, cv.attribute_value as cav \
FROM publication p \
JOIN publication_has_attribute pha ON p.publication_id = pha.publication_id \
JOIN attribute ON pha.attribute_id = attribute.attribute_id \
JOIN controlled_vocabulary cv ON cv.controlled_vocabulary_id = attribute.attribute_value \
WHERE attribute.attribute_key = 'variable';"

cursor_var.execute(query_var)

project_variables = {}
variable_projects = {}

for (pid, cva) in cursor_var:
    if cva in variable_projects:
        variable_projects[cva].append(pid)
    else:
        variable_projects[cva] = [pid]


    if pid in project_variables:
        project_variables[pid].append(cva)
    else:
        project_variables[pid] = [cva]


totals = {}

for freq in frequencies:
    totals[freq] = {}
    for pid in frequencies[freq]:
        if pid in project_variables:
            for var in project_variables[pid]:
                if var in totals[freq]:
                    totals[freq][var].append(pid)
                else:
                    totals[freq][var] = [pid]

# ---
# experiments
# ---

cursor_experiment = connection.cursor()
query_experiment  = """SELECT p.publication_id as pid, cv.attribute_value as cav 
FROM publication p
JOIN publication_has_attribute pha ON p.publication_id = pha.publication_id
JOIN attribute ON pha.attribute_id = attribute.attribute_id
JOIN controlled_vocabulary cv ON cv.controlled_vocabulary_id = attribute.attribute_value
WHERE attribute.attribute_key = 'experiment';"""

cursor_experiment.execute(query_experiment)

project_experiments = {}

for (pid, cva) in cursor_experiment:
    if pid in project_experiments:
        project_experiments[pid].append(cva)
    else:
        project_experiments[pid] = [cva]


# ---
# save to file
# ---

from csv import DictWriter

with open("cmip.csv", "w") as f:
    writer = DictWriter(f, ["frequency", "variable", "count", "experiments"])
    writer.writeheader()
    for freq in totals:
        for var in totals[freq]:
            experiments = set()
            for pid in totals[freq][var]:
                if pid not in project_experiments:
                    continue
                for expr in project_experiments[pid]:
                    experiments.add(expr)

            writer.writerow({"frequency":freq, "variable":var, "count":len(totals[freq][var]), "experiments":", ".join(experiments)})

