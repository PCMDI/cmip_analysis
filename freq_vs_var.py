#!/usr/local/python

from SECRET import get_user
from SECRET import get_pw
from SECRET import get_db
from SECRET import get_host
import mysql.connector

connection = mysql.connector.connect(user=get_user(), database=get_db(), host=get_host(), password=get_pw())

# ---
cursor_freq = connection.cursor()
query_freq =  "SELECT p.publication_id as pid, cv.attribute_value as cva \
FROM publication p \
JOIN publication_has_attribute pha ON p.publication_id = pha.publication_id \
JOIN attribute ON pha.attribute_id = attribute.attribute_id \
JOIN controlled_vocabulary cv ON cv.controlled_vocabulary_id = attribute.attribute_value \
WHERE attribute.attribute_key = 'frequency';"

cursor_freq.execute(query_freq)
dict_freq = {}
count_freq = 0
for (pid, cva) in cursor_freq:
    dict_freq[pid] = cva
    count_freq += 1

print len(dict_freq)
print count_freq

# ---

print "# ---"

# ---

cursor_var = connection.cursor()
query_var = "SELECT p.publication_id as pid, cv.attribute_value as cav \
FROM publication p \
JOIN publication_has_attribute pha ON p.publication_id = pha.publication_id \
JOIN attribute ON pha.attribute_id = attribute.attribute_id \
JOIN controlled_vocabulary cv ON cv.controlled_vocabulary_id = attribute.attribute_value \
WHERE attribute.attribute_key = 'variable';"

cursor_var.execute(query_var)
dict_var = {}
count_var = 0
for (pid, cva) in cursor_var:
    dict_var[pid] = cva
    count_var += 1

print len(dict_var)
print count_var

# ---

print "# ---"
print "BAM"
