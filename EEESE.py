#!/usr/bin/python3
# ---------------------------------------------------------------------------------------------
# File Name: Extend Educators Subscriptions.py
# Version  : 1.0
# Date      : 2021 April 20
# Purpose  :

# ----------------------------------------------------------------------------------------------

import cx_Oracle
import os
import csv
import datetime
import logging
import random
import smtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# list to store fetched ACC data
tbaExtList = []
count_from_backuptable_sql = []


# database connection string
# conn_str = 'smsops/sms0p5u53r@dbs-b3-2504-vip:1521/smsprod_int'
conn_str = 'smsusr/smsn0tMS@dbs-b3-2103-vip:1521/smsppe_int'

# get last month in string format
today = datetime.date.today()
first = today.replace(day=1)
lastMonth = first - datetime.timedelta(days=1)
str_lastMonth = lastMonth.strftime("%B")

# get year
year = str(datetime.date.today().year)

# report path
report_path = "/export/home/users/opsjobs/EDU_SUBS_EXTEND/"

# create a logger object
logger = logging.getLogger('Edu_etext_subs_extend')

# create file handler
handler = logging.FileHandler(report_path + 'Edu_etext_subs_extend.log')

# create logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)
logger.setLevel(logging.INFO)
# sendmail function
def sendMail(subject,body):
    msg = MIMEMultipart()
    part = MIMEText(body, 'html')
    msg.attach(part)

    #msg['To'] = 'sajith.egodage@pearson.com'
    msg['To'] = 'chamara.methruwan@pearson.com'
   # msg['Cc'] =  'chamara.methruwan@pearson.com'
    msg['Subject'] = subject
    msg['From'] = 'donotreply@pearson.com'

    # Send the message via our own SMTP server.
    server = smtplib.SMTP('localhost')
    server.send_message(msg)
    server.quit()

#create a mail body for DB connection check
mail_body1 ="Hi All, <br/> <br/>"
mail_body1 += "Failed to run the Extend Educators Etext Subscription Extension Cronjob. please check on vm-ops07 <br/> <br/><br/> <br/>"

# get db time
try:
    conn = cx_Oracle.connect(conn_str)
    curs = conn.cursor()
    curs.callproc("DBMS_OUTPUT.enable", (None,))  # or explicit integer size
except cx_Oracle.DatabaseError as exception:
    logger.error('Failed to connect to Database', exc_info=True)
    sendMail("'Failed to connect to Database", mail_body1)
else:
    logger.info('Sucessfully connected  to SMS prod database when trying get db time')

# Function for success mail body
def createSuccessMessage(count_from_backuptable):
    html = html = '<html><head><style>table, th, td {border: 1px solid black; border-collapse: collapse;} th{ text-align: left;}</style></head><body><table style="width:50%">><th>'+str(count_from_backuptable[0])+'</th>'
    html += '</table></body></html>'
    return html

# connecting to smsprod database
try:
    logger.info('Connecting to SMS prod database')
    conn = cx_Oracle.connect(conn_str)
except cx_Oracle.DatabaseError as exception:
    logger.error('Failed to connect to Database', exc_info=True)
    sendMail("'Failed to connect to Database", mail_body1)
else:
    logger.info('Sucessfully connected  to SMS prod database')


hash = random.getrandbits(32)
print(today)
print(type(today))
backup_table_name = today.strftime("%m%d%Y")
backup_table_name = str('tablebackup' + backup_table_name)
print(type(today))
print(backup_table_name)
print (str(type(backup_table_name)))
#backup_table_name2=str('backup_table_name2')


##########################################################################################
# create backup table to insert subscriptions
# SQL Query to backup subscriptions

count_from_backuptable_sql = "select count(*) from SUBS_BACK_CHG0280424123 where HANDLED_STATE=1 "


sql1 = "CREATE TABLE "
sql2 = backup_table_name
sql3 = " AS SELECT SUBSCRIPTIONID, SMSUSERID, MODULEID , EXPIRATIONDATE, EXPIRED, sysdate as CREATIONDATE,0 as HANDLED_STATE, sysdate as MODIFIEDDATE FROM SUBSCRIPTION WHERE SUBSCRIPTIONID  IN ( SELECT SUBSCRIPTIONID FROM SUBSCRIPTIONROLES WHERE ROLEID =2 ) AND MODULEID IN (21707,21708,21709,21711)  AND EXPIRATIONDATE >= sysdate - 7 AND EXPIRATIONDATE <= sysdate + 7 "

insert_sql =  sql1 + sql2 + sql3

print (insert_sql)

update_sql = """
DECLARE
        my_count NUMBER;
        ERR_CODE NUMBER;
        ERR_MES VARCHAR2(256);
    start_time timestamp;
        temptime VARCHAR2(256);
    complete_time timestamp;
BEGIN

        my_count := 1;
        FOR i IN (select s.SUBSCRIPTIONID, s.MODULEID, s.EXPIRATIONDATE From SUBS_BACK_CHG0280424123 s WHERE HANDLED_STATE = 0)
        LOOP
                BEGIN
            -- Real Update happen
                        UPDATE SUBSCRIPTION
                        SET EXPIRED = 'N', EXPIRATIONDATE = i.EXPIRATIONDATE + 730
                        WHERE SUBSCRIPTIONID = i.SUBSCRIPTIONID
                        AND MODULEID = i.MODULEID;

                        --update handled state and modified date
                        UPDATE SUBS_BACK_CHG0280424123 SET HANDLED_STATE = 1, MODIFIEDDATE = sysdate
                        WHERE SUBSCRIPTIONID = i.SUBSCRIPTIONID;

                        my_count := my_count + 1;
                        DBMS_OUTPUT.PUT_LINE(i.SUBSCRIPTIONID);

                        IF (my_count > 500)
                        THEN
                                COMMIT;
                                DBMS_OUTPUT.PUT_LINE('Committed 500 records');
                                my_count := 1;
                        END IF;

                        EXCEPTION WHEN OTHERS THEN
                                ROLLBACK;
                                ERR_CODE := SQLCODE;
                                ERR_MES := SUBSTR(SQLERRM, 1,256);
                                DBMS_OUTPUT.PUT_LINE('ERR_CODE : ' || ERR_CODE || ' -- ERR_MES : ' || ERR_MES);
                END;

        END LOOP;
        COMMIT;
    select LOCALTIMESTAMP into complete_time from DUAL;
    DBMS_OUTPUT.PUT_LINE( 'PL/SQL Completed Time : ' || complete_time);
END;
"""
##########################################################################################

try:
    logger.info('create table')
    curs.execute(insert_sql)
except cx_Oracle.DatabaseError as exception:
    logger.error('Failed to execute sql query', exc_info=True)
else:
    logger.info('Finished Executing SQL query')

try:
    logger.info('Started Backing up Data')
    curs.execute(count_from_backuptable_sql)
    count_from_backuptable = curs.fetchall()
except cx_Oracle.DatabaseError as exception:
    logger.error('Failed to execute insert sql query', exc_info=True)
else:
     logger.info('Finished Executing insert SQL query')

#Create mail body
mail_body ="Hi All, <br/> <br/>"
body_html_all_transactions =  createSuccessMessage(count_from_backuptable)
mail_body += "Please find the stats for transactions created on or after 8/17/2020 00:00:00 (EST). <br/> <br/>"+ str(body_html_all_transactions) + "<br/> <br/>"
sendMail("Course Creation Hourly Stats", mail_body)
print(year)

# closing database connection
logger.info('Closing database connection')
curs.close()
conn.close()
logger.info('Database connection is closed')
