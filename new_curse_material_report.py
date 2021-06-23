#!/usr/bin/python3

# 2017-10-18
# Last Update 2017-10-30 : V-2
# Authored by Aruna Dharmathilaka

import csv
import cx_Oracle
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime

dt = str(datetime.now())
email_sender = "donotreply@pearson.com"
subject="New Course Materials created on : "
body="Hi Team,\n\n\nPlease find the attachment below which consist of the newly created materials.\n\n\nIf you have any concerns please get back to us. \n\nRegards,\nOTS Team"

to_list="websiterelease@pearson.com"
#to_list="aruna.dharmathilaka@pearson.com"
cc_list="megan.higginbotham@pearson.com,sl-rhine-team@pearson.com,aruna.dharmathilaka@pearson.com,PE-SL-tier2-ops-admin@pearson.com"
#cc_list="aruna.dharmathilaka@pearson.com"

failure_subject="Failure in Aruna Partner TETS Report execution"
failure_to="aruna.dharmathilaka@pearson.com"
failure_cc="PE-SL-tier2-ops-admin@pearson.com"
#failure_to="aruna.dharmathilaka@pearson.com"
#failure_cc="aruna.dharmathilaka@pearson.com"
failure_body = "Hi OTS Team,\nError occured While processing the Report\nReprted error as below"

def setup_db ():
    #Establish the connection with CATALOG
    connstr='catalog/in44tram@dbs-b3-2701-vip:1521/cat11prd.pearsonltg.com'
    #connstr='catalog/catppe9@dbs-b3-0201:1521/cat11ppe.pearsonltg.com'
    conn = cx_Oracle.connect(connstr)

    #curs = conn.cursor()

    return conn

def db_execute (dbconn):
    curs = dbconn.cursor()
    curs.execute('SELECT course_material_version_id, master_course_id,course_material_id,active,date_created FROM catalog.course_material_version WHERE date_created between sysdate-1 and sysdate')
    data=curs.fetchall()

    print ("DB connection Close")
    curs.close()
    dbconn.close() #Close the connection with Portal DB

    return data

def csvfilecreate (outputrows):

    if (len(outputrows) == 0):

        print (">>>> DB ROWS ARE ZERO <<<<<")
        #newname = "no_data.csv"
        #print (newname)
        fp = open('./file.csv', 'w')
        myFile = csv.writer(fp, lineterminator='\n')
        myFile.writerow(["MATERIAL VERSION ID", "MATER COURSE ID", "COURSE MATERIAL ID", "ACTIVE", "DATE CREATED"])
        myFile.writerows(outputrows)

        myFile.writerow(["No data retrived for today execution !!! "])

        fp.close()

        newname = 'No_data_file_'+dt+'.csv'

        os.rename('file.csv', newname)

        return newname

    else:

        print (">>>> DB ROWS ARE NOT ZERO <<<<<")

        fp = open('./file.csv', 'w')
        myFile = csv.writer(fp, lineterminator='\n')
        myFile.writerow(["MATERIAL VERSION ID", "MATER COURSE ID", "COURSE MATERIAL ID", "ACTIVE", "DATE CREATED"])
        myFile.writerows(outputrows)
        fp.close()

        for row in outputrows:
            course_material_version_id = row[0]
            active = row[1]
        #print (course_material_version_id)
        #print (active)

        #f.writerow([str(row)])
        newname = 'file_'+dt+'.csv'
        os.rename('file.csv', newname)

        return newname

def sendMail(subject,body,attachement_name, to_list, cc_list):
    msg = MIMEMultipart()
    part = MIMEText(body)
    msg.attach(part)

    print (">>>> SUbject adding date <<<<<")
    today = datetime.now()
    formattedtoday = today.strftime("%x")

    #print (today.strftime("%x"))
    print (formattedtoday)

    print (subject + formattedtoday)

    newsubject = subject + formattedtoday

    print (newsubject)

    msg['Subject'] = newsubject
    msg['From'] = email_sender
    msg['To'] = to_list

    if (cc_list is not None):
        msg['Cc'] = cc_list

    if (attachement_name is not None):
        part = MIMEApplication(open(attachement_name,"rb").read())
        part.add_header('Content-Disposition', 'attachment', filename=attachement_name)
        msg.attach(part)

    #Send the message via our own SMTP server.
    server = smtplib.SMTP('localhost')
    server.send_message(msg)
    server.quit()

try:
    dbconn = setup_db ()
    outputrows = db_execute (dbconn)
    reportname = csvfilecreate (outputrows)
    attachement_name="{}".format(reportname)

    print ("--------------------")
    print (reportname)
    print ("--------------------")
    attachement_name="{}".format(reportname)
    print (attachement_name)

except Exception  as err:

    print ("DB Connection issue !!!")

try:
    print ("Second part--------------")
    print (attachement_name)

    print ("Sending mail part")
    sendMail(subject ,body ,attachement_name, to_list, cc_list)

except Exception  as err:

    print ("Mail issue !!!")
