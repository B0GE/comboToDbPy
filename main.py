import sys
import threading
import time
import configparser
from mysql.connector import MySQLConnection, Error
from mysql_dbconfig import read_db_config
#setting vars 
targetFile = "dummyCombo.txt"

#Setting up configparser, so the script knows what to do
botConfig = configparser.ConfigParser()
botConfig.sections()
botConfig.read('botConf.ini')
#########################################################

max_inserts = int(botConfig["main"]["max_inserts"])

class insertToDb(threading.Thread):
    def __init__(self,id,data):
        threading.Thread.__init__(self)
        self.id=id
        self.data=data

    def run(self):
        print(f"Starting Thread {self.id} for Insert")
        print(self.data)
        try:
            db_config = read_db_config()
            conn = MySQLConnection(**db_config)

            cursor = conn.cursor()
            cursor.execute(self.data,multi=True)

            conn.commit()
        except Error as error:
            print(error)

        finally:
            cursor.close()
            conn.close()
        print(f"Stopping Thread {self.id}")


def getData(target,seperator):
    file = open(target,"r")

    with file as t_file:
        querryString = ""
        resetTicker = 0
        for line in t_file:
            stripped = line.strip()
            splitted = stripped.split(seperator)
            querryString +=f"INSERT INTO data (identifier,authenticator) VALUES ('{splitted[0]}','{splitted[1]}');"
            resetTicker +=1
            if resetTicker >= max_inserts:
                t1 = insertToDb(1,querryString)
                t1.start()
                resetTicker = 0
                querryString = ""
                time.sleep(2.4)
    #If data remains push it to the thread aswell
    if querryString != "":
        t1 = insertToDb(1,querryString)
        t1.start()
        resetTicker = 0
        querryString = ""
    
    file.close()
    #print(querryString)
getData(targetFile,":")



print("Stopping Main thread!")