import pandas as pd;
import cx_Oracle as db;
import pyodbc as PyDB, yaml
import random;
import math;

path = 'C:\\Alok\\Python\\Equifax\\Load Run\\Update\\'

#Read Config file
configPath = path+"PyConfig.yaml"
with open(configPath,'r') as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
logPath = path+"BulkLoader_Logs.log"    
file = open(logPath, "w+")

#Extract connection values from config file
HOST=cfg['DB']['HOST']
USERNAME=cfg['DB']['USERNAME']
PASSWORD=cfg['DB']['PASSWORD']
SCHEMA=cfg['DB']['SCHEMA']

#print ('hi')
data1 = pd.read_csv(path+'op_Final_Phone.csv',encoding='windows-1252',low_memory=False);
#print (data);
data = data1.where(pd.notnull(data1), None)

for row in data.itertuples():
    if "PETPE_TRADINGPARTNER" in row.TABLENAME:
        x = row.TABLENAME.split("(")
        #print (x[1])
        tpQuery1 = f"UPDATE PETPE_TRADINGPARTNER SET PHONE = '1234567890' where PK_ID= '{x[1]}';"
        print (tpQuery1)
        f = open(path+"opUpdate.csv","a+")
        f.write(tpQuery1+'\n')
        f.close()
      