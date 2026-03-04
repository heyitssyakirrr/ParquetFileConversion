
import sys
import cx_Oracle
import oracledb
import os
from pathlib import Path
project_root = Path(__file__).resolve().parents[3]
sys.path.insert(0,str(project_root))
from Data_Warehouse.Common.Jobs.PASSWORD_DECRYPTOR import decrypt_password

os.environ["ORACLE_HOME"] = '/var/oracle19/client/'
os.environ["ORACLE_BASE"] = '/var/oracle19'
os.environ["ORACLE_SID"] = 'pbbdw'
os.environ["LD_LIBRARY_PATH"] = '$ORACLE_HOME/lib:$LD_LIBRARY_PATH'
dsn = cx_Oracle.makedsn("svdwh003", 2281, service_name="pbbdw")
# detica config
detica_dsn =  cx_Oracle.makedsn("svdetdbs001", 2281, service_name="amltmpbb")

def create_ora_con_pbbdw():
     # decrypt password
    pbbdw_pwd = decrypt_password('pbbdw')
    #create con to oracle pbbdw
    connection = cx_Oracle.connect(user="pbbdw", password=f"{pbbdw_pwd}",dsn=dsn)

    return connection

def create_ora_con_srcdw():
    # decrypt password
    orasrc_pwd = decrypt_password('srcdw')
    #create con to oracle srcdw
    connection = cx_Oracle.connect(user="srcdw", password=f"{orasrc_pwd}",dsn=dsn)

    return connection

def create_ora_con_detica():
    # decrypt password
    detica_pwd = decrypt_password('detica_edw')
    # create con to oracle srcdw
    connection = cx_Oracle.connect(user="detica_edw", password=f"{detica_pwd}",dsn=detica_dsn)

    return connection

def close_ora_con(connection):
    # close con to oracle
    connection.close()
