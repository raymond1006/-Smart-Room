from scd30_i2c import SCD30
import time
import datetime
import grovepi

import threading
import signal
import sys
import socket
import pymysql
import configparser
import logging
import os

from pymodbus.client.sync import ModbusSerialClient as ModbusClient
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder

class room_insert:
    model = 'ray'
    hostname = socket.gethostname()
    
    
    
    parser = configparser.ConfigParser()
    parser.read("/home/pi/sensor/multi/config/config.txt")
    
    # SQL
    empty = "-"
    sqlconnected = ''
    connection  = ''
    cursor  = ''
    
    sql_host=parser.get("config", "host")
    sql_user=parser.get("config", "user")
    sql_passwd=parser.get("config", "passwd")
    sql_database=parser.get("config", "database")
    sql_portsql=int(parser.get("config", "portsql"))

         
    sqlconnected = False
    selectSql_temp = "SELECT  CREATED_DATETIME FROM room_device where DEVICE_ID=%s"
    selectSql_power = "SELECT  CREATED_DATETIME FROM room_device where DEVICE_ID=%s"
    selectSql_socket = "SELECT  CREATED_DATETIME FROM room_device where DEVICE_ID=%s"
    selectSql_aircond1 = "SELECT  CREATED_DATETIME FROM room_device where DEVICE_ID=%s"
    selectSql_aircond2 = "SELECT  CREATED_DATETIME FROM room_device where DEVICE_ID=%s"
    
    selectSql_all = "SELECT  DEVICE_ID,CREATED_DATETIME FROM room_device where ROOM_ID=%s"

    
    #insertSql_motion = "INSERT INTO history (ROOM_ID, DEVICE_ID,STATUS,POWER_COMSUMPTION,Motiondetection,TEMPERATURE,HUMIDITY,CO2) VALUES (%s,%s,$s,%s,%s,%s,%s,%s)"
    insertSql_temp = "INSERT INTO history_temperature (ROOM_ID, DEVICE_ID,TEMPERATURE,HUMIDITY,CO2) VALUES (%s,%s,%s,%s,%s)"
    insertSql_power = "INSERT INTO history_power_usage (ROOM_ID, DEVICE_ID,POWER_COMSUMPTION) VALUES (%s,%s,%s)"
    insertSql_socket = "INSERT INTO history_power_usage (ROOM_ID, DEVICE_ID,POWER_COMSUMPTION) VALUES (%s,%s,%s)"
    insertSql_light = "INSERT INTO history_power_usage (ROOM_ID, DEVICE_ID,POWER_COMSUMPTION) VALUES (%s,%s,%s)"
    insertSql_aircond1 = "INSERT INTO history_power_usage (ROOM_ID, DEVICE_ID,POWER_COMSUMPTION) VALUES (%s,%s,%s)"
    insertSql_aircond2 = "INSERT INTO history_power_usage (ROOM_ID, DEVICE_ID,POWER_COMSUMPTION) VALUES (%s,%s,%s)"

    # TEMPERATURE sensor variable 
    scd30 = SCD30()
 
    #

    # power comsumption
    modbus_method = parser.get("config", "method")
    modbus_portusb = parser.get("config", "portusb")
    modbus_baurate = int(parser.get("config", "baudrate"))
    
   

    # ID variable
    sdm_sensor_id_1 = parser.get("config", "sdm_sensor_id_1")
    temp_sensor_id  = parser.get("config", "temp_sensor_id")

    Light_id_1 = parser.get("config", "Light_id_1")
    Aircond_id_1 = parser.get("config", "Aircond_id_1")
    Aircond_id_2 = parser.get("config", "Aircond_id_2")


    myresult_light = ""
    myresult_socket = ""
    myresult_aircond1 = ""
    myresult_aircond2 = ""
    
    temp_first=True
    temp_old = '0.0'
    temp ='0.0'
    humd='0'
    co2='0'

    scd_thread = ''
    sdm_thread= ''
    usbcheckrelay_thread =''
    
    usbcheckssdm_thread =''
    
    current_date = datetime.datetime.now()
    now_hour = current_date.hour
    now_minute = current_date.minute
    now_second = current_date.second
    
    now_date = current_date.date()
    temp_date = current_date.date()
    light_date = current_date.date()
    socket_date = current_date.date()
    aircond1_date = current_date.date()
    aircond2_date = current_date.date()
    
    relay_usb=False # check usb port 
    sdm_usb=False # check usb port 
    
    usb_count= 0
    usb_sdm_count= 0
    
    sqlcommitcode= ''
    
    v = ''
    v1 = ''
    f = ''
    f1 = ''
    i = '' 
    i1 = '0.00'
    
    decoder = ''
    m  = ''
    
    # logging
    loginsert = parser.get("config", "loginsert")
     # Create and configure logging
     #level=logging.DEBUG
    logging.basicConfig( filename=loginsert+ str(datetime.datetime.now()) +'.log', format='%(asctime)s %(levelname)s:%(message)s')
    
                  
   
    
    def sqlconnect(self):
        while True:
            try:
                self.connection = pymysql.connect(host=self.sql_host,user=self.sql_user,passwd=self.sql_passwd,database=self.sql_database, port=self.sql_portsql, autocommit=True)                 
                
                if self.connection.open:
                    if self.sqlconnected == False:
                         self.cursor = self.connection.cursor()
                         self.sqlconnected  = True
                     
                else :
                    self.sqlconnected  = False
                    logging.warning('Sql Discconected')
                    
            except Exception as err:
                    logging.error('Sql Fail connection '+str(err))
                    self.sqlconnected  = False
                    pass
            except KeyboardInterrupt:
                logging.info('Ctrl C pressed')
                self.sqlconnected  = False
                self.cleanup()
    
    def checkusb_run(self):
        while True:
            try:
                self.relay_usb=os.path.exists(self.modbus_portusb)
                #logging.info("check power meter usb")
                if self.relay_usb:
                    #logging.info("USB power meter Connected")
                    self.usb_sdm_count = 0
                else :
                    if self.usb_sdm_count <= 20:
                        
                        logging.warning("USB power meter Disconnected"+str(self.usb_sdm_count))
                        self.usb_sdm_count=self.usb_sdm_count+1
                        time.sleep(1)
                    else :
                        
                        logging.warning('code exiting for power meter due to port not found more than '+str(self.usb_sdm_count)+' time')
                        sys.exit(0)
            except Exception as err:
                    logging.error('Sql check usb sdm '+str(err))
                    self.sqlconnected  = False
                    pass       
                        

    
    def scd30_run(self):
        while True:
            try:
                #logging.info('sdc30')
                self.scd30.set_measurement_interval(2)
                self.scd30.start_periodic_measurement()
                if self.scd30.get_data_ready():
                    self.m = self.scd30.read_measurement()
                    if self.m is not None:
                        self.temp ='%.1f' %(self.m[1])
                        self.humd=int((self.m[2]))
                        self.co2=int((self.m[0]))
                        
                        if self.temp_first:
                            self.temp_old =self.temp
                            self.temp_first = False
                            
                            
                        samevalue = float(self.temp) - float(self.temp_old)
                      
                        if samevalue > 0:
                            self.temp_old =self.temp  # get now value
                            
                        else :
                            self.temp =self.temp_old # get now temp value from previous
                            
        
                    time.sleep(2)
                else:
                    time.sleep(0.2)
            
            except KeyboardInterrupt:
                self.cleanup()
            except Exception as err:
                logging.error ("Error scd 30"+str(err))
                pass
    
    def sdm_run(self):
        while True:
            try:
                if self.relay_usb:
                    
                   # self.ser = serial.Serial(port= self.portusbrelay, baudrate = self.baudraterelay, parity=serial.PARITY_NONE ,stopbits=serial.STOPBITS_ONE, bytesize=serial.EIGHTBITS, timeout=self.timeoutrelay)
                    self.modbus = ModbusClient(method=self.modbus_method, port=self.modbus_portusb, baudrate=self.modbus_baurate, timeout=1)
                    self.modbus.connect()
                    time.sleep(1)
                    self.v = self.modbus.read_input_registers(0x00,2 , unit=1)
                    self.f = self.modbus.read_input_registers(0x46,2 , unit=1)
                    self.i = self.modbus.read_input_registers(384,2 , unit=1)

                    self.decoder = BinaryPayloadDecoder.fromRegisters(self.v.registers,byteorder=Endian.Big)
                                                     
    #                 self.v1=decoder.decode_32bit_float()
    #                 logging.info ("voltage is ", v1, "V")
    # 
    #                 self.decoder = BinaryPayloadDecoder.fromRegisters(self.f.registers,byteorder=Endian.Big)
    #                 self.f1=decoder.decode_32bit_float()
    #                 logging.info ("frequency is ",f1, "hz")

                    self.decoder = BinaryPayloadDecoder.fromRegisters(self.i.registers,byteorder=Endian.Big)
                    self.i1  ='%.2f' %  self.decoder.decode_32bit_float()
               
                    
                    time.sleep(5) 
          
            
            except KeyboardInterrupt:
                self.cleanup()
            except Exception as err:
                print ("Error sdm "+str(err))
                logging.error ("Error sdm "+str(err))
                pass
                
   
    
    def signal_handler(signum, frame):
        signal.signal(signum, signal.SIG_IGN) # ignore additional signals
        self.cleanup()
    
    def cleanup(self):
        logging.info('code  start exiting')
        self.scd_thread.join()
        self.sdm_thread.join()
        
        self.usbcheckssdm_thread.join()
        logging.info('code exiting')
        sys.exit(0)
    
    
    
    def sqlcommit(self):
        while True:
            try:
               # logging.info ('IN commit sql')
                if self.connection.open:
                    
                    self.current_date = datetime.datetime.now()
                    self.now_hour = self.current_date.hour
                    self.now_minute = self.current_date.minute
                    self.now_second = self.current_date.second   
                   
                  
                    #print(self.now_hour,self.now_minute,self.now_second)   
                    if self.now_hour == 0 and self.now_minute == 1 and self.now_second ==0:
                    #if self.now_hour == 22 and self.now_minute == 45 and self.now_second ==1: # debug
                       
                        self.cursor.execute(self.selectSql_all,(self.hostname))
                        myresult = self.cursor.fetchall()
                        if myresult != None:
                             
                             for i in myresult:
                                 
                                 if i[0] == self.hostname+self.temp_sensor_id:
                                       
                                       self.cursor.execute(self.insertSql_temp,(self.hostname,self.hostname+self.temp_sensor_id,self.temp,self.humd,self.co2))
                                       logging.debug(str(self.temp_sensor_id)+'insert')
                                 elif i[0] == self.hostname+self.sdm_sensor_id_1:
                                       
                                       
                                       self.cursor.execute(self.insertSql_socket,(self.hostname,self.hostname+self.sdm_sensor_id_1,self.i1))                                  
                                       logging.debug(str(self.sdm_sensor_id_1)+'insert')
                                       
                                 elif i[0] == self.hostname+self.Light_id_1:
                                       
                                       
                                       self.cursor.execute(self.insertSql_light,(self.hostname,self.hostname+self.Light_id_1,self.i1))                                  
                                       logging.debug(str(self.Light_id_1)+'insert')
                                 elif i[0] == self.hostname+self.Aircond_id_1:
                                      
                                       self.cursor.execute(self.insertSql_aircond1,(self.hostname,self.hostname+self.Aircond_id_1,self.i1))
                                       logging.debug(str(self.Aircond_id_1)+'insert')
                                 elif i[0] == self.hostname+self.Aircond_id_2:
                                       
                                       self.cursor.execute(self.insertSql_aircond2,(self.hostname,self.hostname+self.Aircond_id_2,self.i1))
                                       logging.debug(str(self.Aircond_id_2)+'insert')
                                 
                                 time.sleep(1)
                        logging.debug ('Done commit sql insert')
                    time.sleep(1)
                else :
                    logging.warning("SQL not connected ")
            
            except KeyboardInterrupt:
                self.cleanup()
            except Exception as err:
                logging.info ("Error commit in insert"+str(err))
                pass
                
  
            
    
    def run_program(self):
        try :

            
            self.usbcheckssdm_thread = threading.Thread(target=self.checkusb_run, args=())
            self.scd_thread = threading.Thread(target=self.scd30_run, args=())
            self.sdm_thread = threading.Thread(target=self.sdm_run, args=())
            self.sql_thread = threading.Thread(target=self.sqlconnect, args=())               
            
            self.sqlcommitcode = threading.Thread(target=self.sqlcommit, args=())
            
            
           
            self.usbcheckssdm_thread.start()
            time.sleep(2)
            self.sql_thread.start()
            time.sleep(2)
            self.scd_thread.start() 
            self.sdm_thread.start()
            time.sleep(2)
            self.sqlcommitcode.start()
                        
            logging.info("Main    : all done")
        except Exception as err:
            
            logging.error ('Error thread sensor insert :'+ str(err))
        except KeyboardInterrupt:
     
            self.cleanup()   
        



if __name__ == "__main__":
    try :
        object = room_insert()
        object.run_program()
    
        
    except KeyboardInterrupt:
            logging.info('Control C pressed')
            object.cleanup()

    except Exception as err:
        
        logging.error ("Unknown main insert error"+ str(err))
        pass

