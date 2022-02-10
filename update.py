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

class room_update:
    model = 'ray'
    hostname = socket.gethostname()
    
    parser = configparser.ConfigParser()
    parser.read("/home/pi/sensor/multi/config/config.txt")

    # SQL
    empty = "-"
    
    connection  = ''
    cursor  = ''
    
    sql_host=parser.get("config", "host")
    sql_user=parser.get("config", "user")
    sql_passwd=parser.get("config", "passwd")
    sql_database=parser.get("config", "database")
    sql_portsql=int(parser.get("config", "portsql"))

         
    sqlconnected = False

    updateSql_motion1 = "UPDATE  room_device SET MOTIONDETECTION = %s WHERE DEVICE_ID = %s;"
    updateSql_motion2 = "UPDATE  room_device SET MOTIONDETECTION = %s WHERE DEVICE_ID = %s;"
    updateSql_motion3 = "UPDATE  room_device SET MOTIONDETECTION = %s WHERE DEVICE_ID = %s;"
    updateSql_motion4 = "UPDATE  room_device SET MOTIONDETECTION = %s WHERE DEVICE_ID = %s;"
    updateSql_temp = "UPDATE  room_device SET CO2 = %s, TEMPERATURE = %s, HUMIDITY = %s WHERE DEVICE_ID = %s;"
    updateSql_power = "UPDATE  room_device SET POWER_COMSUMPTION = %s WHERE DEVICE_ID = %s ;"


    #(ROOM_ID, DEVICE_ID,STATUS,POWER_COMSUMPTION,Motiondetection,TEMPERATURE,HUMIDITY,CO2) VALUES (%s,%s,$s,%s,%s,%s,%s,%s)"

    # PIR sensor variable 
    pir_sensor_1 = int (parser.get("config", "pir_sensor_1"))
    motion_sensor1 = '0'
    motion1 = ''


    pir_sensor_2 = int (parser.get("config", "pir_sensor_2"))
    motion_sensor2 = '0'
    motion2 = ''


    pir_sensor_3 = int (parser.get("config", "pir_sensor_3"))
    motion_sensor3 = '0'
    motion3 = ''


    pir_sensor_4 = int (parser.get("config", "pir_sensor_4"))
    motion_sensor4 = '0'
    motion4 = ''
    #grovepi.pinMode(pir_sensor,"INPUT")

    #SDM power

    # modbus = ModbusClient(method='rtu', port='/dev/ttyUSB0', baudrate=2400, timeout=1)
    # modbus.connect()
    #

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
    pir_sensor_id_1 = parser.get("config", "pir_sensor_id_1")
    pir_sensor_id_2 = parser.get("config", "pir_sensor_id_2")
    pir_sensor_id_3 = parser.get("config", "pir_sensor_id_3")
    pir_sensor_id_4 = parser.get("config", "pir_sensor_id_4")
    Light_id_1 = parser.get("config", "Light_id_1")
    Aircond_id_1 = parser.get("config", "Aircond_id_1")
    Aircond_id_2 = parser.get("config", "Aircond_id_2")


    myresult_light = ""
    myresult_socket = ""
    myresult_aircond1 = ""
    myresult_aircond2 = ""
    
    temp ='0'
    humd='0'
    co2='0'

    scd_thread = ''
    sdm_thread= ''
    pir_thread_1= ''
    pir_thread_2= ''
    pir_thread_3= ''
    pir_thread_4= ''
    
    sqlcommitcode= ''
    
    relay_usb=False # check usb port
    
    v = ''
    v1 = ''
    f = ''
    f1 = ''
    i = '' 
    i1 = ''
    decoder = ''
    m  = ''
    
    failsdc = True
    
    # logging
    logupdate = parser.get("config", "logupdate")
     # Create and configure logging
    logging.basicConfig(filename=logupdate+ str(datetime.datetime.now()) +'.log', format='%(asctime)s %(levelname)s:%(message)s')
    
    now= datetime.datetime.now()
    formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
    
    #print(formatted_date)
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
                    logging.warning('sql Discconected')
                    
            except Exception as err:
                    logging.error('Sql fail'+str(err))
                    self.sqlconnected  = False
                    pass
            except KeyboardInterrupt:
                
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
                    if self.failsdc :
                        self.scd30.set_measurement_interval(2)
                        self.scd30.start_periodic_measurement()
                        self.failsdc = False
                        time.sleep(2)
                    if self.scd30.get_data_ready():
                        self.m = self.scd30.read_measurement()
                        if self.m is not None:
                            self.temp ='%.1f' %(self.m[1])
                            self.humd=int((self.m[2]))
                            self.co2=int((self.m[0]))
                            #logging.info(f"CO2: {self.m[0]:.2f}ppm, temp: {self.m[1]:.2f}'C, rh: {self.m[2]:.2f}%")
                            
                        
                        time.sleep(2)
                    else:
                        time.sleep(0.2)
                except Exception as err:
                    
                    logging.error ("Error scd 30 : "+str(err) )
                    self.failsdc = True
                    pass
                except KeyboardInterrupt:
                    self.cleanup()
    
    
    def sdm_run(self):
        while True:
            if self.relay_usb:
                try:
                    self.modbus = ModbusClient(method=self.modbus_method, port=self.modbus_portusb, baudrate=self.modbus_baurate, timeout=1)
                    self.modbus.connect()
                    time.sleep(0.2)
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
                    self.i1=float('%.2f' % self.decoder.decode_32bit_float())
                   
              
                    #logging.info ("KWH is ", self.i1, "Kwh")
                    time.sleep(5)
          
                except Exception as err:
                   
                    logging.error ("Error sdm : "+str(err))
                    pass
                except KeyboardInterrupt:
                    self.cleanup()
    
    def pir_1(self):
        
            try:
                while True:
                    
                    #logging.info(self.pir_sensor_1)
                    # Sense motion, usually human, within the target range
                    self.motion_sensor1 ='0'
                    if grovepi.digitalRead(self.pir_sensor_1):
                        self.motion1 ='Motion Detected1'
                        self.motion_sensor1 ='1'
                        #logging.info ('motion 1 true')
                        #logging.info (hostname+pir_sensor_id_1)
                        #cursor.execute(updateSql_motion,(motion_sensor1,hostname+pir_sensor_id_1))
                        # if self.sqlconnected == True:
                            # logging.info('update motion 1 true')
                                                   # cursor.execute(insertSql_motion,(hostname,hostname+pir_sensor_id1,empty,empty,motion_sensor1,empty,empty,empty))
                        
                    else:
                        #logging.info ('0')
                        self.motion_sensor1 ='0'
                        #logging.info(' motion 1 false')
                        #logging.info (hostname+pir_sensor_id_1)
                        #cursor.execute(updateSql_motion,(motion_sensor1,hostname+pir_sensor_id_1))
                        #cursor.execute(insertSql_motion,(hostname,hostname+pir_sensor_id1,empty,empty,motion_sensor1,empty,empty,empty))
                        # if self.sqlconnected == True:
                            # logging.info(' motion 1 false')
                            #self.cursor.execute(self.updateSql_motion1,(self.motion_sensor1,self.hostname+self.pir_sensor_id_1)) 
             
                    # if your hold time is less than this, you might not see as many detections
                    time.sleep(5)
         
            except Exception as err:
                logging.error ("Error pir1 : "+str(err))
                pass
            except KeyboardInterrupt:
                logging.info ("Error pir1 ")
                self.cleanup()
    
    def pir_2(self):
       
            try:
                while True:
                    
                    self.motion_sensor2 =False
                    # Sense motion, usually human, within the target range
                    if grovepi.digitalRead(self.pir_sensor_2):
                        self.motion2 ='Motion Detected2'
                        self.motion_sensor2 ='1'
                       # logging.info(' motion 2 true')
                        # if self.sqlconnected == True:
                            # logging.info(' motion 2 true')
                           # self.cursor.execute(self.updateSql_motion2,(self.motion_sensor2,self.hostname+self.pir_sensor_id_2))
                        
                        #cursor.execute(insertSql_motion,(hostname,hostname+pir_sensor_id2,empty,empty,motion_sensor2,empty,empty,empty))
                        
                    else:
                        self.motion_sensor2='0'
                       # logging.info(' motion 2 false')
                        # if self.sqlconnected == True:
                            # logging.info(' motion 2 false')
                            #self.cursor.execute(self.updateSql_motion2,(self.motion_sensor2,self.hostname+self.pir_sensor_id_2))
                            
                        #cursor.execute(insertSql_motion,(hostname,hostname+pir_sensor_id2,empty,empty,motion_sensor2,empty,empty,empty))
                        
                       
             
                    # if your hold time is less than this, you might not see as many detections
                    time.sleep(5)
         
            except Exception as err:
                logging.error ("Error pir2 :"+str( err))
                pass
            except KeyboardInterrupt:
               
                self.cleanup()
    
    def pir_3(self):
        while True:
            try:
                
                self.motion_sensor3 =False
                # Sense motion, usually human, within the target range
                if grovepi.digitalRead(self.pir_sensor_3):
                    self.motion3 ='Motion Detected3'
                    self.motion_sensor3 ='1'
                     #logging.info('motion 3 true')
                    #self.cursor.execute(self.updateSql_motion,(self.motion3,self.hostname+self.pir_sensor_id_3))
                    # if self.sqlconnected == True:
                        # logging.info('update motion 3')
                        #self.cursor.execute(self.updateSql_motion3,(self.motion_sensor3,self.hostname+self.pir_sensor_id_3))
                    #cursor.execute(insertSql_motion,(hostname,hostname+pir_sensor_id3,empty,empty,motion_sensor2,empty,empty,empty))
                    
                else:
                    self.motion_sensor3 ='0'
                    #logging.info(' motion 3 false')
                    # if self.sqlconnected == True:
                        # logging.info('update motion 3')
                        #self.cursor.execute(self.updateSql_motion3,(self.motion_sensor3,self.hostname+self.pir_sensor_id_3))
                    
                    #cursor.execute(insertSql_motion,(hostname,hostname+pir_sensor_id3,empty,empty,motion_sensor2,empty,empty,empty))
                    
         
                # if your hold time is less than this, you might not see as many detections
                time.sleep(5)
         
            except Exception as err:
                logging.info ("Error pir3 :"+str(err))
                pass
            except KeyboardInterrupt:
                self.cleanup()
    
    
    def pir_4(self):
        while True:
            try:
                
                self.motion_sensor4 =False
                # Sense motion, usually human, within the target range
                if grovepi.digitalRead(self.pir_sensor_4):
                    self.motion4 ='Motion Detected4'
                    self.motion_sensor4 ='1'
                   # logging.info(' motion 4 true')
                    # if self.sqlconnected == True:
                        # logging.info('update motion 4')
                        #self.cursor.execute(self.updateSql_motion4,(self.motion_sensor4,self.hostname+self.pir_sensor_id_4))
                    
                   # cursor.execute(insertSql_motion,(hostname,hostname+pir_sensor_id4,empty,empty,motion_sensor2,empty,empty,empty))
                    
                else:
                    self.motion4 ='Motion Detected4'
                    self.motion_sensor4 ='0'
                    #if self.sqlconnected == True:
                        #logging.info('update motion 4')
                        #self.cursor.execute(self.updateSql_motion4,(self.motion_sensor4,self.hostname+self.pir_sensor_id_4))
                    #cursor.execute(insertSql_motion,(hostname,hostname+pir_sensor_id4,empty,empty,motion_sensor2,empty,empty,empty))
                    
                # if your hold time is less than this, you might not see as many detections
                time.sleep(5)
         
            except Exception as err:
                logging.info ("Error pir4 :"+str(err))
                pass
            except KeyboardInterrupt:
                self.cleanup()
    
    
    def signal_handler(signum, frame):
        signal.signal(signum, signal.SIG_IGN) # ignore additional signals
        self.cleanup()
    
    def cleanup(self):
        logging.info('code  start exiting')
        self.scd_thread.join()
        self.sdm_thread.join()
        self.pir_thread_1.join()
        self.pir_thread_2.join()
        self.pir_thread_3.join()
        self.pir_thread_4.join()
        logging.info('code exiting')
        sys.exit(0)
    
    
    
    def sqlcommit(self):
        while True:
            try:
                #logging.info ('commit sql')
                if self.connection.open:
                    logging.info ('commit sql')
                    self.cursor.execute(self.updateSql_motion1,(self.motion_sensor1,self.hostname+self.pir_sensor_id_1)) 
                    self.cursor.execute(self.updateSql_motion1,(self.motion_sensor2,self.hostname+self.pir_sensor_id_2)) 
                    self.cursor.execute(self.updateSql_motion1,(self.motion_sensor3,self.hostname+self.pir_sensor_id_3))
                    self.cursor.execute(self.updateSql_motion1,(self.motion_sensor4,self.hostname+self.pir_sensor_id_4))
                    self.cursor.execute(self.updateSql_temp,(self.co2,self.temp,self.humd,self.hostname+self.temp_sensor_id))
                    self.cursor.execute(self.updateSql_power,(self.i1,self.hostname+self.sdm_sensor_id_1))

                    time.sleep(9)
         
            except Exception as err:
                logging.info ("Error commit data"+str(err))
                pass
            except KeyboardInterrupt:
                self.cleanup()
    
  
            
    
    def run_program(self):
        try :

            
            
            self.scd_thread = threading.Thread(target=self.scd30_run, args=())
            self.sdm_thread = threading.Thread(target=self.sdm_run, args=())
            self.usbcheckssdm_thread = threading.Thread(target=self.checkusb_run, args=())              
            self.pir_thread_1 = threading.Thread(target=self.pir_1, args=())
            self.pir_thread_2 = threading.Thread(target=self.pir_2, args=())
            self.pir_thread_3 = threading.Thread(target=self.pir_3, args=())
            self.pir_thread_4 = threading.Thread(target=self.pir_4, args=())
            
            
            self.sqlcommitcode = threading.Thread(target=self.sqlcommit, args=())
            self.sql_thread = threading.Thread(target=self.sqlconnect, args=())
            
            logging.info("Main    : before running thread")
            self.usbcheckssdm_thread.start()
            time.sleep(2)
            self.sql_thread.start()
            time.sleep(2)
            self.scd_thread.start()
            self.sdm_thread.start()
            self.pir_thread_1.start()
            self.pir_thread_2.start()
            #self.pir_thread_3.start()
            #self.pir_thread_4.start()
            time.sleep(5)
            self.sqlcommitcode.start()
            logging.info("Main    : all done")

        except KeyboardInterrupt:
           # logging.info('hello')
            self.cleanup()   
        



if __name__ == "__main__":
    try :
        object = room_update()
        object.run_program()
    except Exception as err:
        logging.error ('Error main Update : '+str(err))
        pass
    except KeyboardInterrupt:
            logging.info('Control C pressed in update ')
            object.cleanup()


