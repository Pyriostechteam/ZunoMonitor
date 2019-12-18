import paho.mqtt.client as mqttClient
import time
import mysql.connector
import json

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="pyrios123",
  database="MonitoringSyetem"
)

mycursor = mydb.cursor()

def on_connect(client, userdata, flags, rc):
    if rc == 0:

        print("Connected to broker")

        global Connected  # Use global variable
        Connected = True  # Signal connection

    else:

        print("Connection failed")

#call back function
def on_message(client, userdata, message):
    print("Message received")

def on_publish(client,userdata,result):             #create function for callback
    #print("data published \n")
    pass

Connected = False  # global variable for the state of the connection
broker_address = "3.16.152.2"  # Broker address
port = 1883  # Broker port
user = "PyriosMonitor_1"  # Connection username
password = "ZunoMonitor_1"  # Connection password
client = mqttClient.Client("10001")  # create new instance
client.username_pw_set(user, password=password)  # set username and password
client.connect(broker_address, port=port)  # connect to broker
client.on_connect = on_connect  # attach function to callback
client.on_message = on_message  # attach function to callback
client.on_publish = on_publish
client.loop_start()  # start the loop

while Connected != True:  # Wait for connection
    time.sleep(0.1)

try:
    while True:
        mycursor = mydb.cursor()
        mycursor.execute("SELECT * FROM Monitor Where DeviceID = 2")
        myresult = mycursor.fetchone()
        while myresult is not None:
            x = {"DeviceID": "3", "Date": myresult[2], "Time": myresult[3],"P1": myresult[4],
                "P2": myresult[5], "P3": myresult[6], "P4": myresult[7],"P5": myresult[8],"P6": myresult[9],
                "P7": myresult[10],"P8": myresult[11],"P9": myresult[12],"P10": myresult[13],"P11": myresult[14],
                "P12": myresult[15],"P13": myresult[16],"P14": myresult[17],"P15": myresult[18],"P16": myresult[19],
                "P17": myresult[20],"P18": myresult[21]}
            payload = json.dumps(x)
            print(payload)
            ret = client.publish("Pyrios/Monitor/Device", payload)  # topic-v1/devices/me/telemetry
            myresult = mycursor.fetchone()
            y = {"DeviceID": "4", "Date": myresult[2], "Time": myresult[3], "P1": myresult[4],
                 "P2": myresult[5], "P3": myresult[6], "P4": myresult[7], "P5": myresult[8], "P6": myresult[9],
                 "P7": myresult[10], "P8": myresult[11], "P9": myresult[12], "P10": myresult[13], "P11": myresult[14],
                 "P12": myresult[15], "P13": myresult[16], "P14": myresult[17], "P15": myresult[18],"P16": myresult[19],
                 "P17": myresult[20], "P18": myresult[21]}
            payload = json.dumps(y)
            print(payload)
            ret = client.publish("Pyrios/Monitor/Device", payload)  # topic-v1/devices/me/telemetry
            time.sleep(60)
            myresult = mycursor.fetchone()
        mycursor.close()
        #print('Database Connection Closed')
        time.sleep(60)

except KeyboardInterrupt:
        #print("exiting")
        client.disconnect()
        client.loop_stop()

