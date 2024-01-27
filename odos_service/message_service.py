from confluent_kafka import Consumer, KafkaException, Producer
import xmlrpc.client
import json

# Configuración de conexión
odoo_server = "https://udla2.odoo.com/"
db = "udla2"
username = 'emilio.salazar@udla.edu.ec'
password = "integracion123"

# Kafka configuration
kafka_conf_consumer = {'bootstrap.servers': "pkc-4r087.us-west2.gcp.confluent.cloud:9092", 
              'security.protocol':'SASL_SSL',
              'sasl.mechanisms':'PLAIN',
              'sasl.username':'IPJC6W4FNIIX2HLP' ,
              'sasl.password':"/698Q1ZII49b8Acy1RL3jeuHnXutbud5o/C+GZoN2YoygJyUOYisxy1PJWHm+Qll" ,
              'group.id': 'messages_service', 
              'auto.offset.reset': 'earliest'}

kafka_conf_producer = {'bootstrap.servers': "pkc-4r087.us-west2.gcp.confluent.cloud:9092", 
              'security.protocol':'SASL_SSL',
              'sasl.mechanisms':'PLAIN',
              'sasl.username':'IPJC6W4FNIIX2HLP' ,
              'sasl.password':"/698Q1ZII49b8Acy1RL3jeuHnXutbud5o/C+GZoN2YoygJyUOYisxy1PJWHm+Qll" }
# Create Kafka consumer
consumer = Consumer(kafka_conf_consumer)

# Subscribe to topic
consumer.subscribe(['files_backup'])
# Crear el productor Kafka
producer = Producer(kafka_conf_producer)

def get_employees_department(department_id):
    common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(odoo_server))
    uid = common.authenticate(db, username, password, {})

    models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(odoo_server))
    employee_ids = models.execute_kw(db, uid, password, 'hr.employee', 'search', [[['department_id', '=', department_id]]])
    employees = models.execute_kw(db, uid, password, 'hr.employee', 'read', [employee_ids])

    return employees

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            # Proper message
            data = msg.value().decode('utf-8')
            print({
                'value' : msg.value().decode('utf-8')
            })
            json_string = data
            json_data = json.loads(json_string)
            print('department is : ', json_data["department"])
            department = json_data["department"]
            url = json_data["url"]
            # Call to employes
            empleados = get_employees_department(department)
            if len(empleados) > 0 :
                empleados = [{
                    "name": employee.get('name'),
                    "job_title": employee.get('job_title'),
                    "department_id": employee.get('department_id')[1],
                    "work_email": employee.get('work_email'),
                    "work_phone": employee.get('work_phone')
                } for employee in empleados]
                # Datos del mensaje
                message_data = {
                    'employees': empleados,
                    'department': department,
                    'url': url
                }
                
                # Convertir los datos del mensaje a JSON
                message_json = json.dumps(message_data)

                # Enviar el mensaje a Kafka
                producer.produce('email_notification', message_json)
            else:
                 # Datos del mensaje
                message_data = {
                    "url": url,
                    "error": f"""The department {department} does not exits o there are not employees related to it, 
                    the notification is not going to be sent.
                    Please check the file at {url}.""",
                }
                
                # Convertir los datos del mensaje a JSON
                message_json = json.dumps(message_data)
                # Enviar el mensaje a Kafka
                producer.produce('files_error', message_json)
                print(f"The department {department} does not exits, the notification is not going to be sent ")
            # # Esperar a que todos los mensajes sean enviados
            # producer.flush()

except KeyboardInterrupt:
    pass

finally:
    # Close down consumer to commit final offsets.
    consumer.close()


