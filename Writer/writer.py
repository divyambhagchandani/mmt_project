import pika
import json
from neo4j import GraphDatabase


driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "password"))
session=driver.session()

connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()
channel.queue_declare(queue='writeQ', durable=True)
print(' [*] Waiting for messages.', flush = True)

def callback(ch, method, properties, body):
	param = json.loads(body)
	print(" [x] Received %r" % body, flush = True)

	booking_id = param['bookingId']
	booker_account_id = param['accountId']
	booker_contact = param['emailId']
	travellers = param['travelleremailId'].split(',')
  

	query = """
	merge (booker:Users{ID:$booker_account_id, contact:$booker_contact}) 
	merge (traveller:Users{contact:$travellers_contact})
	create (booker)-[:BOOKER_OF{ID:$booking_id}]->(traveller)
	"""
	for traveller_contact in travellers:
		query_parameter_map = {"travellers_contact": traveller_contact, "booker_account_id": booker_account_id, "booking_id": booking_id, "booker_contact": booker_contact}
		session.run(query,query_parameter_map)
	
	#driver.close()
	print("Booking Information successfully saved.", flush = True)
	

	ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='writeQ', on_message_callback=callback)
channel.start_consuming()