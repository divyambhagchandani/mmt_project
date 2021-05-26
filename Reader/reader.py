import json
import pika
from neo4j import GraphDatabase


driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "password"))
session = driver.session()

connection1 = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel1 = connection1.channel()

channel1.queue_declare(queue='readQ', durable=True)
print(' [*] Waiting for messages from readQ', flush = True)

def callback(ch, method, properties, body):

	body=json.loads(body)
	print(" [x] Received from readQ %r" % body, flush = True)

	for key in body:
		if key == "bookingId":
			query = """
			match (traveller:Users) <- [r:BOOKER_OF{ID:$booking_id}] - (booker:Users)
			return r.ID as bookingId, booker.ID as bkgAccountId, booker.contact as bkgLoginId, collect(traveller.contact) as travellerLoginId
			"""
			query_parameter_map = {
			"booking_id":body[key]
			}
			result = session.run(query,query_parameter_map)
			data = result.data()
			print(data, flush = True)
			#driver.close()

		if key == "accountId":
			query = """
			match (traveller:Users) <- [r:BOOKER_OF] - (booker:Users{ID:$booker_account_id})
			return r.ID as bookingId, booker.ID as bkgAccountId, booker.contact as bkgLoginId, collect(traveller.contact) as travellerLoginId
			"""
			query_parameter_map = {
			"booker_account_id":body[key]
			}
			result = session.run(query,query_parameter_map)
			data = result.data()
			print(data, flush = True)
			#driver.close()

		if key == "emailId":
			query1 = """
			match (b:Users) - [r1:BOOKER_OF] -> (t:Users{contact:$contact_info}) 
			match (booker:Users) - [r:BOOKER_OF{ID:r1.ID}] -> (traveller:Users)
			return r.ID as bookingId, booker.ID as bkgAccountId, booker.contact as bkgLoginId, collect(traveller.contact) as travellerLoginId
			"""
			query2 = """
			match (booker:Users{contact:$contact_info}) - [r:BOOKER_OF] -> (traveller:Users) 
			return r.ID as bookingId, booker.ID as bkgAccountId, booker.contact as bkgLoginId, collect(traveller.contact) as travellerLoginId
			"""
			query_parameter_map = {
			"contact_info":body[key]
			}

			result1 = session.run(query1,query_parameter_map)
			data1 = result1.data()

			result2 = session.run(query2,query_parameter_map)
			data2 = result2.data()

			print(data1, data2, flush = True)
			data = data1 + data2 
			print(data, flush = True)
			#driver.close()

	dump = data # dump should be from database
	print(dump, flush = True)
		
	connection2 = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
	channel2 = connection2.channel()
	channel2.queue_declare(queue='responseQ', durable=True)
	channel2.basic_publish(exchange='',routing_key='responseQ',body=json.dumps(dump)) 
	print(" [x] Sent to responseQ %r" % dump)
	connection2.close()
	print(" [x] Done")
		
	ch.basic_ack(delivery_tag=method.delivery_tag)

channel1.basic_qos(prefetch_count=1)
channel1.basic_consume(queue='readQ', on_message_callback=callback)
channel1.start_consuming()