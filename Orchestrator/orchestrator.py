import json
import pika
import redis
from flask import Flask,jsonify,request,abort 
from neo4j import GraphDatabase
import threading

app = Flask(__name__)
client = redis.Redis(host = "redis", port = 6379)

client.flushall()
lock = threading.Lock()
lock1 = threading.Lock()

driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "password"))
session = driver.session()
session.run("match(n) detach delete n")


@app.route("/api/write", methods = ["POST"])
def write():
	body=dict(request.get_json())
	if client.exists(json.dumps(body)):
		return jsonify("Already Present"), 200

	lock.acquire()
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
	channel = connection.channel()
	channel.queue_declare(queue='writeQ', durable = True)
	channel.basic_publish(exchange='', routing_key='writeQ', body=json.dumps(body))
	print(" [x] Sent Message to writeQ", flush = True)
	connection.close()

	if client.exists(body["accountId"]):
		l = json.loads(client.get(body["accountId"]))
	else:
		l=[]
	l.append(body)
	client.setex(body["accountId"], 60*60*4, json.dumps(l))


	if client.exists(body["bookingId"]) == 0:
		client.setex(body["bookingId"], 60*60*4, json.dumps(body))

	if client.exists(body["emailId"]):
		l = json.loads(client.get(body["emailId"]))
	else:
		l = []
	l.append(body)
	client.setex(body["emailId"], 60*60*4,json.dumps(l))

	l1 = body["travelleremailId"].split(",")
	for email in l1:
		if client.exists(email):
			l = json.loads(client.get(email))
		else:
			l = []
		l.append(body)
		client.setex(email, 60*60*4, json.dumps(l))

	client.set(json.dumps(body), 1)

	lock.release()

	return jsonify("Write successfull"), 200

@app.route("/api/bookingId", methods = ["POST"])
@app.route("/api/emailId", methods = ["POST"])
@app.route("/api/accountId", methods = ["POST"])
def read():
	dump = dict(request.get_json())

	for key in dump.keys():
		st = key

	if client.exists(dump[st]):
		return client.get(dump[st]), 200

	lock1.acquire()
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
	channel = connection.channel()
	channel.queue_declare(queue='readQ', durable = True)
	channel.basic_publish(exchange='', routing_key='readQ', body=json.dumps(dump))
	print(" [x] Sent Message to readQ", flush = True)
	connection.close()

	connection1 = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
	channel1 = connection1.channel()
	channel1.queue_declare(queue='responseQ', durable=True)
	print(' [*] Waiting for messages from responseQ', flush = True)

	def callback(ch, method, properties, body):
		global message
		message = json.loads(body)
		print(" [x] Received from responseQ %r" % message, flush = True)
		print(" [x] Done from responseQ", flush = True)
		ch.basic_ack(delivery_tag=method.delivery_tag)
		channel1.stop_consuming()

	channel1.basic_qos(prefetch_count=1)
	channel1.basic_consume(queue='responseQ', on_message_callback=callback)

	while channel1._consumer_infos:
		channel1.connection.process_data_events(time_limit=1)

	if message != []:
		client.setex(dump[st], 60*60*4, json.dumps(message))

	lock1.release()
	return jsonify(message), 200

if __name__ == '__main__':
	app.debug=True
	app.run(host="0.0.0.0",port=80, threaded = True)
	app.use_reloader=False