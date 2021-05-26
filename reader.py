from flask import Flask, jsonify, request
from neo4j import GraphDatabase
import csv

def db_connection():
  with open("Credentials.txt") as file:
    data=csv.reader(file,delimiter=",")
    for row in data:
        username=row[0]
        passwd=row[1]
        uri=row[2]
  #print(username,passwd,uri)
  driver=GraphDatabase.driver(uri=uri,auth=(username,passwd))
  return driver

def searchByBookingID(booking_id):
  driver = db_connection()
  session=driver.session()
  query = """
  match (traveller:Users) <- [r:BOOKER_OF{ID:$booking_id}] - (booker:Users)
  return r.ID as bookingId, booker.ID as bkgAccountId, booker.contact as bkgLoginId, collect(traveller.contact) as travellerLoginId
  """
  query_parameter_map = {
    "booking_id":booking_id
  }
  result = session.run(query,query_parameter_map)
  data = result.data()
  driver.close()
  return data

def searchByAccountID(booker_account_id):
  driver = db_connection()
  session=driver.session()
  query = """
  match (traveller:Users) <- [r:BOOKER_OF] - (booker:Users{ID:$booker_account_id})
  return r.ID as bookingId, booker.ID as bkgAccountId, booker.contact as bkgLoginId, collect(traveller.contact) as travellerLoginId
  """
  query_parameter_map = {
    "booker_account_id":booker_account_id
  }
  result = session.run(query,query_parameter_map)
  data = result.data()
  driver.close()
  return data

def searchBYContactInfo(contact_info):
  driver = db_connection()
  session=driver.session()
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
    "contact_info":contact_info
  }
  
  result1 = session.run(query1,query_parameter_map)
  data1 = result1.data()
  
  result2 = session.run(query2,query_parameter_map)
  data2 = result2.data()
  
  data1.extend(data2)
  driver.close()
  
  return data1

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, World!'
  
@app.route('/read',methods=["GET"])
def read():
  param = request.get_json()
  booking_id = param.get('bookingId')
  booker_account_id = param.get('accountId')
  contact_info = param.get('emailId')
  
  if booking_id is not None:
    data = searchByBookingID(booking_id)
  elif booker_account_id is not None:
    data = searchByAccountID(booker_account_id)
  elif contact_info is not None:
    data = searchBYContactInfo(contact_info)
  else:
    data = {
      "response": "Information not send properly"
    }
  
  return jsonify(data)
  
if __name__=="__main__":
    app.run(debug=True)