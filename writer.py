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

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, World!'
  
@app.route("/save_booking_info",methods=["POST"])
def saveBookingInfo():
  param = request.get_json()
  booking_id = param['bookingId']
  booker_account_id = param['accountId']
  booker_contact = param['emailId']
  travellers = param['travelleremailId'].split(',')
  
  driver = db_connection()
  session=driver.session()
  
  # check_query = """
  # match (n:Users{ID:$booker_account_id}) return n.contact as contact
  # """
  # check_query_parameter_map = {"booker_account_id": booker_account_id}
  # result = session.run(check_query, check_query_parameter_map)
  # original_broker_contact = result.data()
  
  # if original_broker_contact[0]['contact'] == booker_contact:
    #create to db
  query = """
  merge (booker:Users{ID:$booker_account_id, contact:$booker_contact}) 
  merge (traveller:Users{contact:$travellers_contact})
  create (booker)-[:BOOKER_OF{ID:$booking_id}]->(traveller)
  """
  for traveller_contact in travellers:
    query_parameter_map = {"travellers_contact": traveller_contact, "booker_account_id": booker_account_id, "booking_id": booking_id, "booker_contact": booker_contact}
    session.run(query,query_parameter_map)
    
  driver.close()
  response = "Booking Information successfully saved."
    
#  else:
#     response = "Booker account ID "+booker_account_id+" does not match with the provided booker contact details "+booker_contact+"."
#     driver.close() 
    
  
  data = {
    "response": response
  }
  return jsonify(data)
  
  
if __name__=="__main__":
    app.run(debug=True)