from flask import Flask, jsonify, request
import json
import pymongo
from flask_cors import CORS, cross_origin  # You'll need to install this: pip install Flask-Cors
import requests
from urllib.parse import quote
from bson import json_util, ObjectId


app = Flask(__name__)

db = "store_data"
table = "patient_data"
conn = pymongo.MongoClient("mongodb+srv://rohannagadiya:sPQQNqNbp2vE3A3Z@database.56qutkr.mongodb.net/")
mydb = conn[db]
mycollection = mydb[table]


CORS(app) # Allow cross-origin requests

@app.route("/")
def index():
    return jsonify({"message": "Welcome to Patient's data"})


@app.route("/load_data", methods=['GET'])
def load_data():
    try:
        # Fetching all data except the '_id' field
        data = list(mycollection.find({}, {'_id': 0}))
        return jsonify({"data": data})
    except Exception as e:
        app.logger.error(f"Error in load_data: {str(e)}")
        return jsonify({"error": str(e)}), 500




@app.route('/search', methods=['get'])
def search():
    query = request.json.get('query')
    
    # Here you'd typically query your database or data source
    # For this example, let's just use a static list
    data = ["Alice", "Bob", "Charlie", "David"]
    results = [item for item in data if query.lower() in item.lower()]

    return jsonify(results)

@app.route('/user_data/<int:user_id>')
def get_user_data(user_id):
    data = list(mycollection.find({}, {'_id': 0}))
    user = next((user for user in data if user['PatientID'] == user_id), None)
    if user:
        return jsonify(user)
    return jsonify({"error": "User not found"}), 404

@app.route('/proxy-to-strataemr', methods=['POST'])
def proxy_to_strataemr():
    try:
        # Extract data from the request body
        data = request.json

        # Create a dynamic XML payload using the extracted data
        xml_payload = f"""<?xml version="1.0" encoding="utf-8"?>
        <Requests>
            <Header>
                <APIKey>cc7f22956f3811b029c83d938fc278e6-SDSMedicalTesting</APIKey>
            </Header>
            <ReferralRequest>
                <Organization>SDSMedical</Organization>
                <PartnerPrivateID></PartnerPrivateID>
                <FirstName>{data['Firstname']}</FirstName>
                <LastName>{data['Lastname']}</LastName>
                <MiddleInitial></MiddleInitial>
                <BirthDate>{data['DOB']}</BirthDate>
                <PatientStreetAddress>{data['Streetaddress']}</PatientStreetAddress>
                <PatientCity>{data['City']}</PatientCity>
                <PatientState>{data['State']}</PatientState>
                <PatientZip>{data['Zipcode']}</PatientZip>
                <Gender>{'1' if data['Gender'] == 'Male' else '0'}</Gender>
                <PatientPhone>{data['PhoneNumber'].replace('(', '').replace(')', '').replace(' ', '').replace('-', '')}</PatientPhone>
                <EmergencyContact></EmergencyContact>
                <EmergencyContactPhone></EmergencyContactPhone>
                <CaseDescription>{data['Case_Description'] + data['policy_payer'] + data['policy_subscriber_id']} </CaseDescription>
                <PatientType>PT</PatientType>
                <ServiceLocation>11</ServiceLocation>
                <From>
                    <Name>{data['Referred_By_Name']}</Name>
                    <Email></Email>
                    <Phone></Phone>
                </From>
            </ReferralRequest>
        </Requests>"""


        encoded_payload = 'xml=' + quote(xml_payload)
        url = "https://api.strataemr.com/Referral/V1.1/"
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        response = requests.post(url, headers=headers, data=encoded_payload)
        print(response.text)
        print(response.status_code)
        return jsonify({"status": response.status_code, "response": response.text})

    except Exception as e:
        return jsonify({"error": str(e)})


if __name__ == "__main__":
    app.run(host='0.0.0.0',debug=True)
