import requests
import json
import pymongo

db = "store_data"
table = "patient_data"
conn = pymongo.MongoClient("mongodb+srv://rohannagadiya:sPQQNqNbp2vE3A3Z@database.56qutkr.mongodb.net/")
mydb = conn[db]

conn1 = mydb[table]


db_url = conn1.find({"status": "pending"})
for data in db_url:
    patient_id = data['PatientID']

    url = f"https://sdsortho.ema.md/ema/ws/v3/patient/header/{patient_id}?selector=primaryProvider(fullName),preferredProvider(fullName),primaryCareProviderWithDateLastSeen,patientReferralsWithDateLastSeen,primaryActiveInsurancePolicy,activeInsurances(id,ranking,insuranceCompanyName,policyType,policyNumber,groupNumber,eligibilityActive,insuranceTermDate),visionInsurancePolicies(id,ranking,insuranceCompanyName,policyType,policyNumber,groupNumber),autoPipInsurancePolicies(id,insuranceCompanyName,policyType,policyNumber,groupNumber),workersCompInsurancePolicies(id,insuranceCompanyName,policyType,policyNumber,groupNumber),addressPrimary,pharmacies(pharmacy(phone,fax),ssPharmacy(phonePrimaryFormattedWithParens,faxFormattedWithParens)),preferredPhone,phoneNumbers,allowLeaveMessage,lastLogin,loginEnabled,email,restricted,pregnant,canMarkPatientPregnant,careTeam(referral(name,specialties,workPhone))"

    headers3 = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Cookie': 'gdpr=provider; JSESSIONID=131F701134B1E8AFA6FF0F4635B5123A; CSID=HZ5F02755F53454AFC84754FA7D8D4E765; AWSALBAPP-1=_remove_; AWSALBAPP-2=_remove_; AWSALBAPP-3=_remove_; amp_6e403e=RUxu-rqw-cUnQ12sxpLllO...1hchj5jjd.1hchj5jjd.0.0.0; OAuth_Token_Request_State=1a1fda95-6de1-4edc-967a-c871a3c40f14; _hp2_ses_props.2907378054=%7B%22ts%22%3A1699598695011%2C%22d%22%3A%22sdsortho.ema.md%22%2C%22h%22%3A%22%2Fema%2Fweb%2Fpractice%2Fstaff%22%7D; _hp2_id.2907378054=%7B%22userId%22%3A%224835702585115963%22%2C%22pageviewId%22%3A%225059894003923398%22%2C%22sessionId%22%3A%221768446095668441%22%2C%22identity%22%3A%22sdsortho.ema.md-18919881%22%2C%22trackerVersion%22%3A%224.0%22%2C%22identityField%22%3Anull%2C%22isIdentified%22%3A1%7D; AWSALBAPP-0=AAAAAAAAAADcuiJZT7Ey9G2rCX5ficjV35JtekMiWwA6/J5JQPLq49QtXCZg5E/ZUfjFPDfRGLfTMO0SLphbGVP0i09jgh3yrlDA9qo6HkUVEVimkO9TQnqtL4rH/YXKQW4g+kNvHDGU+sA=',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    }

    response = requests.get(url=url, headers=headers3)

    print(response.status_code)

    data = json.loads(response.text)
    DATA = data['activeInsurances']
    item = {}
    policy_type = ""
    if 'primaryActiveInsurancePolicy' in data:
        policy_payer = data['primaryActiveInsurancePolicy']['insuranceCompanyName']
        policy_subscriber_id = data['primaryActiveInsurancePolicy']['policyNumber']
    else:
        policy_payer = data['activeInsurances'][1]['insuranceCompanyName']
        policy_subscriber_id = data['activeInsurances'][1]['policyNumber']


    try:
        conn1.update_one({"PatientID": patient_id}, {'$set': {'policy_payer': policy_payer}}, upsert=True)
        TGREEN = '\033[1;32m'
        print(TGREEN + '\rData Inserted ...')
    except Exception as e:
        print(e)

    try:
        conn1.update_one({"PatientID": patient_id}, {'$set': {'policy_subscriber_id': policy_subscriber_id}}, upsert=True)
        TGREEN = '\033[1;32m'
        print(TGREEN + '\rData Inserted .....')
    except Exception as e:
        print(e)

    try:
        conn1.update_one({"PatientID": patient_id}, {'$set': {'status':"done"}},upsert=True)
        TGREEN = '\033[1;32m'
        print(TGREEN + '\rData Inserted ......')
    except Exception as e:
        print(e)




