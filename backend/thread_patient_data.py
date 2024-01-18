import requests
import json
import pymongo
import threading

def fetch_patient_data(start_page, end_page = None):
    db = "store_data"
    table = "patient_data"
    conn = pymongo.MongoClient("mongodb+srv://rohannagadiya:sPQQNqNbp2vE3A3Z@database.56qutkr.mongodb.net/")
    mydb = conn[db]
    conn = mydb[table]
    page = start_page

    while True:

        for page in range(start_page, end_page):
            url = f"https://sdsortho.ema.md/ema/ws/v3/patients?selector=lastName,firstName,mrn,pmsId,dateOfBirth,preferredPhoneNumber,phoneNumbers,email,establishedPatient,dateLastVisit,encryptedId,statementNumber&where=&paging.pageSize=25&paging.pageNumber={page}&sorting.sortBy=lastName&sorting.sortOrder=ASC&showInactive=false"
            headers = {
                'Accept': 'application/json, text/plain, */*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
                'Connection': 'keep-alive',
                'Cookie': 'gdpr=provider; JSESSIONID=63183F2BF53AC4E238947404FB2DBB39; CSID=HZA85FE7EF06A74749A2C4216DD4EDE650; AWSALBAPP-1=_remove_; AWSALBAPP-2=_remove_; AWSALBAPP-3=_remove_; amp_6e403e=RUxu-rqw-cUnQ12sxpLllO...1hchj5jjd.1hchj5jjd.0.0.0; OAuth_Token_Request_State=8add4d7f-6a20-432a-8740-e3ca9f0b7c46; _hp2_ses_props.2907378054=%7B%22ts%22%3A1698299249286%2C%22d%22%3A%22sdsortho.ema.md%22%2C%22h%22%3A%22%2Fema%2Fweb%2Fpractice%2Fstaff%22%2C%22g%22%3A%22%23%2Fpractice%2Fstaff%2Fdashboard%22%7D; _hp2_id.2907378054=%7B%22userId%22%3A%224835702585115963%22%2C%22pageviewId%22%3A%225807460117290232%22%2C%22sessionId%22%3A%22379949830544778%22%2C%22identity%22%3A%22sdsortho.ema.md-18919881%22%2C%22trackerVersion%22%3A%224.0%22%2C%22identityField%22%3Anull%2C%22isIdentified%22%3A1%7D; AWSALBAPP-0=AAAAAAAAAACvrqVK3a9LrT3QddeLQdWGtBagdCj/dgpDUOBiEYQPBt25vuTy593Cqa7wDkNKBWEPuxeD3d4xDwSifLx+WhjlIrqpOBhQzt+MiOWNdjWuRsOyUP+FJAnCvV696dJN4D5CoBM=',
                'Host': 'sdsortho.ema.md',
                'Referer': 'https://sdsortho.ema.md/ema/web/practice/staff',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
            }

            response = requests.get(url=url, headers=headers)
            print(response.status_code)

            data = json.loads(response.text)


            if not data:
                break
            page += 1

            if end_page and page>= end_page:
                break

            item1 = {}
            for item in data:
                try:
                    patient_id = item['id']
                    if conn.find_one({'PatientID': patient_id}):
                        print(f"Patient ID {patient_id} already in database.")
                        continue
                    item1['PatientID'] = patient_id
                except Exception as e:
                    patient_id = ""
                try:
                    firstname = item['firstName']
                    item1['Firstname'] = firstname
                except Exception as e:
                    firstname = ""

                try:
                    lastname = item['lastName']
                    item1['Lastname'] = lastname
                except Exception as e:
                    lastname = ""
                try:
                    DOB = item['displayDateOfBirth']
                    item1['DOB'] = DOB
                except Exception as e:
                    print(e)

                try:
                    phone_number = item['phoneNumbers'][0]['formattedPhoneNumber']
                    item1['PhoneNumber'] = phone_number
                except Exception as e:
                    print(e)

                url1 = f"https://sdsortho.ema.md/ema/ws/v3/patients/{patient_id}?selector=prefix,firstName,middleName,lastName,suffix,phoneNumbers,addressPrimary,preferredPhone,email,emailAlt,maritalStatus,inactiveInsuranceGroups(policies(groupNumber,patientRelationshipToPolicyHolder,policyHolderSsn,policyHolderDateOfBirth,policyHolderFirstName,policyHolderMiddleName,policyHolderLastName)),ungroupedInsurancePolicies(groupNumber,patientRelationshipToPolicyHolder,policyHolderSsn,policyHolderDateOfBirth,policyHolderFirstName,policyHolderMiddleName,policyHolderLastName),activeInsuranceGroups(policies(groupNumber,patientRelationshipToPolicyHolder,policyHolderSsn,policyHolderDateOfBirth,policyHolderFirstName,policyHolderMiddleName,policyHolderLastName)),allInsurancePolicies(groupNumber,patientRelationshipToPolicyHolder,policyHolderSsn,policyHolderDateOfBirth,policyHolderFirstName,policyHolderMiddleName,policyHolderLastName)"

                response1 = requests.get(url=url1, headers=headers)
                print(response1.status_code)

                # address data and payer data of patient
                data1 = json.loads(response1.text)

                try:
                    streeadress = data1['addressPrimary']['street1']
                    item1['streetaddress'] = streeadress
                except Exception as e:
                    streeadress = ""

                try:
                    gender = data1['gender']
                    item1['gender'] = gender
                except Exception as e:
                    gender = ""

                try:
                    city = data1['addressPrimary']['city']
                    item1['city'] = city
                except Exception as e:
                    city = ""

                try:
                    zipcode = data1['addressPrimary']['zipcode']
                    item1['zipcode'] = zipcode
                    if not zipcode:
                        zipcode = ''
                except Exception as e:
                    zipcode = ""

                try:
                    state = data1['addressPrimary']['state']
                    item1['state'] = state
                except Exception as e:
                    state = ""

                try:
                    streeadress2 = data1['addressPrimary']['street2']
                    if not streeadress2:
                        streeadress2 = ""
                except Exception as e:
                    streeadress2 = ""

                try:
                    item1['status'] = "pending"
                except Exception as e:
                    print(e)

                # subscriber_id = ['ungroupedInsurancePolicies'][0]['policyNumber']
                try:
                    conn.insert_one(dict(item1))
                    TGREEN = '\033[1;32m'
                    print(TGREEN + '\rData Inserted ...')
                    print(page)
                except Exception as e:
                    print("Status_Update_duplicate")
                    print(e)

def main():
    # NUM_THREADS = 25
    # START_PAGE = 1608
    # END_PAGE = 1609
    # PAGES_PER_THREAD = (END_PAGE - START_PAGE) // NUM_THREADS

    NUM_THREADS = 25
    START_PAGE = 1608

    MAX_END_PAGE = 10000 
    PAGES_PER_THREAD = 10

    threads = []

    for i in range(NUM_THREADS):
        start = START_PAGE + i * PAGES_PER_THREAD
        end = start + PAGES_PER_THREAD

        # Assign end page for the last thread
        if i == NUM_THREADS - 1:
            end = MAX_END_PAGE

        t = threading.Thread(target=fetch_patient_data, args=(start, end))
        threads.append(t)
        t.start()

    # Wait for all threads to finish
    for t in threads:
        t.join()

    print("All threads completed.")


if __name__ == "__main__":
    main()
