import requests
import json
import pymongo
import re
import PyPDF2
import io
import os
import fitz
import hashlib
import threading

def process_patient_data(patient_id):
    # Your code for processing a single patient's data goes here
    try:
        db = "store_data"
        table = "patient_data"
        conn = pymongo.MongoClient("mongodb+srv://rohannagadiya:sPQQNqNbp2vE3A3Z@database.56qutkr.mongodb.net/")
        mydb = conn[db]

        conn1 = mydb[table]

        directory_path = "../scrapping/pdf_path"
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

        headers3 = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Cookie': 'gdpr=provider; JSESSIONID=8D013D51FA8E567C8B760F017DB5C3FB; CSID=HZA20AB6072FE3483F8BA0C678A240A7D6; AWSALBAPP-1=_remove_; AWSALBAPP-2=_remove_; AWSALBAPP-3=_remove_; amp_6e403e=RUxu-rqw-cUnQ12sxpLllO...1hchj5jjd.1hchj5jjd.0.0.0; OAuth_Token_Request_State=059b0618-4756-4847-8218-d1e153193169; _hp2_ses_props.2907378054=%7B%22ts%22%3A1698644127592%2C%22d%22%3A%22sdsortho.ema.md%22%2C%22h%22%3A%22%2Fema%2Fweb%2Fpractice%2Fstaff%22%2C%22g%22%3A%22%23%2Fpractice%2Fstaff%2Fdashboard%22%7D; _hp2_id.2907378054=%7B%22userId%22%3A%224835702585115963%22%2C%22pageviewId%22%3A%226780038209985703%22%2C%22sessionId%22%3A%228824906709776932%22%2C%22identity%22%3A%22sdsortho.ema.md-18919881%22%2C%22trackerVersion%22%3A%224.0%22%2C%22identityField%22%3Anull%2C%22isIdentified%22%3A1%7D; AWSALBAPP-0=AAAAAAAAAACuepa+66glulKcrevjoR54PkQkPGsWQKY7C94Hgy7I8RJi2527VtVnaQ/CEAjztZUkoDO9K7YINOXFfcqtlyj/5+qCmYp04203e14DoqgQd1cZ3/UB0fvUST7/Iy8OZXuVok4=',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
        }

        url3 = f"https://sdsortho.ema.md/ema/ws/v3/order/management/log?grouping.sortOrder=asc&paging.pageNumber=1&paging.pageSize=10&params=%7B%22showPleaseWait%22:true%7D&selector=metadata,cdsmData,settings(allowedToSendVisitNote),facility,tests,performAt(type,enhancedOrders,name,status,labId,institutionTypes),performAtFacility,provider,patient(mrn,encryptedId,dateOfBirth,ageInYears,ageInMonths,activeInsurances(mavPolicyType,policyType)),allowedActions,labOrderSent,orderRecallStatus,withPendingAOEs,insuranceLabel,treatmentCase(id,number,name,insurancePolicy),insurancePolicy(mavPolicyType,policyType)&sorting.sortBy=date&sorting.sortOrder=desc&where=patient%3Din%3D(%22{patient_id}%22)+and+status%3Din%3D(%22OPEN%22)"
        response3 = requests.get(url=url3, headers=headers3)
        print(response3.status_code)

        # This request is for log id of patients
        data3 = json.loads(response3.text)
        item1 = {}
        if not data3:
            print(f"There is no data for this patient {patient_id}")
            try:
                conn1.update_one({"PatientID": patient_id}, {'$set': {'status': "done_with_nodata"}}, upsert=True)
                TGREEN = '\033[1;32m'
                print(TGREEN + '\rStatus update successfully with no data')
            except Exception as e:
                print(e)
            return

        log_id = data3[0]['id']

        url4 = f"https://sdsortho.ema.md/ema/ws/v3/order/management/log/{log_id}/attachments?paging.pageNumber=1&paging.pageSize=100&params=%7B%22showPleaseWait%22:true%7D&selector=fileAttachment(inlineFilePath,encryptedId,fileAttachmentCategory),dateCreated,type"
        response4 = requests.get(url=url4, headers=headers3)
        print(response4.status_code)

        # Request for order logs of customer
        data4 = json.loads(response4.text)

        pdf_path11 = data4[0]['fileAttachment']['inlineFilePath']
        pdf_path = "https://sdsortho.ema.md" + pdf_path11
        pdf_path1111 = pdf_path
        item1['description_link'] = pdf_path1111
        print(pdf_path)

        final_pdf = pdf_path1111.split('?')[0]

        pdf_download = requests.get(final_pdf, headers=headers3)
        pdf_content = pdf_download.content

        local_pdf_path = os.path.join(directory_path, f"pdf_for_patient_{patient_id}.pdf")
        with open(local_pdf_path, 'wb') as f:
            f.write(pdf_content)

        # Use fitz to extract text
        doc = fitz.open(local_pdf_path)
        text = ""
        for page in doc:
            text += page.get_text()

        # Your regex pattern
        regex_pattern = r'Indication:\s*(.*)'
        matches = re.search(regex_pattern, text)
        if matches:
            extracted_data = matches.group(1)
            item1['description_data'] = extracted_data
            print("Extracted Data:", extracted_data)
        else:
            print("No match found.")

        date_search = re.search(r"Electronically Signed By: .+, (\d{2}/\d{2}/\d{4})", text)
        if date_search:
            date = date_search.group(1)
            print("Extracted Date:", date)
        else:
            print("Date not found.")

        referredby_name = re.search(r"Electronically Signed By: ([^,]+),", text)
        if referredby_name:
            referrer_name = referredby_name.group(1)
            print("Referrer Name:", referrer_name)
        else:
            print("Referrer name not found.")

        try:
            conn1.update_one({"PatientID": patient_id}, {'$set': {'description_data': extracted_data}}, upsert=True)
            TGREEN = '\033[1;32m'
            print(TGREEN + '\rData Inserted ...')
        except Exception as e:
            print(e)

        try:
            conn1.update_one({"PatientID": patient_id}, {'$set': {'description_date': date}}, upsert=True)
            TGREEN = '\033[1;32m'
            print(TGREEN + '\rData Inserted ...')
        except Exception as e:
            print(e)

        try:
            conn1.update_one({"PatientID": patient_id}, {'$set': {'referredby_name': referrer_name}}, upsert=True)
            TGREEN = '\033[1;32m'
            print(TGREEN + '\rData Inserted ...')
        except Exception as e:
            print(e)

        try:
            conn1.update_one({"PatientID": patient_id}, {'$set': {'status': "done"}}, upsert=True)
            TGREEN = '\033[1;32m'
            print(TGREEN + '\rStatus update successfully')
        except Exception as e:
            print(e)

        os.remove(local_pdf_path)

    except Exception as e:
        print("Error processing patient data:", e)

def pdf_data():
    db = "store_data"
    table = "patient_data"
    conn = pymongo.MongoClient("mongodb+srv://rohannagadiya:sPQQNqNbp2vE3A3Z@database.56qutkr.mongodb.net/")
    mydb = conn[db]

    conn1 = mydb[table]

    directory_path = "../scrapping/pdf_path"
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

    db_url = conn1.find({"status": "pending"})

    # Create a list to store thread objects
    threads = []

    for data in db_url:
        patient_id = data['PatientID']
        # Create a thread for each patient and start it
        thread = threading.Thread(target=process_patient_data, args=(patient_id,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    print("All patient data processed.")

if __name__ == "__main__":
    pdf_data()
