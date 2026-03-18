# -*- coding: utf-8 -*-
"""
Created on Sun Mar  8 11:49:16 2026

@author: Subin-PC
"""

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

ORTHANC_URL = "http://localhost:8042"
CREDENTIALS = ("admin", "password")
MAX_WORKERS = 10  # Adjust based on your Orthanc server capacity

PATIENT_IDS = ["926818","667725"]

STUDY_INSTANCE_UIDS = [
    "1.2.840.113619.2.332.3.168453386.529.1743833025.260",
    "1.2.840.113619.2.332.3.168453386.441.1743166214.928"
    # Add more Study Instance UIDs here
]

session = requests.Session()
session.auth = CREDENTIALS

def find_and_delete_patient(patient_id):
    try:
        response = session.post(
            f"{ORTHANC_URL}/tools/find",
            json={"Level": "Patient", "Query": {"PatientID": patient_id}}
        )
        response.raise_for_status()
        uuids = response.json()

        if not uuids:
            return patient_id, "NOT FOUND", []

        deleted = []
        for uuid in uuids:
            del_response = session.delete(f"{ORTHANC_URL}/patients/{uuid}")
            del_response.raise_for_status()
            deleted.append(uuid)

        return patient_id, "OK", deleted

    except requests.RequestException as e:
        return patient_id, f"ERROR: {e}", []

def find_and_delete_study(study_instance_uid):
    try:
        response = session.post(
            f"{ORTHANC_URL}/tools/find",
            json={"Level": "Study", "Query": {"StudyInstanceUID": study_instance_uid}}
        )
        response.raise_for_status()
        uuids = response.json()

        if not uuids:
            return study_instance_uid, "NOT FOUND", []

        deleted = []
        for uuid in uuids:
            del_response = session.delete(f"{ORTHANC_URL}/studies/{uuid}")
            del_response.raise_for_status()
            deleted.append(uuid)

        return study_instance_uid, "OK", deleted

    except requests.RequestException as e:
        return study_instance_uid, f"ERROR: {e}", []

def main():
    # Step 1: Delete by Patient IDs
    print("=" * 50)
    print("STEP 1: Deleting by Patient IDs")
    print("=" * 50)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(find_and_delete_patient, pid): pid for pid in PATIENT_IDS}
        for future in as_completed(futures):
            patient_id, status, deleted = future.result()
            if status == "OK":
                print(f"[OK]  Patient {patient_id} → deleted {len(deleted)} record(s): {deleted}")
            elif status == "NOT FOUND":
                print(f"[--]  Patient {patient_id} → not found")
            else:
                print(f"[ERR] Patient {patient_id} → {status}")

    # Step 2: Delete by Study Instance UIDs
    print("\n" + "=" * 50)
    print("STEP 2: Deleting by Study Instance UIDs")
    print("=" * 50)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(find_and_delete_study, uid): uid for uid in STUDY_INSTANCE_UIDS}
        for future in as_completed(futures):
            study_uid, status, deleted = future.result()
            if status == "OK":
                print(f"[OK]  Study {study_uid} → deleted {len(deleted)} record(s): {deleted}")
            elif status == "NOT FOUND":
                print(f"[--]  Study {study_uid} → not found")
            else:
                print(f"[ERR] Study {study_uid} → {status}")

if __name__ == "__main__":
    main()
