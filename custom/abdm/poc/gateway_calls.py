from datetime import datetime, timedelta
from custom.abdm.milestone_one.utils.request_util import get_response_http_post

X_CM_ID = 'sbx'     # sandbox consent manager id
PATIENT_ON_SHARE_GW_URL = '/v1.0/patients/profile/on-share'
CARE_CONTEXT_ON_DISCOVER_GW_URL = '/v0.5/care-contexts/on-discover'
CARE_CONTEXT_LINK_ON_INIT_GW_URL = '/v0.5/links/link/on-init'
CARE_CONTEXT_LINK_ON_CONFIRM_GW_URL = '/v0.5/links/link/on-confirm'

CONSENT_ON_NOTIFY_GW_URL = '/v0.5/consents/hip/on-notify'


# TODO : Send error if validation fails, remove hard coding for all below calls

def gw_patient_profile_on_share(request_id, health_id):
    print("Sending callback request to gateway: gw_patient_profile_on_share")
    data = {
        "requestId": "5f7a535d-a3fd-416b-b069-c97d021fbacd",
        "timestamp": datetime.utcnow().isoformat(),
        "acknowledgement": {
            "status": "SUCCESS",
            "healthId": health_id,
            "tokenNumber": "101"
        },
        # "error": {
        #     "code": 1000,
        #     "message": "Not a valid request"
        # },
        "resp": {
            "requestId": request_id
        }
    }
    additional_headers = {'X-CM-ID': X_CM_ID}
    print(get_response_http_post(api_url=PATIENT_ON_SHARE_GW_URL, payload=data,
                                 additional_headers=additional_headers))


def gw_care_context_on_discover(request_id, transaction_id):
    print("Sending callback request to gateway: gw_care_context_on_discover")
    data = {
        "requestId": "5f7a535d-a3fd-416b-b069-c97d021fbacd",
        "timestamp": datetime.utcnow().isoformat(),
        "transactionId": transaction_id,
        "patient": {
            "referenceNumber": "PT-101",
            "display": "Ajeet",
            "careContexts": [
                {
                    "referenceNumber": "CC-101",
                    "display": "Dummy Visit 01 to Ashish Eye Care"
                },
                {
                    "referenceNumber": "CC-102",
                    "display": "Dummy Visit 02 to Ashish Eye Care"
                }
            ],
            "matchedBy": [
                "MOBILE"
            ]
        },
        "resp": {
            "requestId": request_id
        }
    }
    additional_headers = {'X-CM-ID': X_CM_ID}
    print(get_response_http_post(api_url=CARE_CONTEXT_ON_DISCOVER_GW_URL, payload=data,
                                 additional_headers=additional_headers))


def gw_care_context_on_init(request_id, transaction_id):
    print("Sending callback request to gateway: gw_care_context_on_init")
    data = {
        "requestId": "5f7a535d-a3fd-416b-b069-c97d021fbacd",
        "timestamp": datetime.utcnow().isoformat(),
        "transactionId": transaction_id,
        "link": {
            "referenceNumber": "LNK-101",
            "authenticationType": "DIRECT",
            "meta": {
                "communicationMedium": "MOBILE",
                "communicationHint": "8291123177",
                "communicationExpiry": (datetime.utcnow() + timedelta(minutes=10)).isoformat()
            }
        },
        "resp": {
            "requestId": request_id
        }
    }
    additional_headers = {'X-CM-ID': X_CM_ID}
    print(get_response_http_post(api_url=CARE_CONTEXT_LINK_ON_INIT_GW_URL, payload=data,
                                 additional_headers=additional_headers))


def gw_care_context_on_confirm(request_id):
    print("Sending callback request to gateway: gw_care_context_on_confirm")
    additional_headers = {'X-CM-ID': X_CM_ID}
    data = {
        "requestId": "5f7a535d-a3fd-416b-b069-c97d021fbacd",
        "timestamp": datetime.utcnow().isoformat(),
        "patient": {
            "referenceNumber": "PT-101",
            "display": "Ajeet",
            "careContexts": [
                {
                    "referenceNumber": "CC-101",
                    "display": "Dummy Visit 01 to Ashish Eye Care"
                },
                {
                    "referenceNumber": "CC-102",
                    "display": "Dummy Visit 02 to Ashish Eye Care"
                }
            ]
        },
        "resp": {
            "requestId": request_id
        }
    }
    print(get_response_http_post(api_url=CARE_CONTEXT_LINK_ON_CONFIRM_GW_URL, payload=data,
                                 additional_headers=additional_headers))


def gw_consents_on_notify(request_id, consent_id):
    print("Sending callback request to gateway: gw_consents_on_notify")
    data = {
        "requestId": "5f7a535d-a3fd-416b-b069-c97d021fbacd",
        "timestamp": datetime.utcnow().isoformat(),
        "acknowledgement": {
            "status": "SUCCESS",
            "consentId": consent_id,
        },
        "resp": {
            "requestId": request_id
        }
    }
    additional_headers = {'X-CM-ID': X_CM_ID}
    print(get_response_http_post(api_url=CONSENT_ON_NOTIFY_GW_URL, payload=data,
                                 additional_headers=additional_headers))
