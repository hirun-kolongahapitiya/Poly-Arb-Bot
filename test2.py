import requests

url = "https://api.polymarket.us/v1/markets"  # REST endpoint
headers = {
    "X-PM-Access-Key": "019c5152-97eb-791f-93cd-d7ba79eee5e9",
    "X-PM-Timestamp": "1771844695346",
    "X-PM-Signature": "EWLUftGtNk4GGsJT+VyIaTj29eQC0VUpPknKrud1t2LtPRsDTCHd94L9U13Wv4UXyeACAKSfZ5grRz+RVtwABQ==",
}

resp = requests.get(url, headers=headers)
print(resp.status_code)
print(resp.text)