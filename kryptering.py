import base64
import hashlib
import hmac
import os
import requests 

username = "Aasmune"

# encrypt username
key = os.environ['key'].encode('utf-8')
cipher = base64.b64encode(key)
h = hmac.new(key, username.encode('utf-8'), hashlib.sha256)
hmac = base64.b64encode(h.digest())
print(hmac.decode('utf-8'))

# decrypt username
dec = base64.b64decode(hmac)
print(dec.decode('utf-8'))


