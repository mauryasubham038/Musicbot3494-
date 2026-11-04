# cookies.txt ko base64 me badalne ke liye: python make_cookies_b64.py
# Output ko Railway Variables me COOKIES_B64 naam se paste karo.
import base64
print(base64.b64encode(open("cookies.txt", "rb").read()).decode())
