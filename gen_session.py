# Assistant account ka session string banane ke liye: python gen_session.py
from pyrogram import Client
api_id = int(input("API_ID: "))
api_hash = input("API_HASH: ")
with Client("gen", api_id=api_id, api_hash=api_hash, in_memory=True) as app:
    print("\nSESSION_STRING:\n", app.export_session_string())
