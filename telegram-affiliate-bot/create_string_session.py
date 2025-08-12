from telethon.sync import TelegramClient
from telethon.sessions import StringSession

print("🔹 API_ID e API_HASH da https://my.telegram.org")

api_id = int(input("API_ID: ").strip())
api_hash = input("API_HASH: ").strip()

with TelegramClient(StringSession(), api_id, api_hash) as client:
    print("Accesso effettuato come:", client.get_me().username)
    print("\nCOPIA la seguente STRING_SESSION e salvala come SECRET (STRING_SESSION):\n")
    print(client.session.save())
    input("\nPremi INVIO per chiudere...")
