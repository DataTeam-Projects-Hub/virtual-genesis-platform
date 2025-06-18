import os
import time
import requests
from web3 import Web3
from web3._utils.events import get_event_data
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from dotenv import load_dotenv
from eth_abi import decode

load_dotenv()

# --- Configuration ---
ALCHEMY_URL = os.getenv("ALCHEMY_HTTP_URL")
MONGO_URI = os.getenv("MONGO_URI")
BLOCK_NUMBER = 31640559  # Block to test

CONTRACT_ADDRESS = Web3.to_checksum_address("0x71B8EFC8BCaD65a5D9386D07f2Dff57ab4EAf533")
NEW_PERSONA_TOPIC = "0xf9d151d23a5253296eb20ab40959cf48828ea2732d337416716e302ed83ca658"
GENESIS_TARGET_TO_ADDRESS = Web3.to_checksum_address("0x42f4f5A3389CA0BeD694dE339f4d432aCdDb1Ea9")

# Setup Mongo & Web3
client = MongoClient(MONGO_URI)
db = client["virtualgenesis"]
persona_collection = db["personas"]
w3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))

# ABI for NewPersona Event
NEW_PERSONA_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": False, "internalType": "uint256", "name": "virtualId", "type": "uint256"},
        {"indexed": False, "internalType": "address", "name": "token", "type": "address"},
        {"indexed": False, "internalType": "address", "name": "dao", "type": "address"},
        {"indexed": False, "internalType": "address", "name": "tba", "type": "address"},
        {"indexed": False, "internalType": "address", "name": "veToken", "type": "address"},
        {"indexed": False, "internalType": "address", "name": "lp", "type": "address"},
    ],
    "name": "NewPersona",
    "type": "event"
}

def decode_new_persona_event(log):
    decoded = get_event_data(w3.codec, NEW_PERSONA_ABI, log)
    args = decoded["args"]
    return {
        "blockNumber": int(log["blockNumber"], 16),
        "tx_hash": log["transactionHash"],
        "virtualId": args["virtualId"],
        "token": Web3.to_checksum_address(args["token"]),
        "dao": Web3.to_checksum_address(args["dao"]),
        "tba": Web3.to_checksum_address(args["tba"]),
        "veToken": Web3.to_checksum_address(args["veToken"]),
        "lp": Web3.to_checksum_address(args["lp"]),
        "timestamp": int(time.time())
    }

def fetch_logs():
    print(f"Fetching events from block {BLOCK_NUMBER}...")
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [{
            "fromBlock": hex(BLOCK_NUMBER),
            "toBlock": hex(BLOCK_NUMBER),
            "address": CONTRACT_ADDRESS,
            "topics": [NEW_PERSONA_TOPIC]
        }]
    }
    response = requests.post(ALCHEMY_URL, json=payload)
    if response.status_code != 200:
        print("Error fetching logs:", response.text)
        return []
    return response.json().get("result", [])

def is_genesis_tx(tx_hash):
    try:
        tx = w3.eth.get_transaction(tx_hash)
        return Web3.to_checksum_address(tx["to"]) == GENESIS_TARGET_TO_ADDRESS
    except Exception as e:
        print(f"Error fetching transaction {tx_hash}: {e}")
        return False

def fetch_token_metadata(token_address):
    name = "unknown"
    symbol = "unknown"

    try:
        name_data = w3.eth.call({
            "to": token_address,
            "data": "0x06fdde03"
        })
        name = decode(["string"], name_data)[0]
    except Exception as e:
        print(f"Error fetching name: {e}")

    try:
        symbol_data = w3.eth.call({
            "to": token_address,
            "data": "0x95d89b41"
        })
        symbol = decode(["string"], symbol_data)[0]
    except Exception as e:
        print(f"Error fetching symbol: {e}")

    return name, symbol

def main():
    persona_logs = fetch_logs()
    if not persona_logs:
        print("No NewPersona events found.")
        return

    for log in persona_logs:
        persona = decode_new_persona_event(log)
        tx_hash = persona.get("tx_hash")

        if not tx_hash:
            print(f"⚠️ Skipping insert: tx_hash is missing or None in persona: {persona}")
            continue

        if is_genesis_tx(tx_hash):
            token_address = persona.get("token")
            name, symbol = fetch_token_metadata(token_address)
            persona["name"] = name
            persona["symbol"] = symbol
            persona["txHash"] = tx_hash

            if not persona["txHash"]:
                print(f"⚠️ Skipping insert: still missing txHash even after assignment: {persona}")
                continue

            print(f"✅ Genesis token confirmed. Storing event from tx: {tx_hash} | name: {name}, symbol: {symbol}")
            try:
                persona_collection.insert_one(persona)
            except DuplicateKeyError:
                print(f"⚠️ Duplicate txHash skipped: {tx_hash}")
        else:
            print(f"❌ Not a genesis token (tx.to != target): {tx_hash}")

if __name__ == "__main__":
    main()
