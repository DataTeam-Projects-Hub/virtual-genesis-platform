import os
import time
import traceback
from web3 import Web3
from web3._utils.events import get_event_data
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from dotenv import load_dotenv
from eth_abi import decode

load_dotenv()                                   #loads our environment variables from our env file
mongolink = os.getenv("MONGO_URI")              #setting up the mongodp link variable
alchemylink = os.getenv("ALCHEMY_HTTP_URL")     #setting up the variable to link with alchemy - for blockchain data

client = MongoClient(mongolink)                 #connect to MongoDB using the connection string we have
db = client['virtualgenesis']                   #in it, select the database called virtualgenesis
collection = db['personas']                     #in that database, select the "personas" collection
collection.delete_many({ "tx_hash": None })
collection.create_index([('tx_hash', ASCENDING)], unique=True)        #an ascending index is created on each uniquely identified transaction hash

w3 = Web3(Web3.HTTPProvider(alchemylink))
contractaddress = Web3.to_checksum_address("0x71B8EFC8BCaD65a5D9386D07f2Dff57ab4EAf533")
npTopic = "0xf9d151d23a5253296eb20ab40959cf48828ea2732d337416716e302ed83ca658"
genesistargetaddress = Web3.to_checksum_address("0x42f4f5A3389CA0BeD694dE339f4d432aCdDb1Ea9")

npABI = {                                       #THE ABI FRAGMENT
    "anonymous": False,
    "inputs": [
        {"indexed": False, "internalType": "uint256", "name": "virtualId", "type": "uint256"},
        {"indexed": False, "internalType": "address", "name": "token", "type": "address"},
        {"indexed": False, "internalType": "address", "name": "dao", "type": "address"},
        {"indexed": False, "internalType": "address", "name": "tba", "type": "address"},
        {"indexed": False, "internalType": "address", "name": "veToken", "type": "address"},
        {"indexed": False, "internalType": "address", "name": "lp", "type": "address"},
    ], "name": "NewPersona", "type": "event"
}
                        
def npeventDecoder(log):                        #NOW WE DECODE EACH LOG BASED ON OUR ABI FRAGMENT
    try:
        decoded = get_event_data(w3.codec, npABI, log)
        args = decoded["args"]
        return {
            "blockNumber": log["blockNumber"],
            "txHash": log["transactionHash"].hex(),
            "virtualId": args["virtualId"],
            "tokenaddress": Web3.to_checksum_address(args["token"]),
            "daoaddress": Web3.to_checksum_address(args["dao"]),
            "tba": Web3.to_checksum_address(args["tba"]),
            "veToken": Web3.to_checksum_address(args["veToken"]),
            "lpaddress": Web3.to_checksum_address(args["lp"]),
            "timestamp": int(time.time())
        }
    except Exception as e:
        print(f"Error while log decoding : {e}")
        return None
    
def isTxGenesis(tx_hash):                       #A METHOD TO CHECK WHETHER THE TRANSACTION IS A GENESIS TRANSACTION OR NOT
    try:
        tx = w3.eth.get_transaction(tx_hash)
        return Web3.to_checksum_address(tx["to"]) == genesistargetaddress   #TRUE if Genesis Transaction, otherwise False
    except Exception as e:
        print(f"Error while fetching transaction {tx_hash}: {e}")
        return False                                                        #False even if unable to fetch a record
    
def tokenMetadata(tokenaddress):                #EXTRACTING THE TOKEN METADATA
    name = "unknown"
    symbol = "unknown"
    try:
        name_data = w3.eth.call({"to": tokenaddress, "data": "0x06fdde03"})
        name = decode(["string"], name_data)[0]
    except Exception:
        pass
    try:
        symbol_data = w3.eth.call({"to": tokenaddress, "data": "0x95d89b41"})
        symbol = decode(["string"], symbol_data)[0]
    except Exception:
        pass
    return name, symbol                        #THESE ARE THE TWO STRING VALUES THIS FUNCTION RETURNS - ARE USED LATER IN npFetchStore

def getLatestBlock():                          #EXTRACTS THE LATEST BLOCK NUMBER
    return w3.eth.block_number

def npFetchStore(from_block, to_block):
    print(f"Checking blocks :: [{from_block}] -TO- [{to_block}]")
    try:
        logs = w3.eth.get_logs({
            "fromBlock": from_block,
            "toBlock": to_block,
            "address": contractaddress,
            "topics": [npTopic]
        })
    except Exception as e:
        print(f"Error encountered while fetching logs: {e}")
        return

    for log in logs:
        try:
            persona_data = npeventDecoder(log)
            if not persona_data:
                continue  # Skip if decoding failed (returns None)

            if not persona_data.get("txHash"):
                print("SKIPPING LOG : MISSING TRANSACTION HASH")
                continue

            tx_hash = persona_data["txHash"]
            if isTxGenesis(tx_hash):
                tokenaddress = persona_data["token"]
                name, symbol = tokenMetadata(tokenaddress)
                persona_data["name"] = name
                persona_data["symbol"] = symbol

                print(f"GENESIS TOKEN CONFIRMED. Storing tx: {tx_hash} | name: {name}, symbol: {symbol}")
                try:
                    collection.insert_one(persona_data)
                except DuplicateKeyError:
                    print(f"~TRANSACTION FOR {tx_hash} EXISTS ALREADY. SKIPPING DUPLICATE INSERT~")
                
            else:
                print(f"NOT A GENESIS TOKEN (tx.to != target): {tx_hash}")

        except Exception as e:
            print(f"PROCESSING ERROR: {e}\n{traceback.format_exc()}")


def main():
    checkpoint = db["checkpoints"].find_one({"_id": "last_block"})
    last_checked = checkpoint["block"] if checkpoint else getLatestBlock()
    print(f"Starting from block: {last_checked}")

    while True:
        try:
            time.sleep(5)
            current_block = getLatestBlock()
            if current_block > last_checked:
                npFetchStore(last_checked, current_block)
                last_checked = current_block
                db["checkpoints"].update_one(
                    {"_id": "last_block"},
                    {"$set": {"block": last_checked}},
                    upsert=True
                )
        except Exception as e:
            print(f"ERROR IN MAIN LOOP: {e}\n{traceback.format_exc()}\n ---PLEASE WAIT AS WE RETRY---")
            time.sleep(10)  # retry delay

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("INTERRUPTED - WILL RESUME ONCE CALLED TO RUN AGAIN")