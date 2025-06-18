import os
import time
import threading
from web3 import Web3
from pymongo import MongoClient
from dotenv import load_dotenv
from eth_abi import decode

load_dotenv()
mongolink = os.getenv("MONGO_URI")
alchemylink = os.getenv("ALCHEMY_HTTP_URL")

client = MongoClient(mongolink)
db = client["virtualgenesis"]

w3 = Web3(Web3.HTTPProvider(alchemylink))

SWAP_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"

tracked_tokens = set()

def swapDataDecoder(data):
    expected_length = 128  # 4 * 32 bytes
    if len(data) != expected_length:
        raise ValueError(f"INVALID LOG DATA LENGTH :: EXPECTED : {expected_length} BYTES || GOT {len(data)}")

    types = ["uint256", "uint256", "uint256", "uint256"]
    decoded = decode(types, data)  # pass HexBytes directly
    return {"amount0In": decoded[0] / 1e18, "amount1In": decoded[1] / 1e18, "amount0Out": decoded[2] / 1e18, "amount1Out": decoded[3] / 1e18,}

def swapTypeFinder(decoded):
    if decoded["amount0In"] > 0 and decoded["amount1Out"] > 0:
        return "buy"
    elif decoded["amount1In"] > 0 and decoded["amount0Out"] > 0:
        return "sell"
    else:
        return "unknown"
    
def track_lp_swaps(lp_address, token_symbol, token_address, start_block):
    lp_address = Web3.to_checksum_address(lp_address)
    token_address = Web3.to_checksum_address(token_address)
    collection = db[f"{token_symbol.lower()}_swap"]
    progress_coll = db["swap_progress"]
    progress_entry = progress_coll.find_one({"lp": lp_address})
    if progress_entry:
        last_processed = progress_entry.get("lastProcessed", start_block)
        if last_processed >= start_block:
            current_block = last_processed + 1
            print(f"Resuming {token_symbol} from checkpoint at block {current_block}")
        else:
            current_block = start_block
    else:
        current_block = start_block
        print(f"No checkpoint found. Starting {token_symbol} at block {current_block}")

    print(f"Ὠ0 Tracking swaps for {token_symbol} (LP: {lp_address}) starting at block {start_block}...")
    chunk_size = 500

    while True:
        latest_block = w3.eth.block_number
        to_block = min(current_block + chunk_size - 1, latest_block)

        if current_block > to_block:
            time.sleep(5)
            continue

        try:
            print(f"PROCESSING BLOCK :: {current_block} :TO: {to_block} :: FOR :  {token_symbol}")
            logs = w3.eth.get_logs({
                "fromBlock": current_block,
                "toBlock": to_block,
                "address": lp_address,
                "topics": [SWAP_TOPIC]
            })

            logs_by_tx = {}
            for log in logs:
                tx_hash = log["transactionHash"].hex()
                if tx_hash not in logs_by_tx:
                    logs_by_tx[tx_hash] = []
                logs_by_tx[tx_hash].append(log)

            for tx_hash, tx_logs in logs_by_tx.items():
                tx = w3.eth.get_transaction(tx_hash)
                block = w3.eth.get_block(tx["blockNumber"])
                block_timestamp = block["timestamp"]

                user_tax_value = None
                user_token = None
                user_token_field = None

                pending_auto_swaps = []

                for log in tx_logs:
                    try:
                        decoded = swapDataDecoder(log["data"])
                    except Exception as e:
                        print(f"Skipping log due to decode error: {e}")
                        continue

                    to_address = Web3.to_checksum_address(log["topics"][2][-20:].hex())
                    is_auto_swap = to_address == "0x7E26173192D72fd6D75A759F888d61c2cdbB64B1"
                    swap_type = swapTypeFinder(decoded)
                    token0 = "Virtual"
                    token1 = token_symbol.upper()

                    if is_auto_swap:
                        pending_auto_swaps.append((log, decoded, swap_type))
                        continue

                    swap_data = {
                        "blockNumber": log["blockNumber"],
                        "txHash": f"0x{tx_hash}",
                        "txLink": f"https://basescan.org/tx/0x{tx_hash}",
                        "lp": lp_address,
                        "maker": tx["from"],
                        "swapType": swap_type,
                        "timestamp": int(time.time()),
                        "timestampReadable": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(block_timestamp)),
                        "label": "swap",
                        "receiver": tx["from"]
                    }

                    if swap_type == "buy":
                        amount_in = decoded["amount0In"]
                        amount_out = decoded["amount1Out"]

                        swap_data[f"{token0}_IN"] = amount_in
                        swap_data[f"{token1}_OUT"] = amount_out

                        tax_amount = amount_out * 0.01
                        pre_tax = amount_out
                        post_tax = amount_out - tax_amount

                        swap_data["Tax_1pct"] = tax_amount
                        swap_data[f"{token1}_OUT_BeforeTax"] = pre_tax
                        swap_data[f"{token1}_OUT_AfterTax"] = post_tax

                        user_tax_value = tax_amount
                        user_token = token1
                        user_token_field = f"{token1}_OUT"

                    elif swap_type == "sell":
                        amount_in = decoded["amount1In"]
                        amount_out = decoded["amount0Out"]

                        swap_data[f"{token1}_IN"] = amount_in
                        swap_data[f"{token0}_OUT"] = amount_out

                        tax_amount = amount_in * 0.01 / 0.99
                        pre_tax = amount_in + tax_amount
                        post_tax = amount_in

                        swap_data["Tax_1pct"] = tax_amount
                        swap_data[f"{token1}_IN_BeforeTax"] = pre_tax
                        swap_data[f"{token1}_IN_AfterTax"] = post_tax

                        user_tax_value = tax_amount
                        user_token = token1
                        user_token_field = f"{token1}_IN"

                    collection.insert_one(swap_data)
                    print(f"{swap_type.upper()} swap stored: {tx_hash}")

                if pending_auto_swaps:
                    for log, decoded, swap_type in pending_auto_swaps:
                        swap_data = {
                            "blockNumber": log["blockNumber"],
                            "txHash": f"0x{tx_hash}",
                            "txLink": f"https://basescan.org/tx/0x{tx_hash}",
                            "lp": lp_address,
                            "maker": token_address,
                            "swapType": swap_type,
                            "timestamp": int(time.time()),
                            "timestampReadable": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(block_timestamp)),
                            "label": "auto-swap" if user_tax_value is not None else "auto-swap-outside-transfer",
                            "receiver": "0x7E26173192D72fd6D75A759F888d61c2cdbB64B1"
                        }

                        token0 = "Virtual"
                        token1 = token_symbol.upper()

                        if swap_type == "buy":
                            amount_in = decoded["amount0In"]
                            amount_out = decoded["amount1Out"]
                            swap_data[f"{token0}_IN"] = amount_in
                            swap_data[f"{token1}_OUT"] = amount_out

                            if user_tax_value is not None:
                                updated_amount = amount_out - user_tax_value
                                swap_data["Tax_1pct"] = user_tax_value
                                swap_data[f"{user_token_field}_BeforeTax"] = amount_out
                                swap_data[f"{user_token_field}_AfterTax"] = updated_amount

                        elif swap_type == "sell":
                            amount_in = decoded["amount1In"]
                            amount_out = decoded["amount0Out"]
                            swap_data[f"{token1}_IN"] = amount_in
                            swap_data[f"{token0}_OUT"] = amount_out

                            if user_tax_value is not None:
                                updated_amount = amount_in - user_tax_value
                                swap_data["Tax_1pct"] = user_tax_value
                                swap_data[f"{user_token_field}_BeforeTax"] = amount_in
                                swap_data[f"{user_token_field}_AfterTax"] = updated_amount

                        collection.insert_one(swap_data)
                        print(f"AUTO-{swap_type.upper()} swap stored: {tx_hash}")

        except Exception as e:
            print(f"Error in block range {current_block}-{to_block}: {e}")
        progress_coll.update_one(
            {"lp": lp_address},
            {"$set": {"lastProcessed": to_block}},
            upsert=True
        )

        current_block = to_block + 1
        time.sleep(1)

def newTokenMonitor():
    while True:
        try:
            personas = db["personas"].find()
            for persona in personas:
                token_address = persona.get("token")
                token_symbol = persona.get("symbol", "unknown")
                lp_address = persona.get("lp")
                block_number = persona.get("blockNumber", w3.eth.block_number)

                if not token_address:
                    print(f"Skipping {token_symbol} due to missing token address")
                    continue

                if lp_address and lp_address.lower() not in tracked_tokens:
                    tracked_tokens.add(lp_address.lower())
                    threading.Thread(
                        target=track_lp_swaps,
                        args=(lp_address, token_symbol, token_address, block_number),
                        daemon=True
                    ).start()
                    print(f"STARTED TRACKING: {token_symbol} WITH LIQUIDITY POOL : ({lp_address})")

        except Exception as e:
            print(f"ERROR IN ::  newTokenMonitor: {e}")

        time.sleep(10)

def main():
    threading.Thread(target=newTokenMonitor, daemon=True).start()
    print("----------TOKEN MONITOR STARTED :: WAITING FOR NEW TOKENS TO TRACK----------")
    while True:
        time.sleep(10)

if __name__ == "__main__":
    main()