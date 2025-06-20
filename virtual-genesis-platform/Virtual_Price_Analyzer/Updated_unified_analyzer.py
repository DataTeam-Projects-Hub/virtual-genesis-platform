"""
Virtual Genesis Unified Analyzer
================================

This script combines real-time tracking and analysis:
1. Tracks Genesis token swaps from Uniswap V3 pools in real-time
2. Immediately analyzes each swap with price calculations
3. Stores analyzed results in MongoDB
4. Outputs all swap and price details to console and CSV

"""

import csv
import time
import threading
from pymongo import MongoClient
from web3 import Web3
import math
from datetime import datetime
import os
from dotenv import load_dotenv
from eth_abi.abi import decode

# Load environment variables from .env file
load_dotenv()

# =============================================================================
# CONFIGURATION
# =============================================================================

# MongoDB Connection (from environment variables)
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME", "virtualgenesis")

# Blockchain RPC (from environment variables)
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY")
ALCHEMY_HTTP_URL = os.getenv("ALCHEMY_HTTP_URL")

# Validate required environment variables
if not MONGO_URI:
    raise ValueError("MONGO_URI environment variable is required. Please set it in your .env file.")
if not ALCHEMY_API_KEY and not ALCHEMY_HTTP_URL:
    raise ValueError("Either ALCHEMY_API_KEY or ALCHEMY_HTTP_URL environment variable is required.")

# Use ALCHEMY_HTTP_URL if available, otherwise construct from API key
if ALCHEMY_HTTP_URL:
    ALCHEMY_URL = ALCHEMY_HTTP_URL
else:
    ALCHEMY_URL = f"https://base-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"

# Token Addresses (Base Network)
VIRTUAL_TOKEN_ADDRESS = "0x0b3e328455c4059eeb9e3f84b5543f74e24e7e1b"  # VIRTUAL token
USDC_ADDRESS = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"  # USDC token

# Uniswap V3 Pool Addresses
VIRTUAL_USDC_POOL = "0x529d2863a1521d0b57db028168fde2e97120017c"  # VIRTUAL/USDC pool

# Analysis Settings
MAX_BLOCK_RETRIES = 5  # Maximum blocks to try when fetching price
VERBOSE_OUTPUT = False  # Set to True for detailed output
SHOW_TRANSACTION_DETAILS = True  # Set to True to show each transaction details
UPDATE_MONGODB = True  # Set to True to update MongoDB documents with calculated prices
ENABLE_REAL_TIME_TRACKING = True  # Set to True to enable real-time swap tracking
ENABLE_PRICE_ANALYSIS = True  # Set to True to enable price analysis
PROCESS_TOKENS_SEQUENTIALLY = False  # Process all tokens in parallel for speed
RATE_LIMIT_DELAY = 0  # Minimal delay for faster processing
ERROR_RETRY_DELAY = 2  # Reduce delay on errors (seconds)

# Swap tracking settings
SWAP_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
CHUNK_SIZE = 2000  # Increased block processing chunk size for speed

# =============================================================================
# BLOCKCHAIN CONNECTIONS
# =============================================================================

# Initialize Web3 connection to Base network
w3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))

# Initialize MongoDB connection
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]

# Tracked tokens set
tracked_tokens = set()

# =============================================================================
# UNISWAP V3 POOL ABI (Minimal functions needed)
# =============================================================================

POOL_ABI = [
    {
        "inputs": [],
        "name": "slot0",
        "outputs": [
            {"internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160"},
            {"internalType": "int24", "name": "tick", "type": "int24"},
            {"internalType": "uint16", "name": "observationIndex", "type": "uint16"},
            {"internalType": "uint16", "name": "observationCardinality", "type": "uint16"},
            {"internalType": "uint16", "name": "observationCardinalityNext", "type": "uint16"},
            {"internalType": "uint8", "name": "feeProtocol", "type": "uint8"},
            {"internalType": "bool", "name": "unlocked", "type": "bool"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "token0",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "token1",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def web3_call_with_retries(func, max_retries=3, delay=1):
    """
    Execute a Web3 call with retry logic and delays
    
    Args:
        func: Function to execute
        max_retries: Maximum number of retries
        delay: Delay between retries in seconds
    
    Returns:
        Result of the function call
    """
    for attempt in range(max_retries + 1):
        try:
            result = func()
            if attempt > 0:
                time.sleep(delay)  # Add delay after successful retry
            return result
        except Exception as e:
            if attempt == max_retries:
                raise e
            if VERBOSE_OUTPUT:
                print(f"⚠️ Web3 call failed (attempt {attempt + 1}/{max_retries + 1}): {e}")
            time.sleep(delay * (attempt + 1))  # Exponential backoff

# =============================================================================
# PRICE CALCULATION FUNCTIONS
# =============================================================================

def sqrtPriceX96ToPrice(sqrtPriceX96, token0IsVirtual):
    """
    Convert sqrtPriceX96 to price using the exact logic
    
    Args:
        sqrtPriceX96: The sqrt price from Uniswap V3 slot0
        token0IsVirtual: Boolean indicating if VIRTUAL is token0 in the pool
    
    Returns:
        float: Price in human-readable format
    """
    # Convert sqrtPriceX96 to price
    # Formula: price = (sqrtPriceX96 / 2^96)^2
    
    # Use Python float to avoid precision issues
    sqrtPriceNum = float(sqrtPriceX96)
    Q96 = 2 ** 96
    
    # Calculate the raw price (token1 per token0) in wei units
    sqrtPriceNormalized = sqrtPriceNum / Q96
    rawPrice = sqrtPriceNormalized ** 2
    
    # The rawPrice is in wei units: (token1_wei / token0_wei)
    # For VIRTUAL/USDC pool: token0=VIRTUAL (18 decimals), token1=USDC (6 decimals)
    # rawPrice = USDC_wei / VIRTUAL_wei
    
    if token0IsVirtual:
        # rawPrice = USDC_wei / VIRTUAL_wei
        # To get human readable USDC per VIRTUAL:
        # (USDC_wei / 10^6) / (VIRTUAL_wei / 10^18) = rawPrice * 10^12
        usdcPerVirtual = rawPrice * (10 ** 12)
        
        # But we want VIRTUAL per USDC, so invert
        finalPrice = 1 / usdcPerVirtual
    else:
        # rawPrice = VIRTUAL_wei / USDC_wei  
        # To get human readable VIRTUAL per USDC:
        # (VIRTUAL_wei / 10^18) / (USDC_wei / 10^6) = rawPrice / 10^12
        finalPrice = rawPrice / (10 ** 12)
    
    return finalPrice

def get_virtual_usdc_price(block_number, max_retries=None):
    """
    Get Virtual/USDC price from Uniswap V3 pool at specific block
    Using the exact logic with fallback to nearby blocks
    
    Args:
        block_number: The blockchain block number to fetch price at
        max_retries: Maximum number of nearby blocks to try (uses config if None)
    
    Returns:
        float: Virtual token price in USDC, or 0.0 if error
    """
    if max_retries is None:
        max_retries = MAX_BLOCK_RETRIES
    
    # Try the exact block first
    for offset in range(max_retries + 1):
        try_block = block_number + offset
        
        try:
            # Create contract instance
            pool = w3.eth.contract(
                address=Web3.to_checksum_address(VIRTUAL_USDC_POOL), 
                abi=POOL_ABI
            )
            
            # Get pool info at the specific block
            slot0 = pool.functions.slot0().call(block_identifier=try_block)
            token0 = pool.functions.token0().call()
            token1 = pool.functions.token1().call()
            
            # Check if VIRTUAL is token0
            token0IsVirtual = token0.lower() == VIRTUAL_TOKEN_ADDRESS.lower()
            
            # Calculate price using logic
            price = 1 / sqrtPriceX96ToPrice(slot0[0], token0IsVirtual)
            
            # Validate price
            if price <= 0 or not math.isfinite(price):
                raise Exception(f"Invalid price calculated: {price}")
            
            if offset > 0 and VERBOSE_OUTPUT:
                print(f"✅ Found VIRTUAL price at block {try_block} (original: {block_number})")
            return price
            
        except Exception as e:
            if offset == 0 and VERBOSE_OUTPUT:
                print(f"⚠️ Block {block_number} failed, trying nearby blocks...")
            continue
    
    if VERBOSE_OUTPUT:
        print(f"❌ Failed to get VIRTUAL price after trying {max_retries + 1} blocks starting from {block_number}")
    return 0.0

# =============================================================================
# SWAP TRACKING FUNCTIONS
# =============================================================================

def swapDataDecoder(data):
    """Decode swap event data"""
    expected_length = 128  # 4 * 32 bytes
    if len(data) != expected_length:
        raise ValueError(f"INVALID LOG DATA LENGTH :: EXPECTED : {expected_length} BYTES || GOT {len(data)}")

    types = ["uint256", "uint256", "uint256", "uint256"]
    decoded = decode(types, data)  # pass HexBytes directly
    return {
        "amount0In": decoded[0] / 1e18, 
        "amount1In": decoded[1] / 1e18, 
        "amount0Out": decoded[2] / 1e18, 
        "amount1Out": decoded[3] / 1e18,
    }

def swapTypeFinder(decoded):
    """Determine swap type from decoded data"""
    if decoded["amount0In"] > 0 and decoded["amount1Out"] > 0:
        return "buy"
    elif decoded["amount1In"] > 0 and decoded["amount0Out"] > 0:
        return "sell"
    else:
        return "unknown"

def enforce_latest_n_swaps(collection, n=10):
    """
    Keep only the latest n documents in the collection (by blockNumber descending).
    To disable this limit and store all swaps, simply comment out the call to this function.
    """
    count = collection.count_documents({})
    if count > n:
        # Find the _ids of the oldest documents to delete
        to_delete = collection.find({}, {"_id": 1}).sort("blockNumber", 1).limit(count - n)
        ids = [doc["_id"] for doc in to_delete]
        if ids:
            collection.delete_many({"_id": {"$in": ids}})

def track_lp_swaps(lp_address, token_symbol, token_address, start_block):
    """
    Track swaps for a specific liquidity pool
    
    Args:
        lp_address: Liquidity pool address
        token_symbol: Token symbol
        token_address: Token contract address
        start_block: Starting block number
    """
    lp_address = Web3.to_checksum_address(lp_address)
    token_address = Web3.to_checksum_address(token_address)
    collection = db[f"{token_symbol.lower()}_swap"]
    progress_coll = db["swap_progress"]
    
    # Check for existing progress
    progress_entry = progress_coll.find_one({"lp": lp_address})
    if progress_entry:
        last_processed = progress_entry.get("lastProcessed", start_block)
        if last_processed >= start_block:
            current_block = last_processed + 1
            print(f"🔄 Resuming {token_symbol} from checkpoint at block {current_block}")
        else:
            current_block = start_block
    else:
        current_block = start_block
        print(f"🚀 No checkpoint found. Starting {token_symbol} at block {current_block}")

    print(f"📊 Tracking swaps for {token_symbol} (LP: {lp_address}) starting at block {start_block}...")

    while True:
        try:
            latest_block = web3_call_with_retries(lambda: w3.eth.block_number)
            if latest_block is None:
                print("⚠️ Could not fetch latest block number, retrying...")
                time.sleep(5)
                continue
                
            to_block = min(current_block + CHUNK_SIZE - 1, latest_block)

            if current_block > to_block:
                time.sleep(5)
                continue
        except Exception as e:
            print(f"❌ Error fetching latest block: {e}")
            time.sleep(5)
            continue

        try:
            if VERBOSE_OUTPUT:
                print(f"🔍 Processing blocks {current_block} to {to_block} for {token_symbol}")
                
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
                try:
                    # Fetch transaction data with proper error handling and delays
                    tx = web3_call_with_retries(lambda: w3.eth.get_transaction(tx_hash))
                    time.sleep(RATE_LIMIT_DELAY)  # Rate limiting delay
                    
                    receipt = web3_call_with_retries(lambda: w3.eth.get_transaction_receipt(tx_hash))
                    time.sleep(RATE_LIMIT_DELAY)  # Rate limiting delay
                    
                    # Calculate transaction fee in ETH
                    gas_price = tx.get("gasPrice", 0) if tx else 0
                    gas_used = receipt.get("gasUsed", 0) if receipt else 0
                    fee_paid = (gas_price * gas_used) / 1e18  # Convert to ETH
                    
                    block_number = tx.get("blockNumber") if tx else None
                    if block_number is None:
                        print(f"⚠️ No block number found for transaction {tx_hash}")
                        continue
                        
                    block = web3_call_with_retries(lambda: w3.eth.get_block(block_number))
                    time.sleep(RATE_LIMIT_DELAY)  # Rate limiting delay
                    block_timestamp = block.get("timestamp", int(time.time())) if block else int(time.time())

                except (Exception, ConnectionError) as e:
                    print(f"❌ Network error while fetching tx/block data for {tx_hash}: {e}")
                    time.sleep(ERROR_RETRY_DELAY)  # Longer delay on error
                    continue

                user_tax_value = None
                user_token = None
                user_token_field = None

                pending_auto_swaps = []

                for log in tx_logs:
                    try:
                        decoded = swapDataDecoder(log["data"])
                    except Exception as e:
                        if VERBOSE_OUTPUT:
                            print(f"⚠️ Skipping log due to decode error: {e}")
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
                        "maker": tx.get("from", "unknown"),
                        "swapType": swap_type,
                        "timestamp": int(time.time()),
                        "timestampReadable": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(block_timestamp)),
                        "label": "swap",
                        "receiver": tx.get("from", "unknown"),
                        "gasPrice": gas_price,
                        "gasUsed": gas_used,
                        "transactionFee": fee_paid,  # ETH fee paid for the transaction
                        "gasPriceGwei": gas_price / 1e9 if gas_price else 0  # Gas price in Gwei
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

                    # Insert swap data
                    result = collection.insert_one(swap_data)
                    # Keep only the latest 10 swaps (comment out next line to store all swaps)
                    enforce_latest_n_swaps(collection, 10)
                    
                    if VERBOSE_OUTPUT:
                        print(f"💾 {swap_type.upper()} swap stored: {tx_hash}")

                    # Analyze price if enabled
                    if ENABLE_PRICE_ANALYSIS:
                        analysis_result = analyze_swap_transaction(swap_data, f"{token_symbol.lower()}_swap")
                        if analysis_result:
                            # Update MongoDB with calculated prices
                            if UPDATE_MONGODB:
                                update_data = {
                                    "virtual_usdc_price": analysis_result["virtual_usdc_price"],
                                    "genesis_virtual_price": analysis_result["genesis_virtual_price"],
                                    "genesis_usdc_price": analysis_result["genesis_usdc_price"],
                                    "genesis_token_name": analysis_result["genesis_token_name"],
                                }
                                
                                collection.update_one(
                                    {"_id": result.inserted_id},
                                    {"$set": update_data}
                                )

                            # Display transaction details
                            if SHOW_TRANSACTION_DETAILS:
                                print(
                                    f"  📊 Block {analysis_result['block']} | TX: {analysis_result['tx_hash'][:10]}... | "
                                    f"Token: {analysis_result['genesis_token_name']} | "
                                    f"Virtual/USDC: ${analysis_result['virtual_usdc_price']:.8f} | "
                                    f"{analysis_result['genesis_token_name']}/USDC: ${analysis_result['genesis_usdc_price']:.8f}"
                                )

                # Handle auto-swaps
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
                            "receiver": "0x7E26173192D72fd6D75A759F888d61c2cdbB64B1",
                            "gasPrice": gas_price,
                            "gasUsed": gas_used,
                            "transactionFee": fee_paid,  # ETH fee paid for the transaction
                            "gasPriceGwei": gas_price / 1e9 if gas_price else 0  # Gas price in Gwei
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

                        # Insert auto-swap data
                        result = collection.insert_one(swap_data)
                        # Keep only the latest 10 swaps (comment out next line to store all swaps)
                        enforce_latest_n_swaps(collection, 10)
                        
                        if VERBOSE_OUTPUT:
                            print(f"🤖 AUTO-{swap_type.upper()} swap stored: {tx_hash}")

                        # Analyze price if enabled
                        if ENABLE_PRICE_ANALYSIS:
                            analysis_result = analyze_swap_transaction(swap_data, f"{token_symbol.lower()}_swap")
                            if analysis_result:
                                # Update MongoDB with calculated prices
                                if UPDATE_MONGODB:
                                    update_data = {
                                        "virtual_usdc_price": analysis_result["virtual_usdc_price"],
                                        "genesis_virtual_price": analysis_result["genesis_virtual_price"],
                                        "genesis_usdc_price": analysis_result["genesis_usdc_price"],
                                        "genesis_token_name": analysis_result["genesis_token_name"],
                                    }
                                    
                                    collection.update_one(
                                        {"_id": result.inserted_id},
                                        {"$set": update_data}
                                    )

                                # Display transaction details
                                if SHOW_TRANSACTION_DETAILS:
                                    print(
                                        f"  🤖 Block {analysis_result['block']} | TX: {analysis_result['tx_hash'][:10]}... | "
                                        f"Token: {analysis_result['genesis_token_name']} | "
                                        f"Virtual/USDC: ${analysis_result['virtual_usdc_price']:.8f} | "
                                        f"{analysis_result['genesis_token_name']}/USDC: ${analysis_result['genesis_usdc_price']:.8f}"
                                    )

        except Exception as e:
            print(f"❌ Error in block range {current_block}-{to_block}: {e}")
            
        # Update progress
        progress_coll.update_one(
            {"lp": lp_address},
            {"$set": {"lastProcessed": to_block}},
            upsert=True
        )

        current_block = to_block + 1
        # time.sleep(1)  # Removed or minimized for speed

def newTokenMonitor():
    """Monitor for new tokens to track"""
    active_tokens = set()  # Track currently active tokens
    
    while True:
        try:
            personas = db["personas"].find()
            new_tokens = []
            
            for persona in personas:
                token_address = persona.get("token")
                token_symbol = persona.get("symbol", "unknown")
                lp_address = persona.get("lp")
                block_number = persona.get("blockNumber", w3.eth.block_number)

                if not token_address:
                    print(f"⚠️ Skipping {token_symbol} due to missing token address")
                    continue

                if lp_address and lp_address.lower() not in tracked_tokens:
                    new_tokens.append((lp_address, token_symbol, token_address, block_number))
                    tracked_tokens.add(lp_address.lower())

            # Process tokens based on configuration
            if PROCESS_TOKENS_SEQUENTIALLY:
                # Process one token at a time
                for lp_address, token_symbol, token_address, block_number in new_tokens:
                    if lp_address.lower() not in active_tokens:
                        active_tokens.add(lp_address.lower())
                        print(f"🚀 STARTING SEQUENTIAL TRACKING: {token_symbol} WITH LIQUIDITY POOL: {lp_address}")
                        
                        # Track this token (this will block until completion or error)
                        try:
                            track_lp_swaps(lp_address, token_symbol, token_address, block_number)
                        except Exception as e:
                            print(f"❌ Error tracking {token_symbol}: {e}")
                            active_tokens.discard(lp_address.lower())
                            tracked_tokens.discard(lp_address.lower())
            else:
                # Process all tokens in parallel (original behavior)
                for lp_address, token_symbol, token_address, block_number in new_tokens:
                    if lp_address.lower() not in active_tokens:
                        active_tokens.add(lp_address.lower())
                        threading.Thread(
                            target=track_lp_swaps,
                            args=(lp_address, token_symbol, token_address, block_number),
                            daemon=True
                        ).start()
                        print(f"🚀 STARTED PARALLEL TRACKING: {token_symbol} WITH LIQUIDITY POOL: {lp_address}")

        except Exception as e:
            print(f"❌ ERROR IN newTokenMonitor: {e}")

        time.sleep(10)

# =============================================================================
# PRICE ANALYSIS FUNCTIONS
# =============================================================================

def analyze_swap_transaction(doc, collection_name):
    """
    Analyze a single swap transaction and calculate prices
    
    Args:
        doc: MongoDB document containing swap data
        collection_name: Name of the collection
    
    Returns:
        dict: Analysis results or None if analysis failed
    """
    block = doc.get("blockNumber")
    tx_hash = doc.get("txHash")
    
    if not block or not tx_hash:
        return None

    # Get Virtual/USDC price at this block
    virtual_usdc_price = get_virtual_usdc_price(block)
    if virtual_usdc_price == 0.0:
        return None

    # Extract VIRTUAL amounts from transaction
    virtual_in = float(doc.get("Virtual_IN", 0) or 0)
    virtual_out = float(doc.get("Virtual_OUT", 0) or 0)

    # Detect Genesis token (non-VIRTUAL token)
    keys = [k for k in doc.keys() if ("_IN" in k or "_OUT" in k) and "Virtual" not in k]
    genesis_token_name = None
    genesis_in = 0.0
    genesis_out = 0.0

    for key in keys:
        token_base = key.replace("_IN", "").replace("_OUT", "")
        
        genesis_token_name = token_base.replace("_AfterTax", "")
        if key.endswith("_IN"):
            genesis_in = float(doc[key] or 0)
        if key.endswith("_OUT"):
            genesis_out = float(doc[key] or 0)

    if not genesis_token_name:
        return None  # Skip if no genesis token detected

    # Calculate Genesis/VIRTUAL price
    if virtual_in > 0 and genesis_out > 0:
        # Sell: Genesis sold for VIRTUAL
        genesis_virtual_price = virtual_in / genesis_out
    elif virtual_out > 0 and genesis_in > 0:
        # Buy: Genesis bought using VIRTUAL
        genesis_virtual_price = virtual_out / genesis_in
    else:
        genesis_virtual_price = 0.0

    # Convert to USDC using price calculation
    genesis_usdc_price = genesis_virtual_price * virtual_usdc_price

    return {
        "collection_name": collection_name,
        "block": block,
        "tx_hash": tx_hash,
        "genesis_token_name": genesis_token_name,
        "virtual_usdc_price": round(virtual_usdc_price, 8),
        "genesis_virtual_price": round(genesis_virtual_price, 8),
        "genesis_usdc_price": round(genesis_usdc_price, 8)
    }

def analyze_existing_swaps():
    """
    Analyze existing swap transactions in MongoDB
    """
    print("🔍 Analyzing existing swap transactions...")
    
    # Find all swap collections in database
    collections = [name for name in db.list_collection_names() if name.endswith("_swap")]
    if not collections:
        print("⚠️ No '_swap' collections found.")
        return

    print(f"📂 Found {len(collections)} swap collections")
    print("🔄 Starting analysis...\n")

    # Statistics tracking
    total_transactions = 0
    successful_transactions = 0
    skipped_blocks = 0
    processed_collections = 0

    # Create output CSV file
    output_file = "genesis_prices_analysis.csv"
    with open(output_file, "w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        
        # Write CSV header
        writer.writerow([
            "Collection Name",
            "Block Number", 
            "Transaction Hash",
            "Genesis Token Name",
            "Virtual Price (USDC)",
            "Genesis Price (Virtual)", 
            "Genesis Price (USDC)"
        ])

        # Process each collection
        for coll_name in collections:
            collection = db[coll_name]
            collection_count = collection.count_documents({})
            print(f"📂 Processing {coll_name} ({collection_count} transactions)")

            # Process each swap transaction
            for doc in collection.find():
                total_transactions += 1
                
                # Analyze the transaction
                analysis_result = analyze_swap_transaction(doc, coll_name)
                if not analysis_result:
                    skipped_blocks += 1
                    continue

                # Update the MongoDB document with calculated prices
                if UPDATE_MONGODB:
                    update_data = {
                        "virtual_usdc_price": analysis_result["virtual_usdc_price"],
                        "genesis_virtual_price": analysis_result["genesis_virtual_price"],
                        "genesis_usdc_price": analysis_result["genesis_usdc_price"],
                        "genesis_token_name": analysis_result["genesis_token_name"],
                    }
                    
                    collection.update_one(
                        {"_id": doc["_id"]},
                        {"$set": update_data}
                    )

                # Save to CSV
                writer.writerow([
                    analysis_result["collection_name"],
                    analysis_result["block"],
                    analysis_result["tx_hash"],
                    analysis_result["genesis_token_name"],
                    analysis_result["virtual_usdc_price"],
                    analysis_result["genesis_virtual_price"],
                    analysis_result["genesis_usdc_price"]
                ])

                # Display detailed transaction information
                if SHOW_TRANSACTION_DETAILS:
                    print(
                        f"  📊 Block {analysis_result['block']} | TX: {analysis_result['tx_hash'][:10]}... | "
                        f"Token: {analysis_result['genesis_token_name']} | "
                        f"Virtual/USDC: ${analysis_result['virtual_usdc_price']:.8f} | "
                        f"{analysis_result['genesis_token_name']}/USDC: ${analysis_result['genesis_usdc_price']:.8f}"
                    )

                successful_transactions += 1

            processed_collections += 1
            print(f"✅ Completed {coll_name}")

    # Final summary
    print("\n" + "=" * 50)
    print("📊 ANALYSIS SUMMARY")
    print("=" * 50)
    print(f"📂 Collections processed: {processed_collections}/{len(collections)}")
    print(f"🔄 Total transactions: {total_transactions}")
    print(f"✅ Successful transactions: {successful_transactions}")
    print(f"⚠️ Skipped blocks: {skipped_blocks}")
    print(f"📈 Success rate: {(successful_transactions/total_transactions*100):.1f}%" if total_transactions > 0 else "📈 Success rate: 0%")
    if UPDATE_MONGODB:
        print(f"💾 MongoDB documents updated: {successful_transactions}")
    print(f"📄 Results saved to: {output_file}")
    print("=" * 50)

def show_latest_genesis_swaps(n=10):
    """
    Show the latest n Genesis token swap events from all _swap collections.
    """
    print(f"\n🔎 Fetching the latest {n} Genesis token swap events...")
    collections = [name for name in db.list_collection_names() if name.endswith("_swap")]
    if not collections:
        print("⚠️ No '_swap' collections found.")
        return
    all_swaps = []
    for coll_name in collections:
        collection = db[coll_name]
        # Get latest n swaps from this collection
        docs = list(collection.find().sort("blockNumber", -1).limit(n))
        for doc in docs:
            all_swaps.append((doc.get("blockNumber", 0), coll_name, doc))
    # Sort all swaps by blockNumber descending
    all_swaps.sort(reverse=True, key=lambda x: x[0])
    # Take the top n
    latest_swaps = all_swaps[:n]
    if not latest_swaps:
        print("⚠️ No swap events found.")
        return
    print(f"\n{'Block':<10} {'Collection':<20} {'Token':<12} {'Type':<6} {'Amount':<18} {'TxHash':<66}")
    print("-"*120)
    for block, coll_name, doc in latest_swaps:
        # Try to extract token and amount info
        token = doc.get("genesis_token_name") or coll_name.replace("_swap", "").upper()
        swap_type = doc.get("swapType", "?")
        amount = doc.get(f"{token}_IN") or doc.get(f"{token}_OUT") or "-"
        tx_hash = doc.get("txHash", "")[0:64]
        print(f"{block:<10} {coll_name:<20} {token:<12} {swap_type:<6} {amount:<18} {tx_hash}")
    print("\nDone.\n")

# =============================================================================
# MAIN FUNCTIONS
# =============================================================================

def start_tracking():
    """Start the real-time tracking system"""
    if not ENABLE_REAL_TIME_TRACKING:
        print("⚠️ Real-time tracking is disabled in configuration")
        return
        
    print("🚀 Starting Virtual Genesis Token Tracker...")
    print("=" * 50)
    print(f"📡 MongoDB: {MONGO_URI.split('@')[1] if MONGO_URI and '@' in MONGO_URI else 'Connected'}")
    print(f"🔗 Base RPC: {ALCHEMY_URL.split('/v2/')[0]}/v2/***")
    print(f"🏊 VIRTUAL/USDC Pool: {VIRTUAL_USDC_POOL}")
    print(f"⚙️ Max block retries: {MAX_BLOCK_RETRIES}")
    print(f"🔊 Verbose mode: {'ON' if VERBOSE_OUTPUT else 'OFF'}")
    print(f"📊 Transaction details: {'ON' if SHOW_TRANSACTION_DETAILS else 'OFF'}")
    print(f"💾 MongoDB updates: {'ON' if UPDATE_MONGODB else 'OFF'}")
    print(f"🔍 Price analysis: {'ON' if ENABLE_PRICE_ANALYSIS else 'OFF'}")
    print("=" * 50)
    
    # Start token monitor in background
    threading.Thread(target=newTokenMonitor, daemon=True).start()
    print("🔄 Token monitor started - waiting for new tokens to track...")
    
    # Keep main thread alive
    while True:
        time.sleep(10)

def main():
    """Main function with menu options"""
    print("🎯 Virtual Genesis Unified Tracker & Analyzer")
    print("=" * 60)
    print("This script tracks Genesis token swaps in real-time,")
    print("analyzes them immediately, and stores results in MongoDB.")
    print("=" * 60)
    print("1. Start real-time tracking & analysis")
    print("2. Show latest 10 Genesis swaps")
    print("3. Exit")
    print("=" * 60)
    
    while True:
        try:
            choice = input("Select an option (1-3): ").strip()
            
            if choice == "1":
                start_tracking()
                break
            elif choice == "2":
                show_latest_genesis_swaps(10)
            elif choice == "3":
                print("👋 Goodbye!")
                break
            else:
                print("❌ Invalid choice. Please select 1-3.")
                
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")

# =============================================================================
# SCRIPT EXECUTION
# =============================================================================

if __name__ == "__main__":
    main() 