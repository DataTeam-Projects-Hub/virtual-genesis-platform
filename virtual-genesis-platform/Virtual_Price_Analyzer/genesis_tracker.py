"""
Genesis Tokens Complete Tracker
===============================

This single script:
1. Sets up the new genesis_tokens_swap_info database
2. Reads personas from virtualgenesis database
3. Starts tracking from each token's genesis block to current block
4. Stores all swaps in the new database structure

"""

import time
import threading
from pymongo import MongoClient
from web3 import Web3
import math
from datetime import datetime
import os
from dotenv import load_dotenv
from eth_abi.abi import decode

# Load environment variables
load_dotenv()

# MongoDB Connection
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("MONGO_URI environment variable is required")

# Database names
NEW_DATABASE = "genesis_tokens_swap_info"
PERSONAS_DATABASE = "virtualgenesis"

# Blockchain RPC
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY")
ALCHEMY_HTTP_URL = os.getenv("ALCHEMY_HTTP_URL")

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
MAX_BLOCK_RETRIES = 5
VERBOSE_OUTPUT = False
SHOW_TRANSACTION_DETAILS = True
UPDATE_MONGODB = True
ENABLE_REAL_TIME_TRACKING = True
ENABLE_PRICE_ANALYSIS = True
PROCESS_TOKENS_SEQUENTIALLY = False
RATE_LIMIT_DELAY = 1.0  # Increased delay to prevent rate limiting
ERROR_RETRY_DELAY = 10  # Increased delay on errors
BLOCK_FETCH_DELAY = 3  # Increased delay between block fetches
MAX_CONCURRENT_TOKENS = 3  # Limit concurrent token tracking

# Swap tracking settings
SWAP_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
CHUNK_SIZE = 500  # Reduced chunk size to prevent API issues

# Initialize connections
w3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))
client = MongoClient(MONGO_URI)
db = client[NEW_DATABASE]
personas_db = client[PERSONAS_DATABASE]

# Tracked tokens set
tracked_tokens = set()

# Semaphore to limit concurrent token tracking
token_semaphore = threading.Semaphore(MAX_CONCURRENT_TOKENS)

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

def setup_genesis_database():
    """Set up the new Genesis database structure"""
    print("🚀 Setting up Genesis Tokens Database")
    print("=" * 50)
    print(f"📁 Database: {NEW_DATABASE}")
    print("🆕 Starting fresh - no old data will be copied")
    print("=" * 50)
    
    try:
        # Create database info collection
        info_collection = db["database_info"]
        
        info_doc = {
            "database_name": NEW_DATABASE,
            "created_at": datetime.utcnow(),
            "description": "Genesis tokens swap data - tracking from genesis blocks",
            "structure": "Each Genesis token has its own collection named {token}_swap",
            "collections": [],
            "status": "ready_for_tracking"
        }
        
        # Remove existing info if any
        info_collection.delete_many({})
        
        # Insert new info
        info_collection.insert_one(info_doc)
        print(f"📋 Database info created: {NEW_DATABASE}")
        
        # Create swap_progress collection for tracking progress
        progress_collection = db["swap_progress"]
        progress_collection.delete_many({})  # Clear any existing progress
        print("📊 Progress tracking collection ready")
        
        print("\n✅ Genesis database setup completed!")
        print(f"📁 Database: {NEW_DATABASE}")
        print("🔄 Ready to start tracking from genesis blocks")
        print("📂 Collections will be created automatically as tokens are tracked")
        
        return True
        
    except Exception as e:
        print(f"❌ Database setup failed: {e}")
        return False

def get_collection_name(token_symbol):
    """Get the collection name for a Genesis token"""
    return f"{token_symbol.lower()}_swap"

def web3_call_with_retries(func, max_retries=3, delay=1):
    """Execute a Web3 call with retry logic and delays"""
    for attempt in range(max_retries + 1):
        try:
            result = func()
            if attempt > 0:
                time.sleep(delay)
            return result
        except Exception as e:
            error_str = str(e).lower()
            
            # Handle rate limiting specifically
            if "429" in error_str or "too many requests" in error_str:
                wait_time = delay * (2 ** attempt)  # Exponential backoff
                print(f"⚠️ Rate limited (attempt {attempt + 1}/{max_retries + 1}). Waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            elif "timeout" in error_str or "connection" in error_str:
                wait_time = delay * (attempt + 1)
                print(f"⚠️ Network error (attempt {attempt + 1}/{max_retries + 1}). Waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            
            if attempt == max_retries:
                raise e
            if VERBOSE_OUTPUT:
                print(f"⚠️ Web3 call failed (attempt {attempt + 1}/{max_retries + 1}): {e}")
            time.sleep(delay * (attempt + 1))

def sqrtPriceX96ToPrice(sqrtPriceX96, token0IsVirtual):
    """Convert sqrtPriceX96 to price using the exact logic"""
    sqrtPriceNum = float(sqrtPriceX96)
    Q96 = 2 ** 96
    sqrtPriceNormalized = sqrtPriceNum / Q96
    rawPrice = sqrtPriceNormalized ** 2
    
    if token0IsVirtual:
        usdcPerVirtual = rawPrice * (10 ** 12)
        finalPrice = 1 / usdcPerVirtual
    else:
        finalPrice = rawPrice / (10 ** 12)
    
    return finalPrice

def get_virtual_usdc_price(block_number, max_retries=None):
    """Get Virtual/USDC price from Uniswap V3 pool at specific block"""
    if max_retries is None:
        max_retries = MAX_BLOCK_RETRIES
    
    for offset in range(max_retries + 1):
        try_block = block_number + offset
        
        try:
            pool = w3.eth.contract(
                address=Web3.to_checksum_address(VIRTUAL_USDC_POOL), 
                abi=POOL_ABI
            )
            
            slot0 = pool.functions.slot0().call(block_identifier=try_block)
            token0 = pool.functions.token0().call()
            token1 = pool.functions.token1().call()
            
            token0IsVirtual = token0.lower() == VIRTUAL_TOKEN_ADDRESS.lower()
            price = 1 / sqrtPriceX96ToPrice(slot0[0], token0IsVirtual)
            
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

def swapDataDecoder(data):
    """Decode swap event data"""
    expected_length = 128
    if len(data) != expected_length:
        raise ValueError(f"INVALID LOG DATA LENGTH :: EXPECTED : {expected_length} BYTES || GOT {len(data)}")

    types = ["uint256", "uint256", "uint256", "uint256"]
    decoded = decode(types, data)
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

def store_swap_data(swap_data, token_address, token_symbol, persona_info=None):
    """Store swap data in the Genesis token's collection"""
    try:
        # Get persona info if not provided
        if persona_info is None:
            persona_info = personas_db["personas"].find_one({"token": token_address})
        
        # Get the collection name for this token
        collection_name = get_collection_name(token_symbol)
        collection = db[collection_name]
        
        # Create document with all swap data and persona info
        swap_doc = {
            # Basic swap information
            "blockNumber": swap_data.get("blockNumber"),
            "txHash": swap_data.get("txHash"),
            "txLink": swap_data.get("txLink"),
            "lp": swap_data.get("lp"),
            "maker": swap_data.get("maker"),
            "swapType": swap_data.get("swapType"),
            "timestamp": swap_data.get("timestamp"),
            "timestampReadable": swap_data.get("timestampReadable"),
            "label": swap_data.get("label"),
            "receiver": swap_data.get("receiver"),
            
            # Gas and fee information
            "gasPrice": swap_data.get("gasPrice"),
            "gasUsed": swap_data.get("gasUsed"),
            "transactionFee": swap_data.get("transactionFee"),
            "gasPriceGwei": swap_data.get("gasPriceGwei"),
            
            # Genesis token information
            "genesis_token_address": token_address,
            "genesis_token_symbol": token_symbol,
            
            # Swap amounts
            "Virtual_IN": swap_data.get("Virtual_IN"),
            "Virtual_OUT": swap_data.get("Virtual_OUT"),
            f"{token_symbol.upper()}_IN": swap_data.get(f"{token_symbol.upper()}_IN"),
            f"{token_symbol.upper()}_OUT": swap_data.get(f"{token_symbol.upper()}_OUT"),
            
            # Tax information
            "Tax_1pct": swap_data.get("Tax_1pct"),
            f"{token_symbol.upper()}_IN_BeforeTax": swap_data.get(f"{token_symbol.upper()}_IN_BeforeTax"),
            f"{token_symbol.upper()}_IN_AfterTax": swap_data.get(f"{token_symbol.upper()}_IN_AfterTax"),
            f"{token_symbol.upper()}_OUT_BeforeTax": swap_data.get(f"{token_symbol.upper()}_OUT_BeforeTax"),
            f"{token_symbol.upper()}_OUT_AfterTax": swap_data.get(f"{token_symbol.upper()}_OUT_AfterTax"),
            
            # Price analysis results (if available)
            "virtual_usdc_price": swap_data.get("virtual_usdc_price"),
            "genesis_virtual_price": swap_data.get("genesis_virtual_price"),
            "genesis_usdc_price": swap_data.get("genesis_usdc_price"),
            "genesis_token_name": swap_data.get("genesis_token_name"),
        }
        
        # Add persona information if available
        if persona_info:
            swap_doc.update({
                "persona_virtualId": persona_info.get("virtualId"),
                "persona_dao": persona_info.get("dao"),
                "persona_tba": persona_info.get("tba"),
                "persona_veToken": persona_info.get("veToken"),
                "persona_lp": persona_info.get("lp"),
                "persona_name": persona_info.get("name"),
                "persona_symbol": persona_info.get("symbol"),
                "persona_txHash": persona_info.get("txHash"),
                "persona_blockNumber": persona_info.get("blockNumber"),
            })
        
        # Store in the token's collection
        result = collection.insert_one(swap_doc)
        print(f"💾 Stored in {collection_name}: {token_symbol} {swap_data.get('swapType')} swap")
        return result.inserted_id
        
    except Exception as e:
        print(f"❌ Error storing swap data: {e}")
        return None

def analyze_swap_transaction(doc, collection_name):
    """Analyze a single swap transaction and calculate prices"""
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
        return None

    # Calculate Genesis/VIRTUAL price
    if virtual_in > 0 and genesis_out > 0:
        genesis_virtual_price = virtual_in / genesis_out
    elif virtual_out > 0 and genesis_in > 0:
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

def track_lp_swaps_from_genesis(lp_address, token_symbol, token_address, genesis_block):
    """Track swaps for a specific liquidity pool from genesis block to current"""
    lp_address = Web3.to_checksum_address(lp_address)
    token_address = Web3.to_checksum_address(token_address)
    collection = db[get_collection_name(token_symbol)]
    progress_coll = db["swap_progress"]
    
    # Check for existing progress
    progress_entry = progress_coll.find_one({"lp": lp_address})
    if progress_entry:
        last_processed = progress_entry.get("lastProcessed", genesis_block)
        if last_processed >= genesis_block:
            current_block = last_processed + 1
            print(f"🔄 Resuming {token_symbol} from checkpoint at block {current_block}")
        else:
            current_block = genesis_block
    else:
        current_block = genesis_block
        print(f"🚀 Starting {token_symbol} from genesis block {genesis_block}")

    print(f"📊 Tracking swaps for {token_symbol} (LP: {lp_address}) from block {genesis_block}...")

    while True:
        try:
            latest_block = web3_call_with_retries(lambda: w3.eth.block_number)
            if latest_block is None:
                print("⚠️ Could not fetch latest block number, retrying...")
                time.sleep(BLOCK_FETCH_DELAY)
                continue
                
            to_block = min(current_block + CHUNK_SIZE - 1, latest_block)

            if current_block > to_block:
                time.sleep(BLOCK_FETCH_DELAY)
                continue
        except Exception as e:
            print(f"❌ Error fetching latest block: {e}")
            time.sleep(BLOCK_FETCH_DELAY)
            continue

        try:
            if VERBOSE_OUTPUT:
                print(f"🔍 Processing blocks {current_block} to {to_block} for {token_symbol}")
                
            try:
                logs = w3.eth.get_logs({
                    "fromBlock": current_block,
                    "toBlock": to_block,
                    "address": lp_address,
                    "topics": [SWAP_TOPIC]  # Use the raw topic string
                })
            except Exception as log_error:
                error_str = str(log_error).lower()
                if "400" in error_str or "bad request" in error_str or "invalid params" in error_str:
                    print(f"⚠️ Invalid request for blocks {current_block}-{to_block}. Skipping this range...")
                    # Skip this block range and continue
                    current_block = to_block + 1
                    continue
                else:
                    raise log_error  # Re-raise other errors

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
                        "maker": tx.get("from", "unknown") if tx else "unknown",
                        "swapType": swap_type,
                        "timestamp": int(time.time()),
                        "timestampReadable": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(block_timestamp)),
                        "label": "swap",
                        "receiver": tx.get("from", "unknown") if tx else "unknown",
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

                    # Store swap data
                    store_swap_data(swap_data, token_address, token_symbol)
                    
                    if VERBOSE_OUTPUT:
                        print(f"💾 {swap_type.upper()} swap stored: {tx_hash}")

                    # Analyze price if enabled
                    if ENABLE_PRICE_ANALYSIS:
                        analysis_result = analyze_swap_transaction(swap_data, f"{token_symbol.lower()}_swap")
                        if analysis_result:
                            if UPDATE_MONGODB:
                                update_data = {
                                    "virtual_usdc_price": analysis_result["virtual_usdc_price"],
                                    "genesis_virtual_price": analysis_result["genesis_virtual_price"],
                                    "genesis_usdc_price": analysis_result["genesis_usdc_price"],
                                    "genesis_token_name": analysis_result["genesis_token_name"],
                                }
                                
                                collection_name = get_collection_name(token_symbol)
                                collection = db[collection_name]
                                
                                collection.update_one(
                                    {
                                        "txHash": swap_data.get("txHash"),
                                        "blockNumber": swap_data.get("blockNumber")
                                    },
                                    {"$set": update_data}
                                )
                                
                                swap_data.update(update_data)

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

                        # Store auto-swap data
                        store_swap_data(swap_data, token_address, token_symbol)
                        
                        if VERBOSE_OUTPUT:
                            print(f"🤖 AUTO-{swap_type.upper()} swap stored: {tx_hash}")

                        # Analyze price if enabled
                        if ENABLE_PRICE_ANALYSIS:
                            analysis_result = analyze_swap_transaction(swap_data, f"{token_symbol.lower()}_swap")
                            if analysis_result:
                                if UPDATE_MONGODB:
                                    update_data = {
                                        "virtual_usdc_price": analysis_result["virtual_usdc_price"],
                                        "genesis_virtual_price": analysis_result["genesis_virtual_price"],
                                        "genesis_usdc_price": analysis_result["genesis_usdc_price"],
                                        "genesis_token_name": analysis_result["genesis_token_name"],
                                    }
                                    
                                    collection_name = get_collection_name(token_symbol)
                                    collection = db[collection_name]
                                    
                                    collection.update_one(
                                        {
                                            "txHash": swap_data.get("txHash"),
                                            "blockNumber": swap_data.get("blockNumber")
                                        },
                                        {"$set": update_data}
                                    )
                                    
                                    swap_data.update(update_data)

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
        time.sleep(RATE_LIMIT_DELAY)  # Add delay between chunks to prevent rate limiting

def newTokenMonitor():
    """Monitor for new tokens to track from their genesis blocks"""
    active_tokens = set()
    
    while True:
        try:
            personas = personas_db["personas"].find()
            new_tokens = []
            
            for persona in personas:
                token_address = persona.get("token")
                token_symbol = persona.get("symbol", "unknown")
                lp_address = persona.get("lp")
                genesis_block = persona.get("blockNumber", w3.eth.block_number)

                if not token_address:
                    print(f"⚠️ Skipping {token_symbol} due to missing token address")
                    continue

                if lp_address and lp_address.lower() not in tracked_tokens:
                    new_tokens.append((lp_address, token_symbol, token_address, genesis_block))
                    tracked_tokens.add(lp_address.lower())

            # Process tokens with limited concurrency
            for lp_address, token_symbol, token_address, genesis_block in new_tokens:
                if lp_address.lower() not in active_tokens:
                    active_tokens.add(lp_address.lower())
                    print(f"🚀 STARTING LIMITED PARALLEL TRACKING: {token_symbol} FROM GENESIS BLOCK {genesis_block}")
                    
                    # Start tracking in a separate thread with semaphore
                    def track_with_semaphore(lp, symbol, address, block):
                        with token_semaphore:
                            try:
                                track_lp_swaps_from_genesis(lp, symbol, address, block)
                            except Exception as e:
                                print(f"❌ Error tracking {symbol}: {e}")
                            finally:
                                active_tokens.discard(lp.lower())
                                tracked_tokens.discard(lp.lower())
                    
                    threading.Thread(
                        target=track_with_semaphore,
                        args=(lp_address, token_symbol, token_address, genesis_block),
                        daemon=True
                    ).start()

        except Exception as e:
            print(f"❌ ERROR IN newTokenMonitor: {e}")

        time.sleep(15)  # Increased delay between token checks

def show_database_status():
    """Show current database status"""
    print("\n🔍 Current Database Status:")
    print("=" * 50)
    
    try:
        # Check database info
        info_collection = db["database_info"]
        info_doc = info_collection.find_one()
        
        if info_doc:
            print(f"📋 Database: {info_doc.get('database_name')}")
            print(f"📅 Created: {info_doc.get('created_at')}")
            print(f"📊 Status: {info_doc.get('status')}")
        else:
            print("⚠️ No database info found")
        
        # Get all collections
        all_collections = db.list_collection_names()
        swap_collections = [col for col in all_collections if col.endswith("_swap")]
        other_collections = [col for col in all_collections if not col.endswith("_swap")]
        
        print(f"\n📂 Collections ({len(all_collections)} total):")
        
        if other_collections:
            print("   System collections:")
            for col in other_collections:
                doc_count = db[col].count_documents({})
                print(f"     - {col}: {doc_count} documents")
        
        if swap_collections:
            print("   Token collections:")
            total_swaps = 0
            for col in swap_collections:
                doc_count = db[col].count_documents({})
                total_swaps += doc_count
                
                latest_doc = db[col].find_one({}, sort=[("blockNumber", -1)])
                latest_block = latest_doc.get("blockNumber") if latest_doc else "N/A"
                latest_time = latest_doc.get("timestampReadable") if latest_doc else "N/A"
                
                print(f"     - {col}: {doc_count} swaps (latest: block {latest_block}, {latest_time})")
            
            print(f"\n📊 Total swaps across all tokens: {total_swaps}")
        else:
            print("   No token collections found yet")
        
        # Check progress tracking
        progress_collection = db["swap_progress"]
        progress_entries = list(progress_collection.find())
        
        if progress_entries:
            print(f"\n📈 Progress Tracking ({len(progress_entries)} entries):")
            for entry in progress_entries:
                lp = entry.get("lp", "Unknown")
                last_block = entry.get("lastProcessed", "N/A")
                print(f"   - {lp}: last processed block {last_block}")
        else:
            print("\n📈 No progress tracking data yet")
        
    except Exception as e:
        print(f"❌ Error checking database status: {e}")

def main():
    """Main function with menu options"""
    print("🎯 Genesis Tokens Complete Tracker")
    print("=" * 60)
    print("This script sets up the database and tracks all Genesis tokens")
    print("from their genesis blocks to the current block.")
    print("=" * 60)
    print("1. Set up database and start tracking")
    print("2. Show database status")
    print("3. Exit")
    print("=" * 60)
    
    while True:
        try:
            choice = input("Select an option (1-3): ").strip()
            
            if choice == "1":
                print("\n🚀 Setting up database and starting tracking...")
                
                # Set up database
                if setup_genesis_database():
                    print("\n✅ Database setup completed!")
                    print("🔄 Starting token monitor...")
                    
                    # Start token monitor in background
                    threading.Thread(target=newTokenMonitor, daemon=True).start()
                    print("🔄 Token monitor started - waiting for tokens to track...")
                    
                    # Keep main thread alive
                    while True:
                        time.sleep(10)
                else:
                    print("❌ Database setup failed")
                break
                
            elif choice == "2":
                show_database_status()
                
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

if __name__ == "__main__":
    main() 