"""
Virtual Genesis Price Analyzer
==============================

This script analyzes Genesis token prices in USDC by:
1. Fetching real-time Virtual/USDC prices from Uniswap V3 pool
2. Processing swap transactions from MongoDB
3. Calculating Genesis token prices in USDC using Virtual as intermediary


"""

import csv
from pymongo import MongoClient
from web3 import Web3
import math
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# =============================================================================
# CONFIGURATION
# =============================================================================

# MongoDB Connection (from environment variables)
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")

# Blockchain RPC (from environment variables)
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY")
ALCHEMY_URL = f"https://base-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"

# Validate required environment variables
if not MONGO_URI:
    raise ValueError("MONGO_URI environment variable is required. Please set it in your .env file.")
if not DATABASE_NAME:
    raise ValueError("DATABASE_NAME environment variable is required. Please set it in your .env file.")
if not ALCHEMY_API_KEY:
    raise ValueError("ALCHEMY_API_KEY environment variable is required. Please set it in your .env file.")

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

# =============================================================================
# BLOCKCHAIN CONNECTIONS
# =============================================================================

# Initialize Web3 connection to Base network
w3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))

# Initialize MongoDB connection
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]

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
# MAIN ANALYSIS FUNCTION
# =============================================================================

def analyze_genesis_prices():
    """
    Main function to analyze Genesis token prices in USDC
    """
    print("🚀 Virtual Genesis Price Analyzer")
    print("=" * 50)
    print(f"📡 MongoDB: {MONGO_URI.split('@')[1] if MONGO_URI and '@' in MONGO_URI else 'Connected'}")
    print(f"🔗 Base RPC: {ALCHEMY_URL.split('/v2/')[0]}/v2/***")
    print(f"🏊 VIRTUAL/USDC Pool: {VIRTUAL_USDC_POOL}")
    print(f"⚙️ Max block retries: {MAX_BLOCK_RETRIES}")
    print(f"🔊 Verbose mode: {'ON' if VERBOSE_OUTPUT else 'OFF'}")
    print(f"📊 Transaction details: {'ON' if SHOW_TRANSACTION_DETAILS else 'OFF'}")
    print(f"💾 MongoDB updates: {'ON' if UPDATE_MONGODB else 'OFF'}")
    print("=" * 50)

    # Find all swap collections in database
    collections = [name for name in db.list_collection_names() if name.endswith("_swap")]
    if not collections:
        print("⚠️ No '_swap' collections found. Exiting.")
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
        
        # Write CSV header with better column names
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
                block = doc.get("blockNumber")
                tx_hash = doc.get("txHash")
                
                if not block or not tx_hash:
                    continue

                # Get Virtual/USDC price at this block using analyze.js logic
                virtual_usdc_price = get_virtual_usdc_price(block)
                if virtual_usdc_price == 0.0:
                    skipped_blocks += 1
                    continue

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
                    continue  # Skip if no genesis token detected

                # Calculate Genesis/VIRTUAL price
                if virtual_in > 0 and genesis_out > 0:
                    # Sell: Genesis sold for VIRTUAL
                    genesis_virtual_price = virtual_in / genesis_out
                elif virtual_out > 0 and genesis_in > 0:
                    # Buy: Genesis bought using VIRTUAL
                    genesis_virtual_price = virtual_out / genesis_in
                else:
                    genesis_virtual_price = 0.0

                # Convert to USDC using analyze.js price calculation
                genesis_usdc_price = genesis_virtual_price * virtual_usdc_price

                # Update the MongoDB document with calculated prices
                if UPDATE_MONGODB:
                    update_data = {
                        "virtual_usdc_price": round(virtual_usdc_price, 8),
                        "genesis_virtual_price": round(genesis_virtual_price, 8),
                        "genesis_usdc_price": round(genesis_usdc_price, 8),
                        "genesis_token_name": genesis_token_name,
                        # "analysis_timestamp": datetime.now().isoformat()
                    }
                    
                    # Update the document in MongoDB
                    collection.update_one(
                        {"_id": doc["_id"]},
                        {"$set": update_data}
                    )

                # Save to CSV
                writer.writerow([
                    coll_name,
                    block,
                    tx_hash,
                    genesis_token_name,
                    round(virtual_usdc_price, 8),
                    round(genesis_virtual_price, 8),
                    round(genesis_usdc_price, 8)
                ])

                # Display detailed transaction information
                if SHOW_TRANSACTION_DETAILS:
                    print(
                        f"  📊 Block {block} | TX: {tx_hash[:10]}... | "
                        f"Token: {genesis_token_name} | "
                        f"Virtual/USDC: ${virtual_usdc_price:.8f} | "
                        f"{genesis_token_name}/USDC: ${genesis_usdc_price:.8f}"
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

# =============================================================================
# SCRIPT EXECUTION
# =============================================================================

if __name__ == "__main__":
    analyze_genesis_prices() 