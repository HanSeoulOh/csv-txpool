import pandas as pd
from web3 import AsyncWeb3
from web3.providers import WebsocketProviderV2
import csv
from dotenv import dotenv_values
import asyncio
import json
import requests
from websockets import connect

import time

writelock = asyncio.Lock()

config = dotenv_values(".env")

async def initialize_web3():
    # Connect to the Ethereum node
    w3 = AsyncWeb3(WebsocketProviderV2(config['WEBSOCKET_PROVIDER']))

    # Check if the connection is successful
    sem = asyncio.Semaphore(1)
    async with sem:
        while not await w3.is_connected():
            await asyncio.sleep(1)
            print("Connecting to Reth node...")
    print("Connected to Reth node")
    return w3

# w3 = asyncio.run(initialize_web3())
# Define the CSV file path
csv_file_path = config['CSV_FILE_PATH']

# Initialize the CSV file with headers
with open(csv_file_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['tx_hash', 'from_address', 'to_address', 'value', 'gas_price', 'gas_limit', 'nonce', 'input_data', 'timestamp'])

# Function to handle and store transactions
async def handle_transaction(tx_hash, w3):
    try:
        tx = await w3.eth.get_transaction(tx_hash)
        if tx:
            tx_data = {
                'tx_hash': tx['hash'].hex(),
                'from_address': tx['from'],
                'to_address': tx['to'] if tx['to'] else 'Contract Creation',
                'value': tx['value'],
                'gas_price': tx['gasPrice'],
                'gas_limit': tx['gas'],
                'nonce': tx['nonce'],
                'input_data': tx['input'],
                'timestamp': pd.Timestamp.now()
            }
            async with writelock:
                with open(csv_file_path, mode='a+', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(tx_data.values())
    except Exception as e:
        print(f"Error processing transaction {tx_hash}: {e}")

# # Subscribe to pending transactions and process them
# subscription = w3.eth.subscribe('pendingTransactions', lambda tx_hash: handle_transaction(tx_hash))
async def get_event():
    async with AsyncWeb3.persistent_websocket(
        WebsocketProviderV2(config['WEBSOCKET_PROVIDER'])
    ) as w3:
        newpt_subscription_id = await w3.eth.subscribe("newPendingTransactions")
        newhead_subscription_id = await w3.eth.subscribe("newHeads")
        
        latest_block = await w3.eth.get_block("latest")
        print(f"Latest block: {latest_block['number']}")

        async for message in w3.ws.process_subscriptions():
            if message['subscription'] == newpt_subscription_id:
                txHash = message['result']
                await handle_transaction(txHash, w3)
            elif message['subscription'] == newhead_subscription_id:
                print(f"New block: {message['result']['number']}")
            else:
                print(f"Unknown message: {message}")

        # while True:
        #     try:
        #         message = await asyncio.wait_for(w3.recv(), timeout=5)
        #         response = json.loads(message)
        #         txHash = response['params']['result']
        #         handle_transaction(txHash)
        #         pass
        #     except Exception as e:
        #         print(f"Error: {e}")
        #         pass

# Keep the script running
timerInterval = 5


if __name__ == "__main__":
    asyncio.run(get_event())
    while True:
        print(f"Waiting {timerInterval} seconds...")
        time.sleep(timerInterval)
