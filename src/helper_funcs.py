import rlp
from eth_typing import HexStr
from eth_utils import to_bytes
from ethereum.transactions import Transaction
import requests
from requests.adapters import HTTPAdapter, Retry
from threading import Thread
from queue import Queue

# for retrying requests
retries = Retry(total=20, backoff_factor=0.00001, status_forcelist=[500, 503, 504], allowed_methods=frozenset(['GET', 'POST']))

def hex_to_bytes(data: str) -> bytes:
    """
    Converts a hexadecimal string to bytes

    Parameters:
    -----------
    data : str
        The hexadecimal string

    Returns:
    -----------
    bytes
        The hexadecimal string as bytes
    """
    return to_bytes(hexstr=HexStr(data))

def tx_hash_to_dict(tx: str) -> dict:
    """
    Converts a transaction object hash / hex to a dictionary

    Parameters:
    -----------
    tx : str
        The transaction object as hexadecimal

    Returns:
    -----------
    dict
        The hexadecimal transaction object decoded as a dict
    """
    try:
        # try to decode
        tx = rlp.decode(hex_to_bytes(tx), Transaction)
        return tx.to_dict()
    except Exception as e:
        # if there was an error, return an empty dictionary
        return {}

def match_transaction(recipient: str, transaction) -> float:
    """
    Tries to match the hash of the transaction sender with the recipient

    Parameters:
    -----------
    recipient : str
        The hash of the recipient
    transaction : obj
        The transaction object

    Returns:
    -----------
    float
        The value of the transaction in ETH
    """
    if transaction == {}:
        # if transaction is empty, return -1
        pass
    elif transaction['sender'] == recipient:
        # otherwise, return value in ETH
        return transaction['value']/1000000000000000000
    
    return -1

def calculate_rewards(block_number: int, eth1_rpc: str):
    """
    Calculates the rewards generated from transactions for a given block number

    Parameters:
    -----------
    block_number : int
        The block number to calculate rewards for
    eth1_rpc : str
        The url / endpoint of an ETH1 rpc

    Returns:
    -----------
    float
        The reward value in ETH
    """
    global retries

    # first convert blocknumber to hex
    bn_hex = hex(block_number)

    try:
        # get the block using eth1 node
        s = requests.Session()
        s.mount('http://', HTTPAdapter(max_retries=retries))
        block = s.post(url=eth1_rpc, headers={'Content-Type': 'application/json'}, data='{"method":"eth_getBlockByNumber","params":["'+bn_hex+'",false],"id":1,"jsonrpc":"2.0"}', timeout=10)
        block.raise_for_status()
    except requests.exceptions.RequestException:
        # if request failed, we can't do anything - exit
        print("ETH1 request failed - calculate_rewards()")
        return -1
    
    # convert block data to json and get only part we need
    block = block.json()
    block = block['result']

    # get values used for calculation
    base_fee = int(block['baseFeePerGas'], 16)
    gas_used = int(block['gasUsed'], 16)
    tx_fees = 0
    
    # for each transaction, calculate the transaction fees and sum them up
    for tx in block['transactions']:
        tx_fees += calculate_tx_fee(tx, eth1_rpc)

    # calculate the amount of ETH/gas that was burnt
    burned_eth = base_fee * gas_used

    # calculate the block reward by subtracting the burned ETH/gas
    block_reward = tx_fees - burned_eth

    # return the value in ETH
    return block_reward/1000000000000000000

def calculate_tx_fee(tx_hash, eth1_rpc):
    """
    Calculates the transaction fee / reward of a specific transaction

    Parameters:
    -----------
    tx_hash : str
        The hash of the transaction
    eth1_rpc : str
        The url / endpoint of an ETH1 rpc

    Returns:
    -----------
    int
        The reward value in Gwei (not in ETH)
    """
    global retries

    tx_rec = {}
    
    try:
        # get gas used value
        s = requests.Session()
        s.mount('http://', HTTPAdapter(max_retries=retries))
        tx_rec = s.post(url=eth1_rpc, headers={'Content-Type': 'application/json'}, data='{"method":"eth_getTransactionReceipt","params":["'+tx_hash+'"],"id":1,"jsonrpc":"2.0"}', timeout=10)
        tx_rec.raise_for_status()

        # convert result to json and get useful part
        tx_rec = tx_rec.json()
        tx_rec = tx_rec['result']
    except requests.exceptions.RequestException:
        # if request failed, then we cannot proceed
        print("ETH1 request failed - calculate_tx_fee()")
        return 0

    # return tx fee / reward value
    return int(tx_rec['effectiveGasPrice'], 16) * int(tx_rec['gasUsed'], 16)

def calc_tx_fee(tx):
    """
    Given a transaction, it calculates the tx fee / reward

    Parameters:
    -----------
    tx : obj
        A transaction object, we only use 'effectiveGasPrice' and 'gasUsed'

    Returns:
    -----------
    int
        The reward value in Gwei (not in ETH)
    """
    return int(tx['effectiveGasPrice'], 16) * int(tx['gasUsed'], 16)

def calculate_rewards_parallel(block_number: int, eth1_rpc: str) -> float:
    """
    Same as calculate_rewards, but process is done in parallel which is significantly faster
    This, however, is more intensive on the RPC node

    Parameters:
    -----------
    block_number : int
        The block number to calculate rewards for
    eth1_rpc : str
        The url / endpoint of an ETH1 rpc

    Returns:
    -----------
    float
        The reward value in ETH
    """
    global retries

    class Worker(Thread):
        """
        A class used to represent a worker thread

        Attributes:
        -----------
        queue : Queue
            Queue of transaction hashes to calculate the rewards for
        results : list[int]
            List of transaction rewards in Gwei
        
        Methods:
        --------
        run
            Performs a request to the ETH1 RPC node to get details about the transaction, and calculates the reward value
        """
        def __init__(self, tx_queue):
            """
            Parameters:
            -----------
            tx_queue : Queue
                The queue where transaction hashes will be placed
            """
            Thread.__init__(self)
            self.queue = tx_queue
            self.results = []

        def run(self):
            """
            Performs a request to the ETH1 RPC node to get details about the transaction, and calculates the reward value

            Returns:
            --------
            int
                The tx fee / reward value of the transaction in Gwei
            """
            while True:
                # get new transaction to calculate the reward for
                tx_hash = self.queue.get()

                if tx_hash == "":
                    # an empty string is a signal to stop
                    break
                try:
                    # start a new session - so we can attempt requests multiple times
                    s = requests.Session()

                    # specify details to retry
                    s.mount('http://', HTTPAdapter(max_retries=retries))

                    # try the request
                    tx_rec = s.post(url=eth1_rpc, headers={'Content-Type': 'application/json'}, data='{"method":"eth_getTransactionReceipt","params":["'+tx_hash+'"],"id":1,"jsonrpc":"2.0"}', timeout=10)
                    tx_rec.raise_for_status()

                    # conver to json and keep useful part
                    tx_rec = tx_rec.json()
                    tx_rec = tx_rec['result']

                    # calculate tx fee / reward, and append to the list of results
                    self.results.append(calc_tx_fee(tx_rec))

                    # notify queue that the current task is done
                    self.queue.task_done()
                except requests.exceptions.RequestException:
                    # if the request failed, we cannot do anything
                    print("ETH1 request failed - calculate_rewards_parallel() [1]")
                    self.results.append(0)
                    self.queue.task_done()
    
    # the number of parallel workers
    num_workers = 50
    q = Queue()
    
    # first convert blocknumber to hex
    bn_hex = hex(block_number)

    try:
        # get the block using eth1 node
        block = requests.post(url=eth1_rpc, headers={'Content-Type': 'application/json'}, data='{"method":"eth_getBlockByNumber","params":["'+bn_hex+'",false],"id":1,"jsonrpc":"2.0"}')
        block.raise_for_status()
    except requests.exceptions.RequestException:
        # if request failed, we can't do anything - exit
        print("ETH1 request failed - calculate_rewards_parallel() [2]")
        return -1

    # convert block data to json and get only part we need
    block = block.json()
    block = block['result']

    # get values used for calculation
    base_fee = int(block['baseFeePerGas'], 16)
    gas_used = int(block['gasUsed'], 16)

    # add each tx hash in the queue
    for tx_hash in block['transactions']:
        q.put(tx_hash)

    # workers don't stop until they receive an empty string, so add an empty string for each worker
    for _ in range(num_workers * 2):
        q.put("")

    # create workers
    workers = []

    # start the workers
    for _ in range(num_workers):
        worker = Worker(q)
        worker.start()
        workers.append(worker)

    # wait for all the workers to be done
    for worker in workers:
        worker.join()

    # get the results
    results = []
    for worker in workers:
        results.extend(worker.results)

    # sum the results
    tx_fees = sum(results)

    # calculate the burnt fees
    burned_eth = base_fee * gas_used

    # calculate the reward of the block
    block_reward = tx_fees - burned_eth

    # return result in ETH
    return block_reward/1000000000000000000