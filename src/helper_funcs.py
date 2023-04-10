from eth_typing import HexStr
from eth_utils import to_bytes
import requests
from requests.adapters import HTTPAdapter, Retry
from threading import Thread
from queue import Queue
import json
from os.path import exists
from os import getcwd

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
    --------
    bytes
        The hexadecimal string as bytes
    """
    return to_bytes(hexstr=HexStr(data))

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
    --------
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
    --------
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
    --------
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
    --------
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
    --------
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
                    print("WARN: ETH1 request failed - calculate_rewards_parallel() [1]")
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
        print("WARN: ETH1 request failed - calculate_rewards_parallel() [2]")
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
    
def check_endpoint_validity_eth2(endpoint: str) -> bool:
    """
    Checks the validity of a consensus-layer endpoint by performing a basic Beacon Node API spec request

    Parameters:
    -----------
    endpoint : str
        The url / endpoint of an ETH2 rpc

    Returns:
    --------
    bool
        Whether the endpoint is valid and reachable or not
    """
    global retries

    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=retries))
    
    try:
        headers = s.get(endpoint+"/eth/v1/beacon/headers", timeout=10)
        if headers.ok:
            return True
        else:
            return False
    except:
        return False

def check_endpoint_validity_eth1(endpoint: str) -> bool:
    """
    Checks the validity of an execution-layer endpoint by performing a basic ETH1 request

    Parameters:
    -----------
    endpoint : str
        The url / endpoint of an ETH1 rpc

    Returns:
    --------
    bool
        Whether the endpoint is valid and reachable or not
    """
    global retries

    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=retries))
    
    try:
        block = requests.post(url=endpoint, headers={'Content-Type': 'application/json'}, data='{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":83}')
        if block.ok:
            return True
        else:
            return False
    except:
        return False
    
def read_config_update_options(options: dict) -> dict:
    """
    Reads the config file (options['config']) and updates the options accordingly. Command-line options are always preferred over the ones found in the config file

    Parameters:
    -----------
    options : dict
        A dict of command-line options as passed from the main script

    Returns:
    --------
    dict
        An updated dict of the options passed, with values updated according to the config file
    """
    # open the config
    f = open(options['config'])
    config = json.load(f)

    # create a new dict to store the new options
    new_options = options
    
    # iterate over all the options and update the values accordingly
    for option, value in options.items():
        if option == 'port' and value is None and 'port' in config:
            try:
                new_options['port'] = int(config['port'])
            except:
                print('ERR: Port number ("port") in config file ("'+str(options['config'])+'") "'+str(config['port'])+'" is not a valid integer.')
                exit()
        elif option == 'eth2_rpc' and value is None and 'eth2_rpc' in config:
            if not check_endpoint_validity_eth2(config['eth2_rpc']):
                print('ERR: Consensus layer endpoint ("eth2_rpc") in config file ("'+str(options['config'])+'") "'+str(config['eth2_rpc'])+'" did not return a successful response. Ensure the endpoint is correct, and that the node is a fully-synced consensus layer node that supports the Beacon Node API spec (https://ethereum.github.io/beacon-APIs/).')
                exit()
            new_options['eth2_rpc'] = config['eth2_rpc']
        elif option == 'rewards' and value is None and ('rewards' in config or 'reward_metrics' in config):
            if 'rewards' in config:
                if not isinstance(config['rewards'], bool):
                    print('ERR: Value in config file ("'+str(options['config'])+'") for "rewards" option is not a valid boolean.')
                    exit()
                new_options['rewards'] = config['rewards']
            elif 'reward_metrics' in config:
                if not isinstance(config['reward_metrics'], bool):
                    print('ERR: Value in config file ("'+str(options['config'])+'") for "reward_metrics" option is not a valid boolean.')
                    exit()
                new_options['rewards'] = config['reward_metrics']
        elif option == 'eth1_rpc' and value is None and 'eth1_rpc' in config:
            if not check_endpoint_validity_eth1(config['eth1_rpc']):
                print('ERR: Execution layer endpoint ("eth1_rpc") in config file ("'+str(options['config'])+'") "'+str(config['eth2_rpc'])+'" did not return a successful response. Ensure the endpoint is correct, and that the node is a fully-synced execution layer node.')
                exit()
            new_options['eth1_rpc'] = config['eth1_rpc']
        elif option == 'eth1_parallel' and value is None and ('eth1_parallel' in config or 'parallel_requests_eth1' in config):
            if 'eth1_parallel' in config:
                if not isinstance(config['eth1_parallel'], bool):
                    print('ERR: Value in config file ("'+str(options['config'])+'") for "eth1_parallel" option is not a valid boolean.')
                    exit()
                new_options['eth1_parallel'] = config['eth1_parallel']
            elif 'parallel_requests_eth1' in config:
                if not isinstance(config['parallel_requests_eth1'], bool):
                    print('ERR: Value in config file ("'+str(options['config'])+'") for "parallel_requests_eth1" option is not a valid boolean.')
                    exit()
                new_options['eth1_parallel'] = config['parallel_requests_eth1']
        elif option == 'eth2_parallel' and value is None and ('eth2_parallel' in config or 'parallel_requests_eth2' in config):
            if 'eth2_parallel' in config:
                if not isinstance(config['eth2_parallel'], bool):
                    print('ERR: Value in config file ("'+str(options['config'])+'") for "eth2_parallel" option is not a valid boolean.')
                    exit()
                new_options['eth2_parallel'] = config['eth2_parallel']
            elif 'parallel_requests_eth2' in config:
                if not isinstance(config['parallel_requests_eth2'], bool):
                    print('ERR: Value in config file ("'+str(options['config'])+'") for "parallel_requests_eth2" option is not a valid boolean.')
                    exit()
                new_options['eth2_parallel'] = config['parallel_requests_eth2']
        elif option == 'pubkeys_file' and value is None and options['pubkeys'] is None and ('pubkeys_file' in config or 'keys_file' in config):
            if 'pubkeys_file' in config:
                if not exists(config['pubkeys_file']):
                    print('ERR: Path in config file ("'+str(options['config'])+'") for "pubkeys_file" "'+str(config['pubkeys_file'])+'" is not valid, or the file does not exist.')
                    exit()
                new_options['pubkeys_file'] = config['pubkeys_file']
            elif 'keys_file' in config:
                if not exists(config['keys_file']):
                    print('ERR: Path in config file ("'+str(options['config'])+'") for "keys_file" "'+str(config['keys_file'])+'" is not valid, or the file does not exist.')
                    exit()
                new_options['pubkeys_file'] = config['keys_file']
        elif option == 'last_slot' and value is None and 'last_slot' in config:
            try:
                new_options['last_slot'] = int(config['last_slot'])
            except:
                print('ERR: Last slot value ("last_slot") in config file ("'+str(options['config'])+'") "'+str(config['last_slot'])+'" is not a valid integer.')
                exit()
        elif option == 'prune' and value is None and ('prune' in config or 'pruning' in config):
            if 'prune' in config:
                if not isinstance(config['prune'], bool):
                    print('ERR: Value in config file ("'+str(options['config'])+'") for "prune" option is not a valid boolean.')
                    exit()
                new_options['prune'] = config['prune']
            elif 'pruning' in config:
                if not isinstance(config['pruning'], bool):
                    print('ERR: Value in config file ("'+str(options['config'])+'") for "pruning" option is not a valid boolean.')
                    exit()
                new_options['prune'] = config['pruning']
        elif option == 'keep_last_slots' and value is None and 'keep_last_slots' in config:
            try:
                new_options['keep_last_slots'] = int(config['keep_last_slots'])
            except:
                print('ERR: Keep last slots value ("keep_last_slots") in config file ("'+str(options['config'])+'") "'+str(config['keep_last_slot'])+'" is not a valid integer.')
                exit()
        elif option == 'sync_committee' and value is None and ('sync_committee' in config or 'sync_committee_participation' in config):
            if 'sync_committee' in config:
                if not isinstance(config['sync_committee'], bool):
                    print('ERR: Value in config file ("'+str(options['config'])+'") for "sync_committee" option is not a valid boolean.')
                    exit()
                new_options['sync_committee'] = config['sync_committee']
            elif 'sync_committee_participation' in config:
                if not isinstance(config['sync_committee_participation'], bool):
                    print('ERR: Value in config file ("'+str(options['config'])+'") for "sync_committee_participation" option is not a valid boolean.')
                    exit()
                new_options['sync_committee'] = config['sync_committee_participation']
        elif option == 'pubkeys' and value is None and options['pubkeys_file'] is None and 'pubkeys' in config:
            new_options['pubkeys'] = config['pubkeys']
        elif option == 'relay_config' and value is None and 'relay_config' in config:
            if not exists(config['relay_config']):
                print('ERR: Path in config file ("'+str(options['config'])+'") for "relay_config" "'+str(config['relay_config'])+'" is not valid, or the file does not exist.')
                exit()
            new_options['relay_config'] = config['relay_config']
    
    return new_options
    
def none_to_default(options: dict) -> dict:
    """
    Reads the options passed and changes any 'None' values to the default value

    Parameters:
    -----------
    options : dict
        A dict of command-line options as passed from the main script

    Returns:
    --------
    dict
        An updated dict of the options passed, with values updated accordingly
    """

    # create variable to hold edited options
    new_options = options

    # iterate over the options
    for option, value in options.items():
        if option == 'relay_config' and value is None:
            if not exists('../default/relay_config.json'):
                print('ERR: No value was passed for option "relay_config" and the config was not found in the default location ("'+getcwd()+'/../default/relay_config.json").')
                exit()
            new_options['relay_config'] = '../default/relay_config.json'
        elif option == 'port' and value is None:
            print('WARN: No value was passed for option "port". Defaulting to port 7999.')
            new_options['port'] = 7999
        elif option == 'pubkeys_file' and value is None and options['pubkeys'] is None:
            if not exists('../data/pubkeys.txt'):
                print('ERR: No value was passed for option "pubkeys_file" or "pubkeys" and the file was not found in the default location ("'+getcwd()+'/../data/pubkeys.txt").')
                exit()
            new_options['pubkeys_file'] = '../data/pubkeys.txt'
        elif option == 'eth1_rpc' and value is None and options['rewards'] is True:
            print('ERR: Reward metrics are enabled but no value was passed for "eth1_rpc". Please provide an execution layer endpoint to enable reward metrics.')
            exit()
        elif option == 'eth2_rpc' and value is None:
            print('ERR: No value was passed for "eth2_rpc". A consensus layer endpoint is required for the tool to work, please provide a valid endpoint.')
            exit()
        elif option == 'prune' and value is True and options['keep_last_slots'] is None:
            print('WARN: No value was passed for option "keep_last_slots" but pruning is enabled. Defaulting to keeping the last 100 slots.')
            new_options['keep_last_slots'] = 100
        elif option == 'prune' and value is None:
            new_options['prune'] = False
        elif option == 'keep_last_slots' and value is not None and (options['prune'] is None or options['prune'] is False):
            print('WARN: A value was passed for option "keep_last_slots", however pruning was not enabled. The value will be ignored.')
        elif option == 'rewards' and value is None:
            new_options['rewards'] = False
        elif option == 'sync_committee' and value is None:
            new_options['sync_committee'] = False
        elif option == 'last_slot' and value is None:
            new_options['last_slot'] = 0
        elif option == "eth1_parallel" and value is None and options['eth1_rpc'] is not None:
            new_options['eth1_parallel'] = False
        elif option == "eth2_parallel" and value is None:
            new_options['eth2_parallel'] = False
        elif option == "pubkeys_file" and value is not None and options['pubkeys'] is not None:
            print('WARN: Values were passed for both "pubkeys" and "pubkeys_file". Only keys passed through "pubkeys" will be considered.')
        elif option == "eth1_rpc" and value is not None and (options['rewards'] is None or options['rewards'] is False):
            print('WARN: An execution layer endpoint was provided but reward metrics were not enabled.')

    return new_options

def csv_to_list(csv: str) -> list:
    """
    Converts a comma-separated list in string form to a list

    Parameters:
    -----------
    csv : str
        A string containing the comma-separated values

    Returns:
    --------
    list
        The comma-separated values as a list
    """

    # split the contents
    values = csv.split(',')

    # create new list to hold unique values
    unique = []

    # remove any duplicates
    for value in values:
        # remove any whitespace and new lines
        value = value.replace(' ','').replace('\r','').replace('\n','')

        # check if it is in our new list
        if value not in unique:
            unique.append(value)

    return unique

def read_keys_from_str(keys_str: str) -> list:
    """
    Reads the public keys from a string (comma-separated values) by calling other functions

    Parameters:
    -----------
    keys_str : str
        A string containing the comma-separated keys

    Returns:
    --------
    list
        The public keys as a list
    """
    print('INF: Reading public keys from passed parameter.')

    # first convert the keys to a list
    keys = csv_to_list(keys_str)

    # ensure all the keys look like hex strings, and return
    return clean_keys(keys)

def read_keys_from_file(path: str) -> list:
    """
    Reads the public keys from a file by calling other functions

    Parameters:
    -----------
    path : str
        The path to the file containing the keys as csv

    Returns:
    --------
    list
        The public keys as a list
    """
    print('INF: Reading public keys from file provided.')

    # open and read the contents of the file
    with open(path, 'r') as f:
        txt = f.read()

        # first convert the keys to a list
        keys = csv_to_list(txt)

        # ensure all the keys look like hex strings, and return
        return clean_keys(keys)
        
def clean_keys(keys: list) -> list:
    """
    Analyzes a list of keys, and alerts the user for any 'keys' that are not hex

    Parameters:
    -----------
    keys : list
        A list of the public keys

    Returns:
    --------
    list
        A list of keys, but only those which are hex
    """
    # first remove any empty strings
    keys = list(filter(None, keys))

    # analyze list and ensure they all look like hex
    keys = [x for x in keys if check_hex_and_alert(x)]

    # if none of the keys were valid, alert and exit
    if len(keys) == 0:
        print("ERR: No valid keys found in public keys list.")
        exit()
    else:
        print("INF: Successfully read "+str(len(keys))+" keys.")
    return keys

def check_hex_and_alert(hex: str) -> bool:
    """
    Checks that a string starts with '0x' and alerts otherwise

    Parameters:
    -----------
    hex : str
        The string to check

    Returns:
    --------
    bool
        Whether the string starts with '0x' or not
    """
    if hex.startswith('0x'):
        return True
    else:
        print('WARN: String "'+str(hex)+'" found in public keys file is not deemed as a valid public key and will be ignored.')
        return False
