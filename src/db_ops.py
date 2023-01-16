import sqlite3 as sl
import json
from os.path import dirname

database_name = dirname(__file__)+'/../data/slot_data.db'

def initialise_db():
    """
    Initialises the database by calling several functions

    Returns:
    --------
    obj
        The data object used by the main function that has the slots and the various metrics
    """
    create_db()
    insert_validators()
    insert_relayers()
    return populate_data_obj()

def create_db():
    """
    Creates the database in which the data is stored
    """
    con = sl.connect(database_name)
    cur = con.cursor()

    # creating the tables and links between them
    cur.execute("""
    CREATE TABLE validators (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        public_key TEXT,
        UNIQUE(public_key)
    )
    """)
    cur.execute("""
    CREATE TABLE relayers (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        endpoint TEXT,
        UNIQUE(endpoint)
    )
    """)
    cur.execute("""
    CREATE TABLE slots (
        number INTEGER NOT NULL PRIMARY KEY,
        proposer_id INTEGER NULL,
        proposer_pubkey TEXT NULL,
        relay_id INTEGER,
        missed INTEGER,
        empty INTEGER,
        reward REAL NULL,
        FOREIGN KEY(proposer_id) REFERENCES validators(id),
        FOREIGN KEY(relay_id) REFERENCES relayers(id)
    )
    """)

    con.commit()
    con.close()

def insert_validators():
    """
    Reads the file containing the public keys of the validators and inserts them into the database
    """
    con = sl.connect(database_name)

    # open the validators file
    f = open(dirname(__file__)+'/../config/config.json')
    config = json.load(f)

    with open(dirname(__file__)+'/../config/'+config["keys_file"], 'r') as fp:
        txt = fp.read()
        keys = txt.split(",")

    # filling the validators table
    cur = con.cursor()

    for key in keys:
        cur.execute('INSERT OR IGNORE INTO validators(public_key) VALUES(?)', (key,))

    con.commit()
    con.close()

def insert_relayers():
    """
    Reads the file containing the relayers and inserts them into the database
    """
    con = sl.connect(database_name)

    # open relayers file
    f = open(dirname(__file__)+'/../config/relay_config.json')
    relay_config = json.load(f)

    # filling the relayers table
    cur = con.cursor()

    for key, value in relay_config.items():
        cur.execute('INSERT OR IGNORE INTO relayers(name, endpoint) VALUES (?, ?)', (key, value,))

    # also insert 'Unknown'
    cur.execute('INSERT OR IGNORE INTO relayers(name, endpoint) VALUES (?, ?)', ('Unknown', 'N/A'))

    con.commit()
    con.close()

def insert_new_slot(slot_number: int, proposer: str, relayer: str, missed: bool, empty: bool, reward: float):
    """
    Inserts a new slot into the table containing slots in the database

    Parameters:
    -----------
    slot_number : int
        The slot number
    proposer : str / hex
        The public key of the proposer of the slot / block
    relayer : str
        The name of the relay used to propose the slot / block
    missed : bool
        Whether the slot / block was missed
    empty : bool
        Whether the slot / block is empty (has no transactions)
    reward : float
        The reward generating by proposing the block
    """
    con = sl.connect(database_name)
    cur = con.cursor()

    # if reward is -1, then it is unknown
    if reward == -1:
        reward = None

    # get required values
    cur.execute('SELECT id FROM validators WHERE public_key = ?', (proposer,))
    proposer_id = -1
    result = cur.fetchall()
    if len(result) > 0:
        proposer_id = result[0][0]
    
    cur.execute('SELECT id FROM relayers WHERE name = ?', (relayer,))
    relay_id = -1
    result = cur.fetchall()
    if len(result) > 0:
        relay_id = result[0][0]
    
    if relay_id == -1:
        raise Exception("The relayer '"+relayer+"' could not be found. Please update your relay config and try again.")

    if proposer_id == -1:
        # proposer is not in our list, so we cannot link tables
        cur.execute("""
        INSERT OR IGNORE INTO slots(number, proposer_pubkey, relay_id, missed, empty, reward)
        VALUES(?, ?, ?, ?, ?, ?)
        """, (slot_number, proposer, relay_id, missed, empty, reward,))
    else:
        # proposer is in our list, we can link tables
        cur.execute("""
        INSERT OR IGNORE INTO slots(number, proposer_id, relay_id, missed, empty, reward)
        VALUES(?, ?, ?, ?, ?, ?)
        """, (slot_number, proposer_id, relay_id, missed, empty, reward,))

    con.commit()
    con.close()

def execute_query_dict(sql: str):
    """
    Executes an SQL query and returns a dict where the values in the first column are the keys and those in the second are the values

    Parameters:
    -----------
    sql : str
        The SQL code to execute

    Returns:
    --------
    dict
        The dictionary with the first column being the keys of the second
    """
    # connect to the database and create a cursor
    con = sl.connect(database_name)
    cur = con.cursor()

    # execute the passed sql
    cur.execute(sql)

    # create an empty dictionary to hold the results
    dict_result = {}

    # fetch all the results
    result = cur.fetchall()

    # if the result contains any rows
    if len(result) > 0:
        for row in result:
            # add the rows to the dict
            dict_result[row[0]] = row[1]
    
    # close the connection and return the results
    con.close()
    return dict_result

def get_relay_blocks_proposed():
    """
    Gets the total number of blocks relayed by our validators through each relayer

    Returns:
    --------
    dict
        The key is the name of the relayer and the value is the number of blocks relayed by our validators (i.e. validators we're monitoring)
    """
    sql = """
    SELECT r.name AS 'relay_name', COUNT(*) AS 'count'
    FROM slots s
    JOIN validators v
        ON s.proposer_id = v.id
    JOIN relayers r
        ON s.relay_id = r.id
    GROUP BY r.name
    """

    return execute_query_dict(sql)

def get_validator_blocks_proposed():
    """
    Gets the total number of blocks relayed by each validator

    Returns:
    --------
    dict
        The key is the public key of the validator and the value is the number of blocks relayed
    """
    sql = """
    SELECT v.public_key, COUNT(*) AS 'count'
    FROM slots s
    JOIN validators v
        ON s.proposer_id = v.id
    GROUP BY v.public_key
    """

    return execute_query_dict(sql)

def get_missed_block_proposals():
    """
    Gets the total number of missed blocks (>0) by each validator

    Returns:
    --------
    dict
        The key is the public key of the validator and the value is the number of blocks missed (if a validator did not miss any blocks, it will not show up here)
    """
    sql = """
    SELECT v.public_key, COUNT(*) AS 'count'
    FROM slots s
    JOIN validators v
        ON s.proposer_id = v.id
    WHERE s.missed = 1
    GROUP BY v.public_key
    """
    
    return execute_query_dict(sql)

def get_empty_block_proposals():
    """
    Gets the total number of empty blocks (>0) by each validator

    Returns:
    --------
    dict
        The key is the public key of the validator and the value is the number of empty blocks proposed (if a validator did not propose any empty blocks, it will not show up here)
    """
    sql = """
    SELECT v.public_key, COUNT(*) AS 'count'
    FROM slots s
    JOIN validators v
        ON s.proposer_id = v.id
    WHERE s.empty = 1
    GROUP BY v.public_key
    """

    return execute_query_dict(sql)

def get_total_relay_blocks_proposed():
    """
    Gets the total number of blocks relayed by each relayer

    Returns:
    --------
    dict
        The key is the name of the relay and the value is the total number of blocks relayed by that relayer
    """
    sql = """
    SELECT r.name, COUNT(*) AS 'count'
    FROM slots s
    JOIN relayers r
        ON s.relay_id = r.id
    GROUP BY r.name
    """

    return execute_query_dict(sql)

def get_relay_total_rewards():
    """
    Gets the total reward generated by each relay

    Returns:
    --------
    dict
        The key is the name of the relay and the value is the total reward value of using that relay
    """
    sql = """
    SELECT r.name, SUM(s.reward) AS 'reward'
    FROM slots s
    JOIN relayers r
        ON s.relay_id = r.id
    GROUP BY r.name
    """

    return execute_query_dict(sql)

def get_avg_relay_rewards():
    """
    Gets the average reward per block by each relay

    Returns:
    --------
    dict
        The key is the name of the relay and the value is the average reward value of using that relay
    """
    sql = """
    SELECT r.name, AVG(s.reward) AS 'reward'
    FROM slots s
    JOIN relayers r
    ON s.relay_id = r.id
    WHERE s.missed = 0 AND s.empty = 0 AND s.reward IS NOT NULL
    GROUP BY r.name
    """

    return execute_query_dict(sql)

def get_unknown_reward_blocks():
    """
    Gets the total number of blocks with an unknown reward value per relay

    Returns:
    --------
    dict
        The key is the name of the relay and the value is the total number of blocks with an unknown reward value
    """
    sql = """
    SELECT r.name, COUNT(*) AS 'count'
    FROM slots s
    JOIN relayers r
        ON s.relay_id = r.id
    WHERE s.reward IS NULL
    GROUP BY r.name
    """

    return execute_query_dict(sql)

def get_total_validator_rewards():
    """
    Gets the total reward generated per each relay for the validators we're monitoring

    Returns:
    --------
    dict
        The key is the name of the relay and the value is the total reward generated by our validators (for all blocks proposed that are in the db)
    """
    sql = """
    SELECT r.name, SUM(s.reward) AS 'reward'
    FROM slots s
    JOIN relayers r
        ON s.relay_id = r.id
    JOIN validators v
        ON s.proposer_id = v.id
    WHERE s.reward IS NOT NULL
    GROUP BY r.name
    """

    return execute_query_dict(sql)

def get_avg_validator_rewards():
    """
    Gets the average reward generated per each relay for the validators we're monitoring

    Returns:
    --------
    dict
        The key is the name of the relay and the value is the average reward generated (for all blocks proposed that are in the db)
    """
    sql = """
    SELECT r.name, AVG(s.reward) AS 'reward'
    FROM slots s
    JOIN relayers r
        ON s.relay_id = r.id
    JOIN validators v
        ON s.proposer_id = v.id
    WHERE s.reward IS NOT NULL
    GROUP BY r.name
    """

    return execute_query_dict(sql)

def get_validator_unknown_reward_blocks():
    """
    Gets the total number of blocks with an unknown reward value per each relay for the validators we're monitoring

    Returns:
    --------
    dict
        The key is the name of the relay and the value is the total number of blocks with an unknown reward (for all blocks proposed that are in the db)
    """
    sql = """
    SELECT r.name, COUNT(*) AS 'count'
    FROM slots s
    JOIN relayers r
        ON s.relay_id = r.id
    JOIN validators v
        ON s.proposer_id = v.id
    WHERE s.reward IS NULL
    GROUP BY r.name
    """

    return execute_query_dict(sql)

def get_relayers_list():
    """
    Gets the name and endpoint of each relayer in the db

    Returns:
    --------
    dict
        The key is the name of the relayer and the value is the endpoint used to reach that relayer
    """
    sql = """
    SELECT name, endpoint
    FROM relayers
    """

    return list(execute_query_dict(sql).keys())

def populate_data_obj():
    """
    Populates the data object used by the main script using the data from the db

    Returns:
    --------
    dict
        Contains the number of the latest slot, an array of all the slots from the db, and the values of the various metrics
    """

    data_obj = {
        "last_slot": 0,
        "slots": [],
        "latest_metrics": {
            "RelayBlocksProposed": {},
            "TotalRelayBlocksProposed": {},
            "RelayTotalRewards": {},
            "AvgRelayerRewards": {},
            "UnknownRewardsBlocks": {},
            "ValidatorBlocksProposed": {},
            "MissedBlockProposals": {},
            "EmptyBlockProposals": {},
            "TotalValidatorRewards": {},
            "AvgValidatorRewards": {},
            "ValUnknownRewardBlocks": {}
        }
    }

    # get slots
    con = sl.connect(database_name)
    cur = con.cursor()
    cur.execute("""
    SELECT s.number, COALESCE(s.proposer_pubkey, v.public_key), r.name, s.missed, s.empty
    FROM slots s
    JOIN relayers r
        ON s.relay_id = r.id
    LEFT JOIN validators v
        ON s.proposer_id = v.id
    ORDER BY s.number ASC
    """)

    # populate slots array
    slots = []
    result = cur.fetchall()
    if len(result) > 0:
        for row in result:
            slots.append({
                "slot": row[0],
                "proposer": row[1],
                "relay": row[2],
                "missed": True if row[3] == 1 else False,
                "empty": True if row[4] == 1 else False
            })

    # fill the data object values
    data_obj['slots'] = slots
    data_obj['last_slot'] = slots[-1]['slot'] if len(slots) > 0 else 0
    data_obj['latest_metrics'] = get_metrics_from_db()

    # initialise certain metrics to avoid key exceptions
    for relayer in get_relayers_list():
        if relayer not in data_obj['latest_metrics']['RelayBlocksProposed']:
            data_obj['latest_metrics']['RelayBlocksProposed'][relayer] = 0
        
        if relayer not in data_obj['latest_metrics']['TotalRelayBlocksProposed']:
            data_obj['latest_metrics']['TotalRelayBlocksProposed'][relayer] = 0
        
        if relayer not in data_obj['latest_metrics']['RelayTotalRewards']:
            data_obj['latest_metrics']['RelayTotalRewards'][relayer] = 0
        
        if relayer not in data_obj['latest_metrics']['AvgRelayerRewards']:
            data_obj['latest_metrics']['AvgRelayerRewards'][relayer] = 0
        
        if relayer not in data_obj['latest_metrics']['UnknownRewardsBlocks']:
            data_obj['latest_metrics']['UnknownRewardsBlocks'][relayer] = 0

        if relayer not in data_obj['latest_metrics']['TotalValidatorRewards']:
            data_obj['latest_metrics']['TotalValidatorRewards'][relayer] = 0
        
        if relayer not in data_obj['latest_metrics']['AvgValidatorRewards']:
            data_obj['latest_metrics']['AvgValidatorRewards'][relayer] = 0

        if relayer not in data_obj['latest_metrics']['ValUnknownRewardBlocks']:
            data_obj['latest_metrics']['ValUnknownRewardBlocks'][relayer] = 0

    return data_obj

def create_archive_db():
    """
    Creates the archive database in which the data is stored after pruning the tables
    """
    con = sl.connect(database_name)
    cur = con.cursor()

    # creating the tables and links between them
    cur.execute("""
    CREATE TABLE archive_relayer_slots (
        relay_id INTEGER NOT NULL,
        total_slots INTEGER NULL,
        total_rewards REAL NULL,
        total_unknown_slots INTEGER NULL,
        UNIQUE(relay_id),
        FOREIGN KEY(relay_id) REFERENCES relayers(id)
    )
    """)
    cur.execute("""
    CREATE TABLE archive_validator_slots (
        validator_id INTEGER NOT NULL,
        relay_id INTEGER NOT NULL,
        total_slots INTEGER NULL,
        total_rewards REAL NULL,
        total_missed INTEGER NULL,
        total_empty INTEGER NULL,
        total_unknown_reward INTEGER NULL,
        UNIQUE(validator_id, relay_id),
        FOREIGN KEY(validator_id) REFERENCES validators(id),
        FOREIGN KEY(relay_id) REFERENCES relayers(id)
    )
    """)

    con.commit()
    con.close()

def archive_exists() -> bool:
    """
    Checks if the archive tables exist or not

    Returns:
    --------
    bool
        True if tables exist, False otherwise
    """
    con = sl.connect(database_name)
    cur = con.cursor()

    # see if tables exist
    cur.execute("""
    SELECT count(name)
    FROM sqlite_master
    WHERE type = 'table'
    AND name IN ('archive_relayer_slots', 'archive_validator_slots')
    """)

    if cur.fetchone()[0] == 2:
        con.close()
        return True
    
    con.close()
    return False

def prune_db(last_slots: int):
    if not archive_exists():
        create_archive_db()

    # get the slots we are getting rid of
    con = sl.connect(database_name)
    cur = con.cursor()

    cur.execute("""
    SELECT MAX(number)
    FROM slots
    """)

    cutoff = cur.fetchone()[0] - last_slots

    # see if there actually is anything to prune
    cur.execute("""
    SELECT COUNT(*)
    FROM slots
    WHERE number < ?
    """, (cutoff,))

    if cur.fetchone()[0] < 1:
        return

    # get the slots we are removing
    cur.execute("""
    SELECT v.id, s.relay_id, COUNT(*), SUM(s.reward)
    FROM slots s
    JOIN validators v
        ON s.proposer_id = v.id
    WHERE --s.reward IS NOT NULL AND s.empty = 0 AND s.missed = 0 AND 
        s.number < ?
    GROUP BY v.id, s.relay_id
    """, (cutoff,))

    result = cur.fetchall()

    result_dict = {}
    if len(result) > 0:
        for row in result:
            if row[0] not in result_dict:
                result_dict[row[0]] = {}
            if row[1] not in result_dict[row[0]]:
                result_dict[row[0]][row[1]] = {}
            result_dict[row[0]][row[1]]['count'] = row[2]
            result_dict[row[0]][row[1]]['reward'] = row[3]

    # get count of missed slots
    cur.execute("""
    SELECT v.id, s.relay_id, COUNT(*)
    FROM slots s
    JOIN validators v
        ON s.proposer_id = v.id
    WHERE s.missed = 1
        AND s.number < ?
    GROUP BY v.id, s.relay_id
    """, (cutoff,))

    result = cur.fetchall()

    if len(result) > 0:
        for row in result:
            if row[0] not in result_dict:
                result_dict[row[0]] = {}
            if row[1] not in result_dict[row[0]]:
                result_dict[row[0]][row[1]] = {}
            result_dict[row[0]][row[1]]['missed'] = row[2]
    
    # get count of empty slots
    cur.execute("""
    SELECT v.id, s.relay_id, COUNT(*)
    FROM slots s
    JOIN validators v
        ON s.proposer_id = v.id
    WHERE s.empty = 1
        AND s.number < ?
    GROUP BY v.id, s.relay_id
    """, (cutoff,))

    result = cur.fetchall()

    if len(result) > 0:
        for row in result:
            if row[0] not in result_dict:
                result_dict[row[0]] = {}
            if row[1] not in result_dict[row[0]]:
                result_dict[row[0]][row[1]] = {}
            result_dict[row[0]][row[1]]['empty'] = row[2]
        
    # get count of unknown reward slots
    cur.execute("""
    SELECT v.id, s.relay_id, COUNT(*)
    FROM slots s
    JOIN validators v
        ON s.proposer_id = v.id
    WHERE s.reward IS NULL
        AND s.number < ?
    GROUP BY v.id, s.relay_id
    """, (cutoff,))

    result = cur.fetchall()

    if len(result) > 0:
        for row in result:
            if row[0] not in result_dict:
                result_dict[row[0]] = {}
            if row[1] not in result_dict[row[0]]:
                result_dict[row[0]][row[1]] = {}
            result_dict[row[0]][row[1]]['unknown'] = row[2]

    for validator, dict in result_dict.items():
        for relay, dict2 in dict.items():
            values = [validator, relay,                                 # 0, 1
                    dict2['count'] if 'count' in dict2 else None,       # 2
                    dict2['reward'] if 'reward' in dict2 else None,     # 3
                    dict2['missed'] if 'missed' in dict2 else None,     # 4
                    dict2['empty'] if 'empty' in dict2 else None,       # 5
                    dict2['unknown'] if 'unknown' in dict2 else None]   # 6
            cur.execute("""
            INSERT INTO archive_validator_slots (validator_id, relay_id, total_slots, total_rewards, total_missed, total_empty, total_unknown_reward)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (validator_id, relay_id) DO
            UPDATE
            SET
                total_slots = COALESCE(total_slots, 0) + COALESCE(excluded.total_slots, 0),
                total_rewards = COALESCE(total_rewards, 0) + COALESCE(excluded.total_rewards, 0),
                total_missed = COALESCE(total_missed, 0) + COALESCE(excluded.total_missed, 0),
                total_empty = COALESCE(total_empty, 0) + COALESCE(excluded.total_empty, 0),
                total_unknown_reward = COALESCE(total_unknown_reward, 0) + COALESCE(excluded.total_unknown_reward, 0)
            """, (values[0], values[1], values[2], values[3], values[4], values[5], values[6],))

    con.commit()

    # fill archive_relayer_slots table
    cur.execute("""
    SELECT relay_id, COUNT(*), SUM(reward)
    FROM slots
    WHERE reward IS NOT NULL
        AND number < ?
    GROUP BY relay_id
    """, (cutoff,))

    result = cur.fetchall()
    result_dict = {}

    if len(result) > 0:
        for row in result:
            result_dict[row[0]] = {
                'count': row[1],
                'reward': row[2]
            }
    
    cur.execute("""
    SELECT relay_id, COUNT(*)
    FROM slots
    WHERE reward IS NULL
        AND number < ?
    GROUP BY relay_id
    """, (cutoff,))

    result = cur.fetchall()

    if len(result) > 0:
        for row in result:
            if row[0] not in result_dict:
                result_dict[row[0]] = {}
            
            result_dict[row[0]]['unknown'] = row[1]
    
    for relay, dict in result_dict.items():
        values = [relay,
                dict['count'] if 'count' in dict else None,
                dict['reward'] if 'reward' in dict else None,
                dict['unknown'] if 'unknown' in dict else None]

        cur.execute("""
        INSERT INTO archive_relayer_slots (relay_id, total_slots, total_rewards, total_unknown_slots)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(relay_id) DO
        UPDATE
        SET
            total_slots = COALESCE(total_slots, 0) + COALESCE(excluded.total_slots, 0),
            total_rewards = COALESCE(total_rewards, 0) + COALESCE(excluded.total_rewards, 0),
            total_unknown_slots = COALESCE(total_unknown_slots, 0) + COALESCE(excluded.total_unknown_slots, 0)
        """, (values[0], values[1], values[2], values[3],))

    con.commit()

    # now delete the slots we pruned
    cur.execute("""
    DELETE
    FROM slots
    WHERE number < ?
    """, (cutoff,))

    con.commit()
    con.close()

def archive_get_relay_blocks_proposed():
    """
    Gets the total number of blocks relayed by our validators through each relayer from the archive tables

    Returns:
    --------
    dict
        The key is the name of the relayer and the value is the number of blocks relayed by our validators (i.e. validators we're monitoring)
    """
    sql = """
    SELECT r.name, SUM(s.total_slots)
    FROM archive_validator_slots s
    JOIN relayers r
        ON s.relay_id = r.id
    GROUP BY r.id
    """

    return execute_query_dict(sql)

def archive_get_validator_blocks_proposed():
    """
    Gets the total number of blocks relayed by each validator from the archive tables

    Returns:
    --------
    dict
        The key is the public key of the validator and the value is the number of blocks relayed
    """
    sql = """
    SELECT v.public_key, SUM(s.total_slots)
    FROM archive_validator_slots s
    JOIN validators v
        ON s.validator_id = v.id
    GROUP BY s.validator_id
    """

    return execute_query_dict(sql)

def archive_get_missed_block_proposals():
    """
    Gets the total number of missed blocks (>0) by each validator from the archive tables

    Returns:
    --------
    dict
        The key is the public key of the validator and the value is the number of blocks missed (if a validator did not miss any blocks, it will not show up here)
    """
    sql = """
    SELECT v.public_key, SUM(s.total_missed)
    FROM archive_validator_slots s
    JOIN validators v
        ON s.validator_id = v.id
    WHERE s.total_missed IS NOT NULL
    GROUP BY s.validator_id
    """
    
    return execute_query_dict(sql)

def archive_get_empty_block_proposals():
    """
    Gets the total number of empty blocks (>0) by each validator from the archive tables

    Returns:
    --------
    dict
        The key is the public key of the validator and the value is the number of empty blocks proposed (if a validator did not propose any empty blocks, it will not show up here)
    """
    sql = """
    SELECT v.public_key, SUM(s.total_empty)
    FROM archive_validator_slots s
    JOIN validators  v
        ON s.validator_id = v.id
    WHERE s.total_empty IS NOT NULL
    GROUP BY s.validator_id
    """

    return execute_query_dict(sql)

def archive_get_total_relay_blocks_proposed():
    """
    Gets the total number of blocks relayed by each relayer from the archive tables

    Returns:
    --------
    dict
        The key is the name of the relay and the value is the total number of blocks relayed by that relayer
    """
    sql = """
    SELECT r.name, s.total_slots+COALESCE(s.total_unknown_slots,0)
    FROM archive_relayer_slots s
    JOIN relayers r
        ON s.relay_id = r.id
    """

    return execute_query_dict(sql)

def archive_get_relay_total_rewards():
    """
    Gets the total reward generated by each relay from the archive tables

    Returns:
    --------
    dict
        The key is the name of the relay and the value is the total reward value of using that relay
    """
    sql = """
    SELECT r.name, s.total_rewards
    FROM archive_relayer_slots s
    JOIN relayers r
        ON s.relay_id = r.id
    """

    return execute_query_dict(sql)

def archive_get_avg_relay_rewards():
    """
    Gets the average reward per block by each relay from the archive tables and regular tables

    Returns:
    --------
    dict
        The key is the name of the relay and the value is the average reward value of using that relay
    """
    sql = """
    SELECT r.name, (a_s.total_rewards + COALESCE(SUM(s.reward),0)) / (a_s.total_slots + COALESCE(COUNT(s.reward),0))
    FROM archive_relayer_slots a_s
    JOIN relayers r
        ON a_s.relay_id = r.id
    LEFT JOIN slots s
        ON s.relay_id = r.id
        AND s.missed = 0
        AND s.empty = 0
        AND s.reward IS NOT NULL
    GROUP BY a_s.relay_id
    """

    return execute_query_dict(sql)

def archive_get_unknown_reward_blocks():
    """
    Gets the total number of blocks with an unknown reward value per relay from the archive tables

    Returns:
    --------
    dict
        The key is the name of the relay and the value is the total number of blocks with an unknown reward value
    """
    sql = """
    SELECT r.name, s.total_unknown_slots
    FROM archive_relayer_slots s
    JOIN relayers r
        ON s.relay_id = r.id
    WHERE s.total_unknown_slots IS NOT NULL
    """

    return execute_query_dict(sql)

def archive_get_total_validator_rewards():
    """
    Gets the total reward generated per each relay for the validators we're monitoring from the archive tables

    Returns:
    --------
    dict
        The key is the name of the relay and the value is the total reward generated (for all blocks proposed that are in the db)
    """
    sql = """
    SELECT r.name, SUM(s.total_rewards)
    FROM archive_validator_slots s
    JOIN relayers r
        ON s.relay_id = r.id
    WHERE s.total_rewards IS NOT NULL
    GROUP BY r.name
    """

    return execute_query_dict(sql)

def archive_get_avg_validator_rewards():
    """
    Gets the average reward generated per each relay for the validators we're monitoring from the archive tables and the regular tables

    Returns:
    --------
    dict
        The key is the name of the relay and the value is the average reward generated (for all blocks proposed that are in the db)
    """
    sql = """
    SELECT r.name, SUM(x.total_rewards) / SUM(x.total_slots)
    FROM (
        SELECT relay_id, total_rewards, total_slots - COALESCE(total_unknown_reward, 0) AS 'total_slots'
        FROM archive_validator_slots
        WHERE total_rewards IS NOT NULL
        UNION ALL
        SELECT s.relay_id, s.reward, 1
        FROM slots s
        JOIN validators v
            ON s.proposer_id = v.id
        WHERE s.reward IS NOT NULL AND s.missed = 0 AND s.empty = 0
    ) x
    JOIN relayers r
        ON x.relay_id = r.id
    GROUP BY x.relay_id
    """

    return execute_query_dict(sql)

def archive_get_validator_unknown_reward_blocks():
    """
    Gets the total number of blocks with an unknown reward value per each relay for the validators we're monitoring from the archive tables

    Returns:
    --------
    dict
        The key is the name of the relay and the value is the total number of blocks with an unknown reward (for all blocks proposed that are in the db)
    """
    sql = """
    SELECT r.name, SUM(s.total_unknown_reward)
    FROM archive_validator_slots s
    JOIN relayers r
        ON s.relay_id = r.id
    WHERE s.total_unknown_reward IS NOT NULL
    GROUP BY r.name
    """

    return execute_query_dict(sql)

def join_archive_queries(reg_dict: dict, archive_dict: dict, avg=False) -> dict:
    """
    Joins data coming from regular and archive tables

    Parameters:
    -----------
    reg_dict : dict
        The dict resulting from getting metrics from the regular tables
    archive_dict : dict
        The dict resulting from getting metrics from the archive tables
    avg : bool
        Whether the metrics are related to avgs - in this case we do not increment values

    Returns:
    --------
    dict
        A dict combining the data from the passed dicts
    """
    combined_dict = {}

    # add archive query results first
    for key, value in archive_dict.items():
        combined_dict[key] = value
    
    # add regular query results
    for key, value in reg_dict.items():
        if key in combined_dict:
            if avg:
                pass
            else:
                combined_dict[key] += value
        else:
            combined_dict[key] = value

    return combined_dict

def get_metrics_from_db():
    """
    Gets the metrics from the database by calling other functions

    Returns:
    --------
    dict
        A dict containing the metrics required by the script
    """
    # initialise a dict to hold results
    metrics = {}

    if archive_exists():
        # if we have an archive db, combine the data to get the metrics
        metrics['RelayBlocksProposed'] = join_archive_queries(get_relay_blocks_proposed(), archive_get_relay_blocks_proposed())
        metrics['TotalRelayBlocksProposed'] = join_archive_queries(get_total_relay_blocks_proposed(), archive_get_total_relay_blocks_proposed())
        metrics['RelayTotalRewards'] = join_archive_queries(get_relay_total_rewards(), archive_get_relay_total_rewards())
        metrics['AvgRelayerRewards'] = join_archive_queries(get_avg_relay_rewards(), archive_get_avg_relay_rewards(), avg=True)
        metrics['UnknownRewardsBlocks'] = join_archive_queries(get_unknown_reward_blocks(), archive_get_unknown_reward_blocks())
        metrics['ValidatorBlocksProposed'] = join_archive_queries(get_validator_blocks_proposed(), archive_get_validator_blocks_proposed())
        metrics['MissedBlockProposals'] = join_archive_queries(get_missed_block_proposals(), archive_get_missed_block_proposals())
        metrics['EmptyBlockProposals'] = join_archive_queries(get_empty_block_proposals(), archive_get_empty_block_proposals())
        metrics['TotalValidatorRewards'] = join_archive_queries(get_total_validator_rewards(), archive_get_total_validator_rewards())
        metrics['AvgValidatorRewards'] = join_archive_queries(get_avg_validator_rewards(), archive_get_avg_validator_rewards(), avg=True)
        metrics['ValUnknownRewardBlocks'] = join_archive_queries(get_validator_unknown_reward_blocks(), archive_get_validator_unknown_reward_blocks())
    else:
        # otherwise, just load the data from the regular tables
        metrics['RelayBlocksProposed'] = get_relay_blocks_proposed()
        metrics['TotalRelayBlocksProposed'] = get_total_relay_blocks_proposed()
        metrics['RelayTotalRewards'] = get_relay_total_rewards()
        metrics['AvgRelayerRewards'] = get_avg_relay_rewards()
        metrics['UnknownRewardsBlocks'] = get_unknown_reward_blocks()
        metrics['ValidatorBlocksProposed'] = get_validator_blocks_proposed()
        metrics['MissedBlockProposals'] = get_missed_block_proposals()
        metrics['EmptyBlockProposals'] = get_empty_block_proposals()
        metrics['TotalValidatorRewards'] = get_total_validator_rewards()
        metrics['AvgValidatorRewards'] = get_avg_validator_rewards()
        metrics['ValUnknownRewardBlocks'] = get_validator_unknown_reward_blocks()

    return metrics