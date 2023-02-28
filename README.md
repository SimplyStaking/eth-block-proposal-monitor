# ETH2 Block Proposal Monitor
ETH2 Block Proposal Monitor is a tool for Ethereum validators to monitor block proposals and rewards of their validators. Through Prometheus metrics, it provides the tools required to monitor and alert whenever a validator proposes an empty block, or misses the proposal. It also contains functionality to monitor MEV rewards, as well as regular rewards, generated by the validators monitored.


## Installation Guide

### Requirements:
- A beacon chain RPC node, such as Prysm. This is required to monitor blocks that were not relayed by a relayer.
- List of validators' public keys to monitor.
- Optional: An ETH1 RPC node for manual reward calculation.

### Setup
1. Install Python 3.8+ (we are using v3.8.10).
2. Install the required packages by running: `pip3 install -r requirements.txt` or `pip install -r requirements.txt`.
3. In `config.json`, edit the value of `"eth2_rpc"` to the endpoint of your beacon chain RPC node.
4. In `config.json`, edit the value of `"reward_metrics"` to true or false. If set to true, ensure to also populate the value of `"eth1_rpc"` to the endpoint of your ETH1 RPC node. Additionally, the value of `"parallel_requests_eth1"` may be changed, which significantly reduces the number of requests done to the ETH1 RPC node per second.
5. In `config.json`, edit the value of `"sync_committee_participation"` to true or false. If set to true, you can additionally edit the value of `"parallel_requests_eth2"`, which significantly reduces the number of requests done to the Beacon Chain node when first getting the Validator indexes.
5. Optional: In `config.json`, edit the value of `"port"` to your preferred value. This value determines on which port the Prometheus metrics will be outputted on.
6. Optional: In `config.json`, the value of `"continue_from_last_slot"` can be edited to start from a later/earlier slot. By default, the first time the script is ran, it will only gather the last 100 slots. After enabling `"continue_from_last_slot"`, also edit the value of `"last_slot"` to the slot value to start gathering data from.
7. Optional: In `config.json`, you can enable `"pruning"` to delete old slot data. You can choose the number of last slots to keep by editing the `"keep_last_slots"` value.
8. Populate `pubkeys.txt` with the public keys of the validators you'd like to monitor. This is a comma-separated list.

### Setup (Linux)
If you're on Linux, you can follow these commands to create a user and run the script as that user:
```bash
# create the non-root user
adduser monitoring --disabled-login
# install python3 with pip
sudo apt-get update -y && sudo apt-get install python3-pip -y

# as the user, clone the repository
su monitoring
cd
git clone https://github.com/SimplyVC/eth-block-proposal-monitor
cd eth-block-proposal-monitor

# install the requirements using pip
pip3 install -r requirements.txt

# at this point you can edit the config.json file to provide an RPC endpoint etc (see Setup above)

exit

# as root, make the setup script executable
chmod +x /home/monitoring/eth-block-proposal-monitor/setup.sh
# run the script and check the logs of the service file to ensure it is working fine
/home/monitoring/eth-block-proposal-monitor/setup.sh && journalctl -f -u eth2_block_monitoring
```

### Config File
The config file contains the following options:
- `"port"`: The port on which the Prometheus metrics will be published.
- `"eth2_rpc"`: The endpoint of the beacon chain node.
- `"reward_metrics"`: Whether to keep track and publish reward metrics or not.
- `"eth1_rpc"`: The endpoint of an ETH1 RPC node. This is used for calculating rewards of blocks, therefore is only needed if `reward_metrics` is set to true.
- `"parallel_requests_eth1"`: Whether to perform parallel requests when gathering data from the ETH1 RPC node. Ideally this is set to true, however it can be set to false at the cost of slower calculation if the endpoint is overloaded or cannot keep up with requests.
- `"parallel_requests_eth2"`: Whether to perform parallel requests when getting validator indexes (only done once) from the Beacon Chain node. If set to true, generally the data is gathered significantly faster, at the expense of the endpoint being more overloaded.
- `"keys_file"`: The name of the file containing a comma-separated list of the public keys of the validators to monitor.
- `"continue_from_last_slot"`: Whether to continue from the last slot in the database or not.
- `"last_slot"`: The slot to continue from. It is ignored if `continue_from_last_slot` is set to true.
- `"pruning"`: Whether to delete slot data in the database or not. If enabled, two new tables will be created in the database and only the last `keep_last_slots` will be kept in the slots table. The two new tables will contain the information needed to keep the metrics accurate.
- `"keep_last_slots"`: The number of slots to keep in the slots table in the database. Regarded only if pruning is enabled.
- `"sync_committee_participation"`: Whether to keep track of validators' sync committee participation (and publish the corresponding metrics) or not.

### Usage
After running the `main.py` script, Prometheus metrics are outputted on localhost on your chosen port. The script queries relays that are included in `relay_config.json` every 20 seconds, and stores information about each slot. After each iteration, the data is saved (to `slot_data.db` - this is an sqlite3 database) and the metrics are published.

### Updating
Before updating to a newer version or commit, we always recommend saving a copy of your database (i.e. `/data/slot_data.db`), so that you can rollback.

## Metrics
The tool contains the following list of Prometheus metrics:
1. `RelayBlocksProposed{relay} -> int`: The number of blocks proposed per relay by the validators we are monitoring.
2. `ValidatorBlocksProposed{public_key} -> int`: The number of blocks proposed by each validator that we are monitoring.
3. `MissedBlockProposals{public_key} -> int`: The number of missed block proposals by each validator that we are monitoring.
4. `EmptyBlockProposals{public_key} -> int`: The number of empty blocks proposed by each validator that we are monitoring.
5. `TotalRelayBlocksProposed{relay} -> int`: The total number of blocks proposed through each relay (since the first slot monitored by the script). This includes blocks proposed by validators that are not being monitored.
6. `RelayTotalRewards{relay} -> float`: The total reward (in ETH) generated by each relay. This metric is global, i.e. it includes rewards generated by validators that are not being monitored.
7. `AvgRelayerRewards{relay} -> float`: The average reward (in ETH) generated by each relay. This metric is global.
8. `UnknownRewardBlocks{relay} -> int`: The number of blocks with an unknown reward per each relay. Blocks will have an unknown reward when there is a missed proposal or when the block is empty. This metric is global.
9. `TotalValidatorRewards{relay} -> float`: The total reward (in ETH) generated through each relay by the validators we are monitoring.
10. `AvgValidatorRewards{relay} -> float`: The average reward (in ETH) generated through each relay by the validators we are monitoring.
11. `ValUnknownRewardBlocks{relay} -> int`: The number of blocks with an unknown reward proposed by the validators we are monitoring through each relay.
12. `ValidatorSyncParticipated{public_key, epoch} -> int`: The number of slots the validator participated in, in the sync committee starting at the epoch indicated.
13. `ValidatorSyncMissed{public_key, epoch} -> int`: The number of slots the validator did not participate in, in the sync committee starting at the epoch indicated.
14. `CurrentSyncCommitteeEpoch{epoch} -> int`: If metric value is 1, it is the starting epoch of the current sync committee, if it is 0, then it is the starting epoch of the previous sync committee. This metric is mostly used for the Grafana dashboard.