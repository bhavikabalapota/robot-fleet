"""Protocol constants. The multicast address is the only fixed network value."""

MCAST_GROUP = "224.1.1.48"
MCAST_PORT = 50048
MCAST_TTL = 1
BUFFER_SIZE = 65535

TICK = 0.1                  # main timer loop resolution (seconds)

# Discovery
HELLO_REPEAT = 3            # initial HELLO is sent this many times
HELLO_REPEAT_GAP = 0.2      # seconds between the initial HELLOs
HELLO_INTERVAL = 5.0        # periodic HELLO
MEMBER_TIMEOUT = 15.0       # drop a member not heard from for this long
DISCOVERY_WAIT = 2.0        # no reply within this -> alone, become dispatcher

# Reliable unicast
RETRY_INTERVAL = 0.4        # resend an un-ACKed message after this long
MAX_RETRIES = 7             # give up (suspect the receiver) after this many sends
SEEN_LIMIT = 5000           # remembered message IDs for duplicate suppression

# Heartbeat / failure detection
HEARTBEAT_INTERVAL = 2.0
HEARTBEAT_MISSES = 3        # 3 * 2 s = 6 s

# Bully election
ELECTION_ANSWER_TIMEOUT = 2.0
COORDINATOR_TIMEOUT = 5.0

# Total order / failover
NACK_INTERVAL = 0.5         # at most one NACK per this interval
NACK_MAX_BATCH = 100        # max operations re-sent for one NACK
RESUBMIT_INTERVAL = 1.0     # resend own not-yet-ordered ops (bids etc.)
SYNC_TIMEOUT = 3.0          # new dispatcher waits this long for SYNC_REPLYs

# State transfer (TCP)
STATE_TIMEOUT = 3.0

# Task allocation
DEFAULT_BID_TIME = 8.0      # seconds a task stays open for bidding
LOAD_PENALTY = 15.0         # cost added per task a robot already has
