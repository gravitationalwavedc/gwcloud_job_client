# Sent by the server when it has finished setting up the connection and is ready for messages
SERVER_READY = 1000

# Submits a new job
SUBMIT_JOB = 2000

# Sent by the client to the server to update the status of a job
# uint: The HpcJob id
# uint: The JobStatus
# string: Any additional information about this status
UPDATE_JOB = 2001

# Request a missing bundle from the server
REQUEST_BUNDLE = 3000

DOWNLOAD_FILE = 4000
FILE_DETAILS = 4001
FILE_ERROR = 4002
FILE_CHUNK = 4003
PAUSE_FILE_CHUNK_STREAM = 4004
RESUME_FILE_CHUNK_STREAM = 4005
FILE_LIST = 4006
FILE_LIST_ERROR = 4007
