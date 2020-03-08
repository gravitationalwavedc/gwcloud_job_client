# Send by the server when it has finished setting up the connection and is ready for messages
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
