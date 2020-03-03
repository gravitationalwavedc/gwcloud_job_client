import asyncio
import queue
import threading
from time import sleep


class Scheduler:
    class Priority:
        Lowest = 19
        Medium = 10
        Highest = 0

    def __init__(self, connection):
        self.connection = connection

        # Initialise the queue
        self.queue = []
        for i in range(Scheduler.Priority.Lowest):
            self.queue.append({})

        # Create the access lock
        self.mutex = threading.Lock()

        # Create a message notification event
        self.dataReady = threading.Event()

        # Start the prune thread
        self.prune_thread = threading.Thread(target=self.prune_sources, daemon=True)
        self.prune_thread.start()

        self.scheduler_thread = None

    def server_ready(self):
        # Start the scheduler thread
        self.scheduler_thread = threading.Thread(target=self.start_run, daemon=True)
        self.scheduler_thread.start()

    def start_run(self):
        asyncio.set_event_loop(asyncio.new_event_loop())
        asyncio.get_event_loop().run_until_complete(self.run())

    def queue_message(self, message):
        with self.mutex:
            if message.source not in self.queue[message.priority]:
                self.queue[message.priority][message.source] = queue.SimpleQueue()

            self.queue[message.priority][message.source].put(message.to_bytes())

        self.dataReady.set()

    async def run_next(self):
        # Iterate over the priorities
        for current_priority in range(len(self.queue)):
            had_data = False
            while True:
                with self.mutex:
                    # Iterate over the map
                    for k, v in self.queue[current_priority].items():
                        # Check if the vector for this source is empty
                        if not v.empty():
                            # Pop the next item from the queue
                            data = v.get()

                            # Send the data
                            await self.connection.send(data)

                            # Data existed
                            had_data = True

                # Check if there is higher priority data to send
                if self.does_higher_priority_data_exist(current_priority):
                    await self.run_next()

                if not had_data:
                    break

    async def run(self):
        # Iterate forever
        while True:
            # Wait for data to be ready to send
            self.dataReady.wait()
            self.dataReady.clear()

            await self.run_next()

    def prune_sources(self):
        # Iterate forever
        while True:
            # Sleep until the next prune
            sleep(60)

            # Acquire the lock to prevent more data being pushed on while we are pruning
            with self.mutex:
                def _next():
                    # Iterate over the priorities
                    for p in self.queue:
                        # Iterate over the map
                        for k, v in p.items():
                            # Check if the vector for this source is empty
                            if v.empty():
                                # Remove this source from the map
                                del p[k]
                                # Start again (Changing dict sizes in an iterator is illegal)
                                _next()
                                # Nothing more to do
                                break

                _next()

    def does_higher_priority_data_exist(self, max_priority):
        for priority in range(len(self.queue)):
            # Check if the current priority is greater or equal to max_priority and return false if not.
            if priority >= max_priority:
                return False

            # Iterate over the map
            for k, v in self.queue[priority].items():
                # Check if the vector for this source is empty
                if not v.empty():
                    return True

        return False
