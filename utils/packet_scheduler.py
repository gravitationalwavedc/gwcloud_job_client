import asyncio


class PacketScheduler:
    class Priority:
        Lowest = 19
        Medium = 10
        Highest = 0

    def __init__(self, connection):
        self.connection = connection

        # Initialise the queue
        self.queue = []
        for i in range(PacketScheduler.Priority.Lowest + 1):
            self.queue.append({})

        # Create the access lock
        self.mutex = asyncio.Lock()

        # Create a message notification event
        self.data_ready = asyncio.Event()

        # Start the prune thread
        self.prune_thread = asyncio.ensure_future(self.prune_sources())

        self.scheduler_thread = None

    def start(self):
        # Start the scheduler thread
        self.scheduler_thread = asyncio.ensure_future(self.run())

    def stop(self):
        self.prune_thread.cancel()

        if self.scheduler_thread:
            self.scheduler_thread.cancel()
            self.scheduler_thread = None

    async def queue_message(self, message):
        async with self.mutex:
            if message.source not in self.queue[message.priority]:
                self.queue[message.priority][message.source] = asyncio.Queue()

            await self.queue[message.priority][message.source].put(message)

        self.data_ready.set()

    async def run_next(self):
        # Iterate over the list of priorities
        for current_priority in range(len(self.queue)):
            while True:
                had_data = False
                async with self.mutex:
                    # Iterate over the source map
                    for k, v in self.queue[current_priority].items():
                        # Check if the vector for this source is empty
                        if not v.empty():
                            # Pop the next item from the queue
                            data = await v.get()

                            # Send the data
                            await self.connection.send(data.to_bytes())

                            # If there is a callback function set, call it
                            if data.callback:
                                data.callback()

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
            await self.data_ready.wait()
            self.data_ready.clear()

            await self.run_next()

    async def prune_sources(self):
        # Iterate forever
        while True:
            # Sleep until the next prune
            await asyncio.sleep(60)

            # Acquire the lock to prevent more data being pushed on while we are pruning
            async with self.mutex:
                async def _next():
                    # Iterate over the priorities
                    for p in self.queue:
                        # Iterate over the map
                        for k, v in p.items():
                            # Check if the vector for this source is empty
                            if v.empty():
                                # Remove this source from the map
                                del p[k]
                                # Start again (Changing dict sizes in an iterator is illegal)
                                await _next()
                                # Nothing more to do
                                break

                await _next()

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
