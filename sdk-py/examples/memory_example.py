from indexify import Memory, Message, DEFAULT_INDEXIFY_URL, wait_until


class DemoMemoryExample:
    def __init__(self):
        self._memory = Memory(DEFAULT_INDEXIFY_URL, "default")

    def execute(self):
        print("Running memory example...")
        # Create a memory session
        session = self._memory.create()
        # Add to the vector and persistence memory
        self._memory.add(Message("human", "Indexify is amazing!"),
                         Message("assistant", "How are you planning on using Indexify?!"))
        # Get all the memory events for a given session.
        response = self._memory.all()
        print([message.to_dict() for message in response])


if __name__ == '__main__':
    demo = DemoMemoryExample()
    demo.execute()
