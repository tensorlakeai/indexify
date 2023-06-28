from indexify import Memory, Message


class DemoSimpleApplication:
    def __init__(self):
        self.idx = Memory(Memory.DEFAULT_INDEXIFY_URL, "default/default")

    def execute(self):
        # Create a memory session
        session = self.idx.create_memory()
        # Add to the vector and persistence memory
        self.idx.add(Message("human", "Indexify is amazing!"),
                     Message("assistant", "How are you planning on using Indexify?!"))
        # Get all the memory events for a given session.
        response = self.idx.all()
        print([message.to_dict() for message in response])


if __name__ == '__main__':
    demo = DemoSimpleApplication()
    demo.execute()
