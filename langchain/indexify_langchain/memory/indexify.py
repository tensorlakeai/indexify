from typing import Any, Dict, List
from langchain.memory.chat_memory import BaseChatMemory

from indexify.data_containers import Message
from indexify.memory import Memory

'''
This class will initialize the Indexify class with the indexify_url your installation

Example:
memory = IndexifyMemory(indexify_url="http://10.0.0.1:8900/")
'''


class IndexifyMemory(BaseChatMemory):
    human_prefix: str = "Human"
    ai_prefix: str = "AI"
    memory_key: str = "history"
    indexify_url: str = "http://localhost:8900"
    indexify_index_name: str = "default"
    memory: Memory = None
    init: bool = False

    def __init__(self,
                 human_prefix: str = "Human",
                 ai_prefix: str = "AI",
                 memory_key: str = "history",
                 indexify_url: str = "http://localhost:8900",
                 indexify_index_name: str = "default",
                 **kwargs: Any):
        super().__init__(**kwargs)
        self.human_prefix = human_prefix
        self.ai_prefix = ai_prefix
        self.memory_key = memory_key
        self.indexify_url = indexify_url
        self.indexify_index_name = indexify_index_name
        self.memory: Memory = Memory(self.indexify_url, self.indexify_index_name)

    @property
    def memory_variables(self) -> List[str]:
        return [self.memory_key]

    def save_context(self, inputs: Dict[str, Any], outputs: Dict[str, str]) -> None:
        self.may_init()
        input_str, output_str = self._get_input_output(inputs, outputs)
        self.memory.add(Message(self.human_prefix, input_str))
        self.memory.add(Message(self.ai_prefix, output_str))

    def load_memory_variables(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        self.may_init()
        all_messages = ""
        for message in self.memory.all():
            all_messages += f"{message.role}: {message.text}\n"
        return {self.memory_key: all_messages}

    def clear(self) -> None:
        # Recreate the memory
        self.memory.create()

    def may_init(self) -> None:
        if not self.init:
            self.memory.create()
            self.init = True
