use std::collections::HashMap;

struct ListNode {
    key: u64,
    value: String,
    prev: Option<Arc<ListNode>>,
    next: Option<Arc<ListNode>>,
}

struct LRUCache {
    capacity: usize,
    map: HashMap<u64, Arc<ListNode>>,
    head: Option<Arc<ListNode>>,
    tail: Option<Arc<ListNode>>,
}

impl LRUCache {
    fn new(capacity: usize) -> Self {
        LRUCache {
            capacity,
            map: HashMap::new(),
            head: None,
            tail: None,
        }
    }

    fn get(&mut self, key: u64) -> Option<&str> {
        if let Some(node) = self.map.get(&key) {
            self.update_usage(node);
            Some(&node.value)
        } else {
            None
        }
    }

    fn put(&mut self, key: u64, value: String) {
        if let Some(node) = self.map.get_mut(&key) {
            node.value = value;
            self.update_usage(node);
        } else {
            let new_node = Arc::new(ListNode {
                key,
                value,
                prev: None,
                next: self.head.take(),
            });

            if let Some(node) = new_node.next.as_mut() {
                node.prev = Some(new_node.clone());
            } else {
                self.tail = Some(new_node.clone());
            }

            self.head = Some(new_node);
            self.map.insert(key, self.head.as_mut().unwrap().clone());

            if self.map.len() > self.capacity {
                if let Some(node) = self.tail.take() {
                    self.map.remove(&node.key);
                    if let Some(prev) = node.prev {
                        self.tail = Some(prev);
                        self.tail.as_mut().unwrap().next = None;
                    }
                }
            }
        }
    }

    fn update_usage(&mut self, node: &mut Arc<ListNode>) {
        if let Some(prev) = node.prev.take() {
            prev.next = node.next.take();
            if let Some(next) = prev.next.as_mut() {
                next.prev = Some(prev.clone());
            } else {
                self.tail = Some(prev.clone());
            }
            node.next = self.head.take();
            if let Some(head) = node.next.as_mut() {
                head.prev = Some(node.clone());
            } else {
                self.tail = Some(node.clone());
            }
            self.head = Some(node.clone());
            self.map.insert(node.key, node.clone());
        }
    }
}

fn main() {
    let mut cache = LRUCache::new(2);
    cache.put(1, "Value 1".to_string());
    cache.put(2, "Value 2".to_string());

    // Retrieves the cached values
    println!("{:?}", cache.get(1)); // Some("Value 1")
    println!("{:?}", cache.get(2)); // Some("Value 2")

    cache.put(3, "Value 3".to_string());

    // Accessing key 1 pushes key 2 out of the cache due to limited capacity
    println!("{:?}", cache.get(1)); // Some("Value 1")
    println!("{:?}", cache.get(2)); // None

    cache.put(4, "Value 4".to_string());

    // Accessing key 1 and key 3 keeps them in the cache, while key 4 replaces key 1
    println!("{:?}", cache.get(1)); // None
    println!("{:?}", cache.get(3)); // Some
}
