use std::{fs::File, io::{Write, Seek, Read}, os::unix::prelude::FileExt};
use std::io::{self};

use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;

const MAX_NODE_SIZE:usize = 3; // should be an odd number 
const DATABASE_FILE_NAME: &str = "database.txt";
const ROOT_REFERENCE_FILE_NAME: &str = "root.txt";

#[derive(Clone, Debug)]
enum BTreeException {
    ElementNotFound
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ValueReference {
    file_name: String,
    starting_offset: u64,
    bytes_length: usize
}

impl ValueReference {
    fn get_value(&self) -> String {
        let f = File::open(&self.file_name).unwrap();
        let mut serialized_node: Vec<u8> = Vec::new();
        let mut reading_point = self.starting_offset;
        while serialized_node.len() < self.bytes_length {
            let mut buf = [0u8; 32];
            let p = f.read_at(&mut buf, reading_point).unwrap();
            reading_point = reading_point + (p as u64);            
            for v in buf.iter() {
                if serialized_node.len() < self.bytes_length { 
                    serialized_node.push(v.clone());
                } else {
                    break;
                }
            }
        }

        let s = String::from_utf8(serialized_node).unwrap();
        return s
    }

    fn new(value: String) -> ValueReference {
        let file_name = DATABASE_FILE_NAME;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(file_name)
            .unwrap();

        file.write(value.as_bytes()).expect("Writing to file failed");
        
        let file_size = file.stream_position().unwrap();
        // println!("The file size is currently: {}", file_size);
        file.write(String::from("\n").as_bytes()).expect("Writing to file failed");

        let bytes_length = value.bytes().len();
        return ValueReference {
            file_name: file_name.to_string(),
            starting_offset: file_size - (bytes_length as u64),
            bytes_length: bytes_length
        }
    }
}



#[derive(Clone, Debug, Serialize, Deserialize)]
struct NodeEntry {
    key: i32,
    value_reference: ValueReference
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NodeReference {
    file_name: String,
    starting_offset: u64,
    bytes_length: usize
}

// #[derive(Clone, Debug, Serialize, Deserialize)]
// struct NodeReference2 {
//     content: Node
// }
// impl NodeReference2 {
//     fn get_value(&self) -> Node {
//         return Node { 
//             node_entries:self.content.node_entries.clone(), 
//             children: self.content.children.clone() 
//         }
//     }

//     fn new() -> NodeReference2 {
//         NodeReference2 {
//             content: Node {
//                 node_entries: Vec::new(),
//                 children: Vec::new()
//             }
//         }
//     }

//     fn of(node: Node) -> NodeReference2 {
//         NodeReference2 {
//             content: node
//         }
//     }
// }

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Node {
    node_entries: Vec<NodeEntry>,
    children: Vec<Option<NodeReference>>
}

#[derive(Clone, Debug)]
struct SplittedNode {
    top: NodeEntry,
    left_child: NodeReference,
    right_child: NodeReference
}

impl Node {
    fn is_full(&self) -> bool {
        return self.node_entries.len() == MAX_NODE_SIZE;
    }

    fn is_empty(&self) -> bool {
        return self.node_entries.is_empty();
    }

    fn is_leaf(&self) -> bool {
        return self.children.is_empty();
    }

    fn insert_entry(&mut self, key: i32, value: String)-> std::result::Result<Node, BTreeException> {
        if self.is_leaf() {
            // println!("leaf");
            let previous_key = -1;
            let mut index_to_insert = self.node_entries.len();
            
            for (index, val) in self.node_entries.iter().enumerate() {
                if key == val.key {
                    let mut new_node = self.clone();
                    let new_node_entry = NodeEntry {
                        key: val.key,
                        value_reference: ValueReference::new(value)
                    };
                    new_node.node_entries[index] = new_node_entry.clone();
                    return Ok(new_node)
                }

                if previous_key < key && key < val.key {
                    index_to_insert = index;
                    break;
                }
            }
            // println!("adding at {}", index_to_insert);
            let new_entry = NodeEntry{
                key: key,
                value_reference: ValueReference::new(value)
            };
            self.node_entries.insert(index_to_insert, new_entry);
            if self.is_full() {
                // println!("to split");
                let splitted_child = self.split_me();

                self.node_entries.clear();
                self.children.clear();
                
                self.node_entries.push(splitted_child.top);
                self.children.push(Some(splitted_child.left_child));
                self.children.push(Some(splitted_child.right_child));
            }

            // println!("Inserted into node: {:?}", self);
            return Ok(self.clone());
        } else {
            while self.children.len() < self.node_entries.len()+1 {
                self.children.push(Option::None);
            }

            let previous_key = -1;
            let mut index_to_insert = self.node_entries.len();
            for (index, val) in self.node_entries.iter().enumerate() {     
                if key == val.key {
                    let mut new_node = self.clone();
                    let new_node_entry = NodeEntry {
                        key: val.key,
                        value_reference: ValueReference::new(value)
                    };
                    new_node.node_entries[index] = new_node_entry.clone();
                    return Ok(new_node)
                }
           
                if previous_key < key && key < val.key {
                    index_to_insert = index;
                }
            }
    
            let child = self.children.get(index_to_insert).unwrap();
            if let &Option::None = &child {
                let new_entry_reference = NodeReference::new();
                self.children[index_to_insert] = Some(new_entry_reference);
            }

            let mut child = self.children.get(index_to_insert)
                .unwrap()
                .as_ref()
                .unwrap()
                .get_value();

            if child.is_full() {
                // println!("to split");
                let splitted_child = child.split_me();
                self.node_entries.insert(index_to_insert, splitted_child.top);
                self.children.insert(index_to_insert, Some(splitted_child.left_child));
                self.children[index_to_insert+1] = Some(splitted_child.right_child);
            }
            
            let p = &child.insert_entry(key, value);
            match p {
                std::result::Result::Ok(child_node)=> {
                    let child_ref = NodeReference::of(child_node.clone());
                    self.children[index_to_insert] = Some(child_ref);
                    return Ok(self.clone())
                },
                std::result::Result::Err(x) => Err(x.clone())
            }
        }
    }

    fn get_entry(&self, key: i32)-> std::result::Result<String, BTreeException> {
        if self.is_empty() {
            return Err(BTreeException::ElementNotFound)
        }
        
        let previous_key = -1;
        let mut child_index = self.node_entries.len();
        for (index, val) in self.node_entries.iter().enumerate() {
            // println!("looking at value {:?} on index {}", val, index);
            if val.key == key {
                return Ok(val.value_reference.get_value());
            }
            if previous_key < key && key < val.key {
                child_index = index;
            }
        }
        let child = self.children.get(child_index);
        return match &child {
            &Option::None => Err(BTreeException::ElementNotFound),
            &Option::Some(x) => { 
                match x {
                    None => Err(BTreeException::ElementNotFound),
                    Some(val) => val.get_value().get_entry(key) 
                }
            }
        }
    }

    fn split_me(&self) -> SplittedNode {
        let mut left_side: Node = Node { node_entries: Vec::new(), children: Vec::new() };
        let mut right_side: Node = Node { node_entries: Vec::new(), children: Vec::new() };
        
        let mut i = 0;


        let first_child = match self.children.get(0) {
            None => None,
            Some(x) => match x {
                None => None,
                Some(xx) => Some(xx.clone())
            }
        };
        left_side.children.push(first_child);
        while i < self.node_entries.len()/2 {
            left_side.node_entries.push(self.node_entries.get(i).unwrap().clone());
            let child = match self.children.get(i+1) {
                None => None,
                Some(x) => match x {
                    None => None,
                    Some(xx) => Some(xx.clone())
                }
            };
            left_side.children.push(child);
            i = i+1;
        }
        
        let top = self.node_entries.get(i).unwrap();
        i = i+1;

        let first_child = match self.children.get(i) {
            None => None,
            Some(x) => match x {
                None => None,
                Some(xx) => Some(xx.clone())
            }
        };
        right_side.children.push(first_child);
        while i < self.node_entries.len() {
            right_side.node_entries.push(self.node_entries.get(i).unwrap().clone());
            let child = match self.children.get(i+1) {
                None => None,
                Some(x) => match x {
                    None => None,
                    Some(xx) => Some(xx.clone())
                }
            };
            right_side.children.push(child);
            i = i+1;
        }

        let res = SplittedNode {
            top: top.clone(),
            left_child: NodeReference::of(left_side),
            right_child: NodeReference::of(right_side)
        };
        // println!("\n\nSplitted Node {:?}\n\n", &res);
        return res;
    }
}

impl NodeReference {
    fn get_value(&self) -> Node {
        let f = File::open(&self.file_name).unwrap();
        let mut serialized_node: Vec<u8> = Vec::new();
        let mut reading_point = self.starting_offset;
        while serialized_node.len() < self.bytes_length {
            let mut buf = [0u8; 32];
            let p = f.read_at(&mut buf, reading_point).unwrap();
            reading_point = reading_point + (p as u64);            
            for v in buf.iter() {
                if serialized_node.len() < self.bytes_length { 
                    serialized_node.push(v.clone());
                } else {
                    break;
                }
            }
        }

        let s = String::from_utf8(serialized_node).unwrap();
        let j: Node = serde_json::from_str(s.as_str()).unwrap();
        return j
    }

    fn new() -> NodeReference {
        let node = Node {
            node_entries: Vec::new(),
            children: Vec::new()
        };        
        return Self::of(node);
    }

    fn of(node: Node) -> NodeReference {
        let file_name = DATABASE_FILE_NAME;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(file_name)
            .unwrap();

        let j = serde_json::to_string(&node).unwrap();
        file.write(j.as_bytes()).expect("Writing to file failed");
        
        let file_size = file.stream_position().unwrap();
        // println!("The file size is currently: {}", file_size);
        file.write(String::from("\n").as_bytes()).expect("Writing to file failed");

        let bytes_length = j.bytes().len();
        return NodeReference {
            file_name: file_name.to_string(),
            starting_offset: file_size - (bytes_length as u64),
            bytes_length: bytes_length
        }
    }
}

struct BTree {
    root: NodeReference,
    is_in_transaction: bool
}

impl BTree {
    fn init() -> BTree { // starting from where we left off
        let file_name = ROOT_REFERENCE_FILE_NAME;
        let file_result = File::open(file_name);
        let mut file;
        
        match file_result {
            Ok(c) => {
                file = c;
            },
            Err(_) => {
                return Self::new();
            }
        }

        // println!("Reading the root from file");
        let mut buffer = String::new();
        file.read_to_string(&mut buffer).expect("Could not read the root reference");

        // println!("The buffer is: {}", buffer);

        let root_reference: NodeReference = serde_json::from_str(&buffer.as_str()).unwrap();
        return BTree {
            root: root_reference,
            is_in_transaction: false
        };
    }

    fn get(&self, key: i32)->std::result::Result<String, BTreeException> {
        return self.root.get_value().get_entry(key);
    }
    
    fn set(&mut self, key: i32, value: String)-> Option<BTreeException> {
        let k = self.root.get_value().insert_entry(key, value);
        match k {
            std::result::Result::Ok(c) => {
                self.root = NodeReference::of(c);
                if !self.is_in_transaction {
                    self.persist_to_disk();
                }
                return None;
            },
            std::result::Result::Err(x) => Some(x)
        }
    }

    fn begin_transaction(&mut self) {
        self.is_in_transaction = true;
    }

    fn end_transaction(&mut self) {
        self.is_in_transaction = false;
        self.persist_to_disk();
    }
    fn rollback_transaction(&mut self) {
        self.is_in_transaction = false;
        *self = Self::init();
    }

    fn clear(&mut self) {
        self.root = NodeReference::new();
        if !self.is_in_transaction {
            self.persist_to_disk();
        }
    }

    fn new() -> BTree { // creating a new, empty database
        let root= NodeReference::new();
        let tree = BTree {
            root: root,
            is_in_transaction: false
        };
        // tree.persist_to_disk();
        return tree;
    }

    fn persist_to_disk(&self) {
        let file_name = ROOT_REFERENCE_FILE_NAME;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(file_name)
            .unwrap();
        file.set_len(0).expect("File cleaning failed");

        let j = serde_json::to_string(&self.root).unwrap();
        file.write(j.as_bytes()).expect("Writing to file failed");
    }

    fn print(&self, range_start: i32, range_end: i32) {
        println!("DB content");
        let mut i = range_start;
        while i <= range_end {
            println!("{}: {:?}", i, &self.get(i));
            i = i+1;
        }
        println!("");    
    }
}

fn main() {
    let mut tree = BTree::init();
    loop {    
        let mut buffer = String::new();
        io::stdin().read_line(&mut buffer).expect("Coud not read stdin");
        let commands: Vec<_> = buffer.split_whitespace().collect();
        if commands.is_empty() {
            continue;
        }

        let first_command = *commands.first().unwrap();
        if first_command == "exit" {
            break;
        } else if first_command == "set" {
            if commands.len() < 3 {
                println!("Not enough arguments");
                continue;
            }
            let key: i32 = commands.get(1).unwrap().parse::<i32>().unwrap();
            let value : String = commands.get(2).unwrap().to_string();
            tree.set(key, value.clone());
            println!("Setting value, associated with key {} to {:?}", key, &value);
        } else if first_command == "get" {
            if commands.len() < 2 {
                println!("Not enough arguments");
                continue;
            }
            let key: i32 = commands.get(1).unwrap().parse::<i32>().unwrap();
            println!("The value is {:?}", tree.get(key));
        } else if first_command == "clear" {
            tree.clear();
            println!("Cleaning database");
        } else if first_command == "print" {
            tree.print(0, 10);
        } else if first_command == "begin_transaction" {
            tree.begin_transaction();
        } else if first_command == "end_transaction" {
            tree.end_transaction();
        } else if first_command == "rollback" {
            tree.rollback_transaction();
        } else {
            println!("Unknown command");
        }
    }

}
