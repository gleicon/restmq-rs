
use sled::{Config};
//use std::result::Result;
//use std::error::Error;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;
use serde::{Serialize, Deserialize};
use chrono::Local;



// persistence manager for restmq
// one database per queue, initialized in the same directory 
// pros: isolation
// cons: disk space
// Vec<u8> in and out
// keys are incremental ids
// value bincode serde

// create queue/database
// on start: read all database handlers
// TODO: on key change: watch_prefix per table w/ watcher registration
// TODO: stats: disk space, queue number, queue sizes
// TODO: bound topics/tables (expire/delete old messages)


#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct Envelope {
    body: String, //Vec<u8>,
}

#[derive(Clone)]
pub struct PersistenceManager{
    root_path: String,
    path: PathBuf,
    databases: HashMap<String, sled::Db>,
}

#[derive(Clone)]
pub struct KVTimestamp {
    pub key: Vec<u8>,
    pub old_value: String,
    pub value: String,
    pub timestamp: i64,
}

impl PersistenceManager {

    pub fn push_item(&mut self, queue_name: String, body: String) -> Result<KVTimestamp, String> {
        let mut db = self.databases.get(&queue_name.clone());
        match db.clone() {
            Some(_) => (),
            None => {
                self.load_or_create_database(queue_name.clone()).unwrap();
                db = self.databases.get(&queue_name);
            },
        }

        let db = db.unwrap();
        let key = format!("{}:{}", queue_name, db.generate_id().unwrap());
        let bbody = Envelope {body: body.clone()};
        let encoded: Vec<u8> = bincode::serialize(&bbody).unwrap();

        let res = db.insert(key.clone(), encoded.clone());

        match res {
            Ok(Some(b)) => { // value existed
                let dt = Local::now();
                let v: Option<String> = bincode::deserialize(&b.to_vec()).unwrap();
                let ev = KVTimestamp {key: key.as_bytes().to_vec(), old_value: v.unwrap(), value: body.clone(), timestamp: dt.timestamp_millis()};
                Ok(ev)
            },
            Ok(None) => { // new value
                let dt = Local::now();
                let ev = KVTimestamp {key: key.as_bytes().to_vec(), old_value: body.clone(), value: body.clone(), timestamp: dt.timestamp_millis()};
                Ok(ev)
            },
            Err(e) => Err(format!("Error persisting message: {}", e)),
        }
    }

    pub fn pop_item(&mut self, queue_name: String) -> Result<Box<Vec<u8>>, String> {
        let db = self.databases.get(&queue_name.clone());

        // fetch or create the db handler
        let dbc = match db.clone() {
            Some(_) => (db),
            None => {
                self.load_or_create_database(queue_name.clone()).unwrap();
                self.databases.get(&queue_name)
            },
        };

        let dbc = dbc.unwrap();

        match dbc.pop_max().unwrap() {
            Some((_, value)) => Ok(Box::new(bincode::deserialize(value.as_ref()).unwrap())),
            None => Err("Empty queue table".to_string()),
        }

    }

    pub fn load_or_create_database(&mut self, queue_name: String) -> Result<bool, String> {
        let mut pb = PathBuf::new();
        pb.push(&self.path);
        pb.push(queue_name.clone());
        let config = Config::new().path(pb);
        match config.open() {
            Ok(d) => { 
                self.databases.insert(queue_name, d); 
                Ok(true)
            },
            Err(e) => {
                Err(format!("Error opening database: {:?}", e))
             } 
        }
    }

    pub fn setup(&mut self) {
        if !Path::new(&self.root_path).exists() {
            fs::create_dir_all(&self.root_path).unwrap();
        }
        
        self.load_persistence(); 
    }

    fn load_persistence(&mut self) {
        let dir = &self.path;
            if dir.is_dir() {
                for entry in fs::read_dir(dir).unwrap() {
                    let path = entry.unwrap().path();
                    if path.is_dir() {
                        let queue_name = path.to_str().unwrap().to_string();
                        self.load_or_create_database(queue_name).unwrap();    
                    };
            }
        }
    }

    pub fn new (basepath:  String) -> Self {
        let bp = Path::new(&basepath);
        let mut s = Self {
            root_path: basepath.clone(),
            databases: HashMap::new(),
            path: bp.to_path_buf(),
        };
        s.setup();
        return s
    }
}
