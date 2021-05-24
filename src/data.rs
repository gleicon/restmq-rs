
use std::collections::{VecDeque, HashMap};
use serde::{Deserialize, Serialize};
use std::sync::{Mutex, Arc};
use tokio::sync::mpsc::Sender;
use actix_web::web::Bytes;
use uuid::Uuid;
use std::time::SystemTime;


pub struct QueueManager {
    pub index: HashMap<String, Arc<Mutex<VecDeque<String>>>>,
    pub subscribers: HashMap<String, Arc<Mutex<Vec<Sender<Bytes>>>>>
}

impl QueueManager {
    fn new_queue(mut self: Self, queue_name: String) {
        self.index.insert(queue_name.clone(),Arc::new(Mutex::new(VecDeque::new())));
        self.subscribers.insert(queue_name,Arc::new(Mutex::new(Vec::new())));

    }
    fn queue_exists(self: Self, queue_name: String) -> bool {
        return self.index.contains_key(&queue_name)
    }

    fn create_queue_if_not_exists_and_push_message(mut self: Self, queue_name: String, message: String) -> Result<String, String> {
        let queue = self.index.get(&queue_name);

        match queue { 
            Some(vect) => match vect.lock() { 
                Ok(mut v) =>  v.push_back(message.clone()),
                Err(e) => return Err(format!("{}", e)),
            }, 
            None =>  { // can't find  queue
                match self.index.insert(queue_name.clone(), Arc::new(Mutex::new(VecDeque::new()))) {
                    Some(qq) => qq.lock().unwrap().push_back(message.clone()), 
                    None => return Err(format!("Error creating queue <{}>", queue_name.clone())),
                }
            },
        }
        self.publish_to_subscribers(queue_name, message.clone());
        return Ok(message)
    }

    fn publish_to_subscribers(self: Self, queue_name: String, message: String) {
        match self.subscribers.get(&queue_name) {
            Some(subs) => {
                for subscriber in subs.lock().unwrap().iter() {
                    let _ = subscriber.try_send(Bytes::from(message.clone() + "\n\n"));
                } 
            },
            None => ()
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct QueueMessage {
    pub id: Uuid,
    pub body: String,
    pub created_at: SystemTime,
}