
use std::collections::{VecDeque, HashMap};
use serde::{Deserialize, Serialize};
use std::sync::{Mutex, Arc};
use tokio::sync::mpsc::Sender;
use actix_web::web::Bytes;
use uuid::Uuid;
use std::time::SystemTime;


#[derive(Deserialize, Serialize)]
pub struct QueueMessageEnvelope {
    pub id: Uuid,
    pub body: String,
    pub created_at: SystemTime,
}

#[derive(Clone)]
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

    pub fn push_message(&mut self, queue_name: String, message: String, create_queue: bool) -> Result<String, String> {

        let uuid = Uuid::new_v4();
        let payload = QueueMessageEnvelope {id: uuid, body: message, created_at: SystemTime::now()};
        let json_payload = serde_json::to_value(&payload);
        let msg = json_payload.unwrap().to_string();


        if !self.index.contains_key(&queue_name) && create_queue {
            self.index.insert(queue_name.clone(), Arc::new(Mutex::new(VecDeque::new())));
        } else if !self.index.contains_key(&queue_name) {
            return Err(format!("Queue <{}> does not exists", queue_name.clone()));
        }

        match self.index.get(&queue_name).unwrap().lock() {
            Ok(mut vect) =>  {  
                vect.push_back(msg.clone());
             },
            Err(e) => return Err(format!("Error queue <{}>: {}", queue_name.clone(), e)),
        }       

        self.publish_to_subscribers(queue_name.clone(), msg.clone());
        return Ok(msg)
    }

    pub fn publish_to_subscribers(&mut self, queue_name: String, message: String) {
        match self.subscribers.get(&queue_name) {
            Some(subs) => {
                for subscriber in subs.lock().unwrap().iter() {
                    let _ = subscriber.try_send(Bytes::from(message.clone() + "\n\n"));
                } 
            },
            None => ()
        }
    }

    fn count_subscribers(&mut self, queue_name: String) -> usize {
        let sbs = self.subscribers.get(&queue_name).unwrap().lock().unwrap().len();
        return sbs.clone()
    }

    pub fn queue_status(self: Self) -> String {
        type QS = (String, usize, usize); // Queue Name, length, subscribers

        let mut queue_status_vec : Vec<QS>;
        
        queue_status_vec = Vec::new();

        for (k,v) in self.index.iter() {
            let ll = v.lock().unwrap().len();
            let sl = self.subscribers.get(&k.to_string()).unwrap().lock().unwrap().len();
            let qm: QS = (k.clone(), ll, sl);
            queue_status_vec.push(qm);
        };
    let payload_all_queues = serde_json::to_value(queue_status_vec);
    return payload_all_queues.unwrap().to_string()
    }

    pub fn queue_retrieve(self: Self, queue_name: String) -> Result<String, String> {
        let queue = self.index.get(&queue_name);
    
        match queue {
            Some(vect) => match vect.lock() { 
                Ok(mut v) => { 
                    match v.pop_front() {
                        Some(payload) => { 
                            print!("-> {}", payload);
                            return Ok(payload)
                        },
                        
                        None => return Err("[]".to_string()),
                    }
                }, 
                Err(e) => return Err(format!("msg: err fetching message from topic {:?} -  {:?}", queue_name.clone(), e)),
            }, 
            None => return Ok(format!("msg: queue <{:?}> not found", queue_name.clone())),
        }
    }

}
