
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

#[derive(Deserialize, Serialize)]
pub struct QueueMessage {
    pub id: Uuid,
    pub body: String,
    pub created_at: SystemTime,
}