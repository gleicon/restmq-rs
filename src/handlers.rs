use std::collections::VecDeque;
use std::sync::{Mutex, Arc};
use std::time::SystemTime;
use uuid::Uuid;

use actix_web::{get, post, web, Result, HttpResponse, Error};
use actix_web::web::{Bytes};
use serde::Deserialize;
use tokio::sync::mpsc::channel;



type QueueStatus = (String, usize); // Queue Name and length

#[derive(Deserialize)]
struct QueueInfo {
    queuename: String,
}


#[get("/")]
async fn list_queues(data: web::Data<Mutex<crate::data::QueueManager>>) ->  Result<HttpResponse, Error> { 
    let data = data.lock().unwrap(); 
    let mut queue_status_vec : Vec<QueueStatus>;
    queue_status_vec = Vec::new();
    for (k,v) in data.index.iter() {
        let ll = v.lock().unwrap().len();
        let  qm: QueueStatus = (k.clone(), ll);
        queue_status_vec.push(qm);
    };
    let payload_all_queues = serde_json::to_value(queue_status_vec);
    return Ok(HttpResponse::Ok().content_type("application/json").body(payload_all_queues.unwrap().to_string()))
}

#[get("/q/{queuename}")]
async fn queue_read(info: web::Path<QueueInfo>, data: web::Data<Mutex<crate::data::QueueManager>>) -> Result<HttpResponse, Error> { 
    let data = data.lock().unwrap(); 
    let queue = data.index.get(&info.queuename);
    
    match queue {
        Some(vect) => match vect.lock() { 
            Ok(mut v) => { 
                match v.pop_front() {
                    Some(payload) => return Ok(HttpResponse::Ok().content_type("application/json").body(payload)),
                    None => return Ok(HttpResponse::NoContent().content_type("application/json").body("")),
                }
                
            }, 
            Err(e) => return Ok(HttpResponse::BadRequest().content_type("application/json").body(format!("msg: err fetching message from topic {:?} -  {:?}", info.queuename, e))),
        }, 
        None => return Ok(HttpResponse::NotFound().content_type("application/json").body(format!("msg: queue [{:?}] not found", info.queuename))),
    }
}

#[post("/q/{queuename}")]
async fn queue_write(req_body: String, info: web::Path<QueueInfo>, data: web::Data<Mutex<crate::data::QueueManager>>) -> Result<HttpResponse, Error>  {
    
    let mut data = data.lock().unwrap();
    let queue = data.index.get(&info.queuename);
    
    let uuid = Uuid::new_v4();
    let payload = crate::data::QueueMessage {id: uuid, body: req_body.clone(), created_at: SystemTime::now()};
    let json_payload = serde_json::to_value(&payload);
    let msg = json_payload.unwrap().to_string();
    
    match queue {
        Some(vect) => match vect.lock() { 
            Ok(mut v) =>  v.push_back(msg.clone()),
            Err(e) => return Ok(HttpResponse::BadRequest().content_type("application/json").body(format!("msg: err {:?}", e))),
        }, 
        None =>  { 
            match data.index.insert(info.queuename.clone(),Arc::new(Mutex::new(VecDeque::new()))) {
                Some(qq) => qq.lock().unwrap().push_back(msg.clone()), 
                None => (),
            }
            match data.index.get(&info.queuename) { 
                Some(vl) => vl.lock().unwrap().push_back(msg.clone()),
                None => (),
            }
        },
    }
    
    match data.subscribers.get(&info.queuename) {
        Some(subs) => {
            for subscriber in subs.lock().unwrap().iter() {
                let _ = subscriber.try_send(Bytes::from(msg.clone() + "\n\n"));
            } 
        },
        None => ()
    }
    
    Ok(HttpResponse::Ok().content_type("application/json").body(format!("message sent {} to topic {}", req_body, info.queuename)))
}

#[get("/c/{queuename}")]
async fn queue_streaming(info: web::Path<QueueInfo>, data: web::Data<Mutex<crate::data::QueueManager>>) -> HttpResponse { 
    let (tx, rx) = channel(100);
    let mut data = data.lock().unwrap();
    let subscribers = data.subscribers.get(&info.queuename);


    match subscribers {
        
        Some(subs) => match subs.lock() {
            Ok(mut v) => v.push(tx),
            Err(e) => return HttpResponse::BadRequest().content_type("application/json").body(format!("msg: err {:?}", e)) // wrap error message
        }, 
        None => {// if the key is not present in the hashmap, create it and insert the subscriber Receive half
            match data.subscribers.insert(info.queuename.clone(),Arc::new(Mutex::new(Vec::new()))) {
                Some(qq) => qq.lock().unwrap().push(tx.clone()), 
                None => (),
            }
            match data.subscribers.get(&info.queuename) { 
                Some(vl) => vl.lock().unwrap().push(tx.clone()),
                None => (),
            }
        }
    }
    
    HttpResponse::Ok().streaming(crate::subscriber::SubscriberChannel(rx))
}


