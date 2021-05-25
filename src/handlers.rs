use std::collections::VecDeque;
use std::sync::{Mutex, Arc};
use std::time::SystemTime;
use uuid::Uuid;

use actix_web::{get, post, web, Result, HttpResponse, Error};
use actix_web::web::{Bytes};
use serde::Deserialize;
use tokio::sync::mpsc::channel;



type QueueStatus = (String, usize); // Queue Name and length

#[derive(Deserialize, Clone)]
struct QueueInfo {
    queuename: String,
}


#[get("/")]
async fn list_queues(data: web::Data<Mutex<crate::queue::QueueManager>>) ->  Result<HttpResponse, Error> { 
    let data = data.lock().unwrap().clone();
    let lq = data.queue_status();
    return Ok(HttpResponse::Ok().content_type("application/json").body(lq))
}

#[get("/q/{queuename}")]
async fn queue_read(info: web::Path<QueueInfo>, data: web::Data<Mutex<crate::queue::QueueManager>>) -> Result<HttpResponse, Error> { 
    let data = data.lock().unwrap().clone();

    match data.queue_retrieve(info.queuename.clone()) {
        Ok(m) => return Ok(HttpResponse::Ok().content_type("application/json").body(m)),
        Err(e) => return Ok(HttpResponse::BadRequest().content_type("application/json").body(format!("msg: err fetching message from topic {:?} -  {:?}", info.queuename, e))),
    };
}

#[post("/q/{queuename}")]
async fn queue_write(req_body: String, info: web::Path<QueueInfo>, data: web::Data<Mutex<crate::queue::QueueManager>>) -> Result<HttpResponse, Error>  {
    
    let mut data = data.lock().unwrap();
    let msg = req_body.clone();

    match data.push_message(info.queuename.clone(), msg.clone(), true) {
        Ok(m) => return Ok(HttpResponse::Ok().content_type("application/json").body(format!("message {} sent to topic {}", m, info.queuename))),
        Err(e) => return Ok(HttpResponse::BadRequest().content_type("application/json").body(format!("msg: err {:?}", e))),
    }

}

#[get("/c/{queuename}")]
async fn queue_streaming(info: web::Path<QueueInfo>, data: web::Data<Mutex<crate::queue::QueueManager>>) -> HttpResponse { 
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


