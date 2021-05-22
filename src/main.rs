use std::{collections::HashMap};
use std::collections::VecDeque;
use std::sync::{Mutex, Arc};
use std::time::SystemTime;
use std::pin::Pin;
use std::task::{Context, Poll};
use uuid::Uuid;
use futures::{Stream};

use actix_web::{get, post, web, App, HttpServer, Result, middleware, HttpResponse, Error};
use actix_web::web::{Bytes};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[derive(Deserialize)]
struct QueueInfo {
    queuename: String,
}

struct QueueManager {
    index: HashMap<String, Arc<Mutex<VecDeque<String>>>>,
    subscribers: HashMap<String, Arc<Mutex<Vec<Sender<Bytes>>>>>

}

struct Subscriber(Receiver<Bytes>);

impl Stream for Subscriber {
    type Item = Result<Bytes, Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>, ) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.0).poll_recv(cx) {
            Poll::Ready(Some(v)) => Poll::Ready(Some(Ok(v))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Deserialize, Serialize)]
struct QueueMessage {
    id: Uuid,
    body: String,
    created_at: SystemTime,
}

type QueueStatus = (String, usize); // Queue Name and length

#[get("/")]
async fn list_queues(data: web::Data<Mutex<QueueManager>>) ->  Result<HttpResponse, Error> { 
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
async fn queue_read(info: web::Path<QueueInfo>, data: web::Data<Mutex<QueueManager>>) -> Result<HttpResponse, Error> { 
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
async fn queue_write(req_body: String, info: web::Path<QueueInfo>, data: web::Data<Mutex<QueueManager>>) -> Result<HttpResponse, Error>  {

    let mut data = data.lock().unwrap();
    let queue = data.index.get(&info.queuename);
//    let subscribers = data.subscribers.get(&info.queuename);

    let uuid = Uuid::new_v4();
    let payload = QueueMessage {id: uuid, body: req_body.clone(), created_at: SystemTime::now()};
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
async fn queue_streaming(info: web::Path<QueueInfo>, data: web::Data<Mutex<QueueManager>>) -> HttpResponse { 
    let (tx, rx) = channel(100);
    let mut data = data.lock().unwrap();
    let subscribers = data.subscribers.get(&info.queuename);

    match subscribers {
        Some(subs) => match subs.lock() {
            Ok(mut v) => v.push(tx),
            Err(e) => ()//Ok(HttpResponse::BadRequest().content_type("application/json").body(format!("msg: err {:?}", e)))
        }, 
        None => {
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
   
    HttpResponse::Ok().streaming(Subscriber(rx))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let data = web::Data::new(Mutex::new(QueueManager{ 
        index: HashMap::new(), 
        subscribers: HashMap::new() }));
    HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .app_data(data.clone())
            .service(queue_read)
            .service(queue_write)
            .service(list_queues)
            .service(queue_streaming)
    }).bind("127.0.0.1:8080")?.run().await
}
