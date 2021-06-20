use std::sync::Mutex;
use actix_web::{get, post, web, Result, HttpResponse, Error};
use serde::Deserialize;

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
    let mut data = data.lock().unwrap().clone();

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
async fn queue_streaming(info: web::Path<QueueInfo>, data: web::Data<Mutex<crate::queue::QueueManager>>) ->  Result<HttpResponse, Error>  { 

    let mut data = data.lock().unwrap();
    match data.append_subscriber(info.queuename.clone()) {
        Ok(sb) =>  return Ok(HttpResponse::Ok().content_type("application/json").streaming(sb)),
        Err(_e) =>  return Ok(HttpResponse::BadRequest().content_type("application/json").body(format!("msg: error subscribing {:?}", &info.queuename))),
    };
}


