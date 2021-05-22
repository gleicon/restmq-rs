
use std::collections::HashMap;
use std::sync::Mutex;
use actix_web::{web, App, HttpServer, middleware};

mod handlers;
mod data;
mod subscriber;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let data = web::Data::new(Mutex::new(data::QueueManager{ 
        index: HashMap::new(), 
        subscribers: HashMap::new() }));
    HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .app_data(data.clone())
            .service(handlers::queue_read)
            .service(handlers::queue_write)
            .service(handlers::list_queues)
            .service(handlers::queue_streaming)
    }).bind("127.0.0.1:8080")?.run().await
}
