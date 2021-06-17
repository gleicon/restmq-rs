
use std::sync::Mutex;
use actix_web::{web, App, HttpServer, middleware};
#[macro_use]
extern crate log;

mod handlers;
mod queue;
mod subscriber;
mod persistence;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    info!("Starting RESTMQ");
    let data = web::Data::new(Mutex::new(queue::QueueManager::new("temp_databases".to_string())));
    
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
