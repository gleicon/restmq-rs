### RestMQ

A port of [RestMQ](https://github.com/gleicon/restmq) to learn Rust. 
It uses Actix, Tokio and Rust. 

#### Running

$ cargo run

#### Usage

<queuename> is a mandatory http compatible queue name.

	- [GET] http://localhost:8080/ - List all queues and stats
	- [POST] http://localhost:8080/q/<queuename> - push the body content into the <queuename> queue
	- [GET] http://localhost:8080/q/<queuename> - pop the earliest item in the queue <queuename>
	- [GET] http://localhost:8080/c/<queuename> - listen to a fan-out for <queuename>, all http clients will receive a copy of any message posted to /q/<queuename>

$ curl http://localhost:8080/c/hello

in another terminal:

$ curl -XPOST -d "world" http://localhost:8080/q/hello

![terminal](restmq-rs.gif)
