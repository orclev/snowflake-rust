#![feature(proc_macro)]

#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate serde;
extern crate hyper;
extern crate chrono;
extern crate base32;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate envoption;

use hyper::net::Openssl;
use hyper::server::{Server, Request, Response, Handler};
use hyper::status::StatusCode;
use std::sync::Mutex;
use base32::encode;
use base32::Alphabet::Crockford;
use serde::{Serialize, Serializer};
use chrono::*;
use std::ops::Deref;
use std::net::SocketAddr;
use std::net::IpAddr;
use std::str::FromStr;

struct SnowflakeState {
    counter: Mutex<Counter>,
    node_id: u16,
    epoch: DateTime<FixedOffset>
}

struct Counter {
    count: u16,
    updated: u64
}

struct Snowflake {
    epoch: u64,
    node_id: u16,
    sequence: u16
}

impl Snowflake {
    fn as_u64(&self) -> u64 {
        let e = (self.epoch & ((2u64).pow(41) - 1)) << 24;
        let n = ((self.node_id as u64) & ((2u64).pow(10) - 1)) << 12;
        let s = (self.sequence as u64) & ((2u64).pow(12) - 1);
        return e | n | s;
    }
}
fn chunk(input: u64) -> [u8; 8] {
    return [
        (input >> 56) as u8,
        (input >> 48) as u8,
        (input >> 40) as u8,
        (input >> 32) as u8,
        (input >> 24) as u8,
        (input >> 16) as u8,
        (input >> 8) as u8,
        input as u8,
    ];
}


impl Serialize for Snowflake {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        let flake = self.as_u64();
        let chunks: [u8; 8] = chunk(flake);
        serializer.serialize_str(encode(Crockford, &chunks).as_str())
    }
}

trait FlakeGenerator {
    fn get_flake(&self) -> Snowflake;
    fn get_sequence(&self) -> (u16, u64);
}

impl FlakeGenerator for SnowflakeState {
    fn get_flake(&self) -> Snowflake {
        let (sequence, now) = self.get_sequence();
        let flake = Snowflake { epoch: now, node_id: self.node_id, sequence: sequence };
        return flake;
    }

    fn get_sequence(&self) -> (u16, u64) {
        let mut updated = self.counter.lock().unwrap();
        let now = now_in_ms(self.epoch);
        if now > updated.updated {
            updated.count = 0;
            updated.updated = now;
        } else {
            updated.count += 1;
        }
        return (updated.count, updated.updated);
    }
}

impl Handler for SnowflakeState {
    fn handle(&self, req: Request, mut res: Response) {
        match req.method {
            hyper::Get => {
                let flake = self.get_flake();
                let serialized = serde_json::to_vec(&flake).unwrap();

                res.send(&serialized).unwrap();
            },
            _ => *res.status_mut() = StatusCode::MethodNotAllowed
        }
    }
}

fn now_in_ms(epoch: DateTime<FixedOffset>) -> u64 {
    let now_utc = UTC::now();
    let offset = now_utc - epoch;
    return offset.num_milliseconds() as u64;
}

fn init_state() -> SnowflakeState {
    let dts: String = envoption::with_default("SNOWFLAKE_EPOCH", "2016-01-01T00:00:00Z").unwrap();
    let epoch = DateTime::parse_from_rfc3339(dts.deref()).unwrap();
    let node_id = envoption::with_default("SNOWFLAKE_NODE_ID", 1u16).unwrap();
    SnowflakeState {
        counter: Mutex::new(Counter {
            count: 0,
            updated: now_in_ms(epoch)
        }),
        node_id: node_id,
        epoch: epoch
    }
}

fn main() {
    env_logger::init().unwrap();
    let port: u16 = envoption::with_default("SNOWFLAKE_PORT", 8443u16).unwrap();
    let threads: usize = envoption::with_default("SNOWFLAKE_THREADS", 4 as usize).unwrap();
    let ssl = Openssl::with_cert_and_key("cert.pem", "key.pem").unwrap();
    println!("Starting server on port {}", port);
    info!("Starting server on port {}", port);
    Server::https(SocketAddr::new(IpAddr::from_str("0.0.0.0").unwrap(), port), ssl).unwrap()
        .handle_threads(init_state(), threads).unwrap();
}
