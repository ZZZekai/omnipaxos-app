use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self, BufRead, Write};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Message {
    src: String,
    dest: String,
    body: serde_json::Value,
}

fn main() -> io::Result<()> {
    let stdin = io::stdin();
    let mut reader = stdin.lock().lines();

    // 我们的临时“数据库”，用来扛住第一波单机测试
    let mut kv_store: HashMap<String, serde_json::Value> = HashMap::new();

    eprintln!("KV Node started, waiting for Maelstrom lin-kv workload...");

    while let Some(line) = reader.next() {
        let line = line?;
        let msg: Message = serde_json::from_str(&line).unwrap();

        let msg_type = msg.body["type"].as_str().unwrap_or("");
        eprintln!("Received operation: {}", msg_type);

        let reply_body = match msg_type {
            "init" => {
                serde_json::json!({
                    "type": "init_ok",
                    "in_reply_to": msg.body["msg_id"]
                })
            }
            "read" => {
                let key = msg.body["key"].to_string();
                if let Some(value) = kv_store.get(&key) {
                    serde_json::json!({
                        "type": "read_ok",
                        "in_reply_to": msg.body["msg_id"],
                        "value": value
                    })
                } else {
                    // Maelstrom 规定：找不到 key 要返回错误码 20
                    serde_json::json!({
                        "type": "error",
                        "in_reply_to": msg.body["msg_id"],
                        "code": 20,
                        "text": "key does not exist"
                    })
                }
            }
            "write" => {
                let key = msg.body["key"].to_string();
                let value = msg.body["value"].clone();
                kv_store.insert(key, value);
                serde_json::json!({
                    "type": "write_ok",
                    "in_reply_to": msg.body["msg_id"]
                })
            }
            "cas" => {
                let key = msg.body["key"].to_string();
                let from = &msg.body["from"];
                let to = msg.body["to"].clone();

                match kv_store.get(&key) {
                    Some(current_value) => {
                        if current_value == from {
                            // 旧值匹配，允许更新
                            kv_store.insert(key, to);
                            serde_json::json!({
                                "type": "cas_ok",
                                "in_reply_to": msg.body["msg_id"]
                            })
                        } else {
                            // 旧值不匹配，返回错误码 22 (Precondition failed)
                            serde_json::json!({
                                "type": "error",
                                "in_reply_to": msg.body["msg_id"],
                                "code": 22,
                                "text": "precondition failed"
                            })
                        }
                    }
                    None => {
                        serde_json::json!({
                            "type": "error",
                            "in_reply_to": msg.body["msg_id"],
                            "code": 20,
                            "text": "key does not exist"
                        })
                    }
                }
            }
            _ => {
                // 如果收到不认识的消息，原样丢回去（比如之前的 echo）
                eprintln!("Ignored unknown message type: {}", msg_type);
                continue; 
            }
        };

        // 打包并发送回复
        let reply = Message {
            src: msg.dest.clone(),
            dest: msg.src.clone(),
            body: reply_body,
        };

        println!("{}", serde_json::to_string(&reply).unwrap());
        io::stdout().flush().unwrap();
    }
    Ok(())
}