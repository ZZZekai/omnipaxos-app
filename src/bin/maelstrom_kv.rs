use omnipaxos::{
    messages::Message as OmniMessage,
    util::LogEntry,
    ClusterConfig as OmniClusterConfig,
    OmniPaxos, OmniPaxosConfig,
    ServerConfig as OmniServerConfig,
};
use omnipaxos_kv::common::kv::{Command, CommandId, KVCommand, NodeId};
use omnipaxos_storage::persistent_storage::{PersistentStorage, PersistentStorageConfig};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::io::{self, BufRead, Write};

// ★ 改为 PersistentStorage
type OmniPaxosInstance = OmniPaxos<Command, PersistentStorage<Command>>;

const LEADER_NAME: &str = "n0";

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Message {
    src: String,
    dest: String,
    body: Value,
}

#[derive(Default)]
struct Database {
    db: HashMap<String, Value>,
}

enum ApplyResult {
    WriteOk,
    ReadOk(Value),
    KeyNotFound,
    CasOk,
    CasKeyNotFound,
    CasPreconditionFailed(Value),
}

impl Database {
    fn apply(&mut self, command: KVCommand) -> ApplyResult {
        match command {
            KVCommand::Put(key, value) => {
                self.db.insert(key, value);
                ApplyResult::WriteOk
            }
            KVCommand::Get(key) => match self.db.get(&key) {
                Some(v) => ApplyResult::ReadOk(v.clone()),
                None => ApplyResult::KeyNotFound,
            },
            KVCommand::Delete(key) => {
                self.db.remove(&key);
                ApplyResult::WriteOk
            }
            KVCommand::Cas(key, expected, new_value) => match self.db.get(&key) {
                None => ApplyResult::CasKeyNotFound,
                Some(current) if *current != expected => {
                    ApplyResult::CasPreconditionFailed(current.clone())
                }
                Some(_) => {
                    self.db.insert(key, new_value);
                    ApplyResult::CasOk
                }
            },
        }
    }
}

enum PendingTarget {
    Local(Message),
    Proxy {
        origin: String,
        origin_req_id: u64,
        expected: Option<Value>,
    },
}

struct Node {
    id: String,
    pid: NodeId,
    msg_counter: u64,
    command_counter: u64,
    current_decided_idx: usize,
    db: Database,
    omnipaxos: Option<OmniPaxosInstance>,
    leader_started: bool,
    pending_commands: HashMap<CommandId, PendingTarget>,
    pending_client_replies: HashMap<u64, Message>,
}

impl Node {
    fn new() -> Self {
        Self {
            id: String::new(),
            pid: 0,
            msg_counter: 0,
            command_counter: 0,
            current_decided_idx: 0,
            db: Database::default(),
            omnipaxos: None,
            leader_started: false,
            pending_commands: HashMap::new(),
            pending_client_replies: HashMap::new(),
        }
    }

    fn next_msg_id(&mut self) -> u64 {
        self.msg_counter += 1;
        self.msg_counter
    }

    fn next_command_id(&mut self) -> CommandId {
        self.command_counter += 1;
        ((self.pid as usize) << 32) | (self.command_counter as usize)
    }

    fn reply(&mut self, req: &Message, mut body: Value, out: &mut impl Write) {
        let msg_id = self.next_msg_id();
        body["msg_id"] = Value::from(msg_id);
        body["in_reply_to"] = req.body["msg_id"].clone();
        let reply = Message {
            src: req.dest.clone(),
            dest: req.src.clone(),
            body,
        };
        writeln!(out, "{}", serde_json::to_string(&reply).unwrap()).unwrap();
        out.flush().unwrap();
    }

    fn send_node_message(&mut self, dest: &str, mut body: Value, out: &mut impl Write) {
        let msg_id = self.next_msg_id();
        body["msg_id"] = Value::from(msg_id);
        let msg = Message {
            src: self.id.clone(),
            dest: dest.to_string(),
            body,
        };
        writeln!(out, "{}", serde_json::to_string(&msg).unwrap()).unwrap();
        out.flush().unwrap();
    }

    fn error_body(code: u64, text: &str) -> Value {
        serde_json::json!({
            "type": "error",
            "code": code,
            "text": text,
        })
    }

    fn parse_node_id(node_id: &str) -> NodeId {
        node_id
            .trim_start_matches('n')
            .parse::<u64>()
            .expect("node_id should look like n0, n1, ...")
            + 1
    }

    fn pid_to_node_name(pid: NodeId) -> String {
        format!("n{}", pid - 1)
    }

    fn key_to_string(v: &Value) -> String {
        match v {
            Value::String(s) => s.clone(),
            _ => v.to_string(),
        }
    }

    fn is_leader_node(&self) -> bool {
        self.id == LEADER_NAME
    }

    // ★ 核心改动：使用 PersistentStorage，路径按节点 ID 区分
    fn init_omnipaxos(&mut self, all_node_names: &[String]) {
        let nodes: Vec<NodeId> = all_node_names
            .iter()
            .map(|n| Self::parse_node_id(n))
            .collect();

        let cluster_config = OmniClusterConfig {
            configuration_id: 1,
            nodes,
            flexible_quorum: None,
        };
        let server_config = OmniServerConfig {
            pid: self.pid,
            ..Default::default()
        };
        let config = OmniPaxosConfig {
            cluster_config,
            server_config,
        };

        // ★ 每个节点用独立路径存储，重启后自动恢复
        let storage_path = format!("./storage_{}", self.id);
        eprintln!("{}: using persistent storage at {}", self.id, storage_path);
        let storage_config = PersistentStorageConfig::with(storage_path);
        let storage = PersistentStorage::open(storage_config);

        let op = config.build(storage).expect("failed to build OmniPaxos");
        self.omnipaxos = Some(op);

        // ★ 重启后从 log 重放已决议的命令，重建内存数据库
        self.recover_db_from_log();
    }

    // ★ 新增：重放 log 重建 DB（重启恢复用）
    fn recover_db_from_log(&mut self) {
        let op = self.omnipaxos.as_mut().expect("OmniPaxos not initialized");
        let decided_idx = op.get_decided_idx();

        if decided_idx == 0 {
            eprintln!("{}: fresh start, no log to recover", self.id);
            return;
        }

        eprintln!("{}: recovering {} decided entries from log", self.id, decided_idx);

        let entries = match op.read_decided_suffix(0) {
            Some(e) => e,
            None => {
                eprintln!("{}: read_decided_suffix(0) returned None", self.id);
                return;
            }
        };

        for entry in entries {
            if let LogEntry::Decided(cmd) = entry {
                self.db.apply(cmd.kv_cmd);
            }
        }

        self.current_decided_idx = decided_idx;
        eprintln!("{}: recovery complete, decided_idx={}", self.id, decided_idx);
    }

    fn start_leader_once(&mut self) {
        if self.is_leader_node() && !self.leader_started {
            if let Some(op) = self.omnipaxos.as_mut() {
                op.try_become_leader();
            }
            self.leader_started = true;
        }
    }

    fn ensure_leader_ready(&mut self) {
        if !self.is_leader_node() {
            return;
        }

        self.start_leader_once();

        for _ in 0..20 {
            let ready = if let Some(op) = self.omnipaxos.as_ref() {
                matches!(op.get_current_leader(), Some((leader, true)) if leader == self.pid)
            } else {
                false
            };

            if ready {
                return;
            }

            if let Some(op) = self.omnipaxos.as_mut() {
                op.try_become_leader();
                op.tick();
            }
        }
    }

    fn progress_local(&mut self, out: &mut impl Write) {
        if self.is_leader_node() {
            self.ensure_leader_ready();
        }

        if let Some(op) = self.omnipaxos.as_mut() {
            op.tick();
        }

        self.flush_outgoing(out);
        self.handle_decided_entries(out);
    }

    fn progress_network(&mut self, out: &mut impl Write) {
        self.flush_outgoing(out);
        self.handle_decided_entries(out);
    }

    fn flush_outgoing(&mut self, out: &mut impl Write) {
        let op = self.omnipaxos.as_mut().expect("OmniPaxos not initialized");
        let mut outgoing = Vec::new();
        op.take_outgoing_messages(&mut outgoing);

        if !outgoing.is_empty() {
            eprintln!("{} outgoing msgs: {}", self.id, outgoing.len());
        }

        for msg in outgoing {
            let receiver = msg.get_receiver();
            let dest = Self::pid_to_node_name(receiver);
            let body = serde_json::json!({
                "type": "op",
                "om_msg": msg
            });
            self.send_node_message(&dest, body, out);
        }
    }

    fn append_as_leader(
        &mut self,
        kv_cmd: KVCommand,
        pending: PendingTarget,
        out: &mut impl Write,
    ) {
        self.ensure_leader_ready();

        let command_id = self.next_command_id();
        let command = Command {
            client_id: 1,
            coordinator_id: self.pid,
            id: command_id,
            kv_cmd,
        };

        self.pending_commands.insert(command_id, pending);

        let op = self.omnipaxos.as_mut().expect("OmniPaxos not initialized");
        op.append(command).expect("append failed");

        self.progress_local(out);
    }

    fn build_response_body(
        result: ApplyResult,
        original_req: Option<&Message>,
        expected: Option<&Value>,
    ) -> Value {
        match result {
            ApplyResult::WriteOk => serde_json::json!({ "type": "write_ok" }),
            ApplyResult::ReadOk(value) => serde_json::json!({
                "type": "read_ok",
                "value": value
            }),
            ApplyResult::KeyNotFound => Self::error_body(20, "key not found"),
            ApplyResult::CasOk => serde_json::json!({ "type": "cas_ok" }),
            ApplyResult::CasKeyNotFound => Self::error_body(20, "key not found"),
            ApplyResult::CasPreconditionFailed(current) => {
                let expected_text = if let Some(req) = original_req {
                    req.body["from"].to_string()
                } else if let Some(v) = expected {
                    v.to_string()
                } else {
                    "null".to_string()
                };
                let text = format!("expected {}, got {}", expected_text, current);
                Self::error_body(22, &text)
            }
        }
    }

    fn handle_decided_entries(&mut self, out: &mut impl Write) {
        let op = self.omnipaxos.as_mut().expect("OmniPaxos not initialized");
        let new_decided_idx = op.get_decided_idx();

        if self.current_decided_idx >= new_decided_idx {
            return;
        }

        let decided_entries = op
            .read_decided_suffix(self.current_decided_idx)
            .expect("read_decided_suffix failed");

        self.current_decided_idx = new_decided_idx;
        eprintln!("{} decided up to {}", self.id, self.current_decided_idx);

        for entry in decided_entries {
            let command = match entry {
                LogEntry::Decided(cmd) => cmd,
                _ => continue,
            };

            let result = self.db.apply(command.kv_cmd);

            if let Some(target) = self.pending_commands.remove(&command.id) {
                match target {
                    PendingTarget::Local(req) => {
                        let body = Self::build_response_body(result, Some(&req), None);
                        self.reply(&req, body, out);
                    }
                    PendingTarget::Proxy {
                        origin,
                        origin_req_id,
                        expected,
                    } => {
                        let body = Self::build_response_body(result, None, expected.as_ref());
                        let proxy_body = serde_json::json!({
                            "type": "proxy_resp",
                            "origin_req_id": origin_req_id,
                            "response_body": body
                        });
                        self.send_node_message(&origin, proxy_body, out);
                    }
                }
            }
        }
    }

    fn handle_client_request(&mut self, msg: Message, out: &mut impl Write) {
        let msg_type = msg.body["type"].as_str().unwrap_or("");

        let kv_cmd = match msg_type {
            "read" => {
                let key = Self::key_to_string(&msg.body["key"]);
                KVCommand::Get(key)
            }
            "write" => {
                let key = Self::key_to_string(&msg.body["key"]);
                let value = msg.body["value"].clone();
                KVCommand::Put(key, value)
            }
            "cas" => {
                let key = Self::key_to_string(&msg.body["key"]);
                let from = msg.body["from"].clone();
                let to = msg.body["to"].clone();
                KVCommand::Cas(key, from, to)
            }
            other => {
                eprintln!("Unknown client message type: {}", other);
                return;
            }
        };

        if self.is_leader_node() {
            let pending = PendingTarget::Local(msg);
            self.append_as_leader(kv_cmd, pending, out);
        } else {
            let origin_req_id = msg.body["msg_id"]
                .as_u64()
                .expect("client request must have numeric msg_id");
            self.pending_client_replies.insert(origin_req_id, msg.clone());

            let proxy_body = serde_json::json!({
                "type": "proxy_req",
                "origin": self.id,
                "origin_req_id": origin_req_id,
                "request_body": msg.body
            });
            self.send_node_message(LEADER_NAME, proxy_body, out);
        }
    }

    fn handle_proxy_req(&mut self, msg: Message, out: &mut impl Write) {
        if !self.is_leader_node() {
            return;
        }

        let origin = msg.body["origin"]
            .as_str()
            .expect("proxy_req.origin missing")
            .to_string();

        let origin_req_id = msg.body["origin_req_id"]
            .as_u64()
            .expect("proxy_req.origin_req_id missing");

        let request_body = msg.body["request_body"].clone();
        let req_type = request_body["type"]
            .as_str()
            .expect("proxy_req.request_body.type missing");

        let (kv_cmd, expected) = match req_type {
            "read" => {
                let key = Self::key_to_string(&request_body["key"]);
                (KVCommand::Get(key), None)
            }
            "write" => {
                let key = Self::key_to_string(&request_body["key"]);
                let value = request_body["value"].clone();
                (KVCommand::Put(key, value), None)
            }
            "cas" => {
                let key = Self::key_to_string(&request_body["key"]);
                let from = request_body["from"].clone();
                let to = request_body["to"].clone();
                (KVCommand::Cas(key, from.clone(), to), Some(from))
            }
            other => {
                eprintln!("Unknown proxy request type: {}", other);
                return;
            }
        };

        let pending = PendingTarget::Proxy {
            origin,
            origin_req_id,
            expected,
        };

        self.append_as_leader(kv_cmd, pending, out);
    }

    fn handle_proxy_resp(&mut self, msg: Message, out: &mut impl Write) {
        let origin_req_id = msg.body["origin_req_id"]
            .as_u64()
            .expect("proxy_resp.origin_req_id missing");

        let response_body = msg.body["response_body"].clone();

        if let Some(original_req) = self.pending_client_replies.remove(&origin_req_id) {
            self.reply(&original_req, response_body, out);
        }
    }

    fn handle_op_message(&mut self, msg: Message, out: &mut impl Write) {
        let om_msg: OmniMessage<Command> = serde_json::from_value(msg.body["om_msg"].clone())
            .expect("failed to decode OmniPaxos message");

        let op = self.omnipaxos.as_mut().expect("OmniPaxos not initialized");
        op.handle_incoming(om_msg);

        self.progress_network(out);
    }

    fn handle(&mut self, msg: Message, out: &mut impl Write) {
        let msg_type = msg.body["type"].as_str().unwrap_or("").to_string();

        match msg_type.as_str() {
            "init" => {
                self.id = msg.body["node_id"]
                    .as_str()
                    .unwrap_or("unknown")
                    .to_string();
                self.pid = Self::parse_node_id(&self.id);

                let node_ids: Vec<String> = serde_json::from_value(msg.body["node_ids"].clone())
                    .expect("init.node_ids missing or invalid");

                self.init_omnipaxos(&node_ids);
                self.start_leader_once();

                let body = serde_json::json!({ "type": "init_ok" });
                self.reply(&msg, body, out);
            }
            "echo" => {
                let body = serde_json::json!({
                    "type": "echo_ok",
                    "echo": msg.body["echo"],
                });
                self.reply(&msg, body, out);
            }
            "read" | "write" | "cas" => {
                self.handle_client_request(msg, out);
            }
            "proxy_req" => {
                self.handle_proxy_req(msg, out);
            }
            "proxy_resp" => {
                self.handle_proxy_resp(msg, out);
            }
            "op" => {
                self.handle_op_message(msg, out);
            }
            other => {
                eprintln!("Unknown message type: {}", other);
            }
        }
    }
}

fn main() {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut out = io::BufWriter::new(stdout.lock());
    let mut node = Node::new();

    for line in stdin.lock().lines() {
        let line = line.expect("failed to read line");
        if line.trim().is_empty() {
            continue;
        }

        match serde_json::from_str::<Message>(&line) {
            Ok(msg) => node.handle(msg, &mut out),
            Err(e) => eprintln!("Failed to parse: {} | input: {}", e, line),
        }
    }
}
