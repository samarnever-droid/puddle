//! puddle — a lightweight lakehouse that speaks PostgreSQL and lives in an S3 bucket.
//! M2: full reference SQL engine + extended query protocol (prepared statements).

// the engine lives in the `puddle` library crate (src/lib.rs): this binary
// is just the TCP server wrapper around it. link the same code in-process
// via `use puddle::...` (rlib) or the C ABI (src/ffi.rs, cdylib).
use puddle::engine::{Db, StmtResult, Ty, Value};
use puddle::VERSION;
use puddle::{auth, catalog, copy, engine, sql, wire};
use std::collections::HashMap;
use std::io::Write;
use std::net::TcpListener;
use std::sync::atomic::{AtomicU64, Ordering};

use std::sync::Arc;
use puddle::auth::Users;

fn main() {
    // one explicit crypto provider for the whole process (rustls can't
    // auto-pick when both ring and aws-lc-rs are in the feature tree)
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut bind = "127.0.0.1".to_string();
    let mut port: u16 = 5433;
    let mut data: Option<String> = std::env::var("PUDDLE_DATA").ok();
    let mut endpoint = std::env::var("AWS_ENDPOINT_URL").ok();
    let mut region = std::env::var("AWS_REGION").ok();
    let mut access_key = std::env::var("AWS_ACCESS_KEY_ID").ok();
    let mut secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok();
    let mut ack = std::env::var("PUDDLE_ACK").unwrap_or_else(|_| "committed".into());
    let mut auth_mode = std::env::var("PUDDLE_AUTH").unwrap_or_else(|_| "trust".into());
    let mut auth_file: Option<String> = None;
    let mut tls_cert: Option<String> = None;
    let mut tls_key: Option<String> = None;
    let mut tls_self_signed = false;
    // also listen on a Unix domain socket (psql -h /path/to/dir)
    let mut unix_sock: Option<String> = None;
    let mut tls_required = false;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--bind" => bind = args.next().unwrap_or_default(),
            "--port" => port = args.next().and_then(|v| v.parse().ok()).unwrap_or(5433),
            "--data" => data = args.next(),
            "--endpoint" => endpoint = args.next(),
            "--region" => region = args.next(),
            "--access-key" => access_key = args.next(),
            "--secret-key" => secret_key = args.next(),
            "--ack" => ack = args.next().unwrap_or_default(),
            "--auth" => auth_mode = args.next().unwrap_or_default(),
            "--auth-file" => auth_file = args.next(),
            "--unix" => unix_sock = args.next(), // socket DIRECTORY
            "--tls-cert" => tls_cert = args.next(),
            "--tls-key" => tls_key = args.next(),
            "--tls" => tls_self_signed = true,
            "--tls-required" => tls_required = true,
            "--scram-secret" => {
                // helper mode: print a SCRAM verifier for an auth file, then exit
                let pass = args.next().unwrap_or_default();
                let salt = auth::random_bytes(16).unwrap_or_default();
                let v = auth::build_verifier(&pass, 4096, &salt);
                println!("{}", auth::verifier_string(&v));
                return;
            }
            "--vectorized" => {
                // on|off: differential-testing switch for the vectorized executor
                match args.next().unwrap_or_default().as_str() {
                    "on" | "" => engine::VECTORIZED.store(true, std::sync::atomic::Ordering::Relaxed),
                    "off" => engine::VECTORIZED.store(false, std::sync::atomic::Ordering::Relaxed),
                    v => {
                        eprintln!("--vectorized expects on|off (got {})", v);
                        std::process::exit(2);
                    }
                }
            }
            "-V" | "--version" => {
                println!("puddle {}", VERSION);
                return;
            }
            _ => {
                eprintln!("unknown option {}", a);
                eprintln!("usage: puddle [--bind IP] [--port N]");
                eprintln!("       puddle --data file:///dir | s3://bucket/prefix ");
                eprintln!("              [--unix /sock/dir] (also listen on <dir>/.s.PGSQL.<port>)");
                eprintln!("              [--endpoint URL] [--region R] [--access-key K] [--secret-key K]");
                eprintln!("              [--ack relaxed|committed] [--vectorized on|off]");
                eprintln!("              [--auth trust|password|scram-sha-256] [--auth-file users.json]");
                eprintln!("              [--tls-cert cert.pem --tls-key key.pem | --tls] [--tls-required]");
                eprintln!("env: PUDDLE_DATA, AWS_*, PUDDLE_ACK, PUDDLE_AUTH
auth file: JSON mapping user -> password / scram verifier (see docs)
       generate a verifier with: puddle --scram-secret PASS");
                std::process::exit(2);
            }
        }
    }

    // bind the listener before touching any storage: a port conflict must
    // fail fast without mutating (or initializing) a datadir that another
    // live server may own
    let listener = match TcpListener::bind((bind.as_str(), port)) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("! could not bind {}:{}: {}", bind, port, e);
            std::process::exit(1);
        }
    };

    let db = match &data {
        Some(url) => {
            eprintln!(
                "* opening durable store: {} (ack={})",
                url,
                if ack == "relaxed" { "relaxed" } else { "committed" }
            );
            match puddle::open_storage(
                url,
                endpoint.as_deref(),
                region.as_deref(),
                access_key.as_deref(),
                secret_key.as_deref(),
                &ack,
            ) {
                Ok(db) => db,
                Err(e) => {
                    eprintln!("! {}", e);
                    std::process::exit(1);
                }
            }
        }
        None => puddle::open_memory(),
    };
    // ---- auth + TLS config (M6) ----
    let auth = match auth_mode.as_str() {
        "trust" | "" => AuthMode::Trust,
        "password" => AuthMode::Password,
        "scram-sha-256" => AuthMode::Scram,
        other => {
            eprintln!("! --auth must be trust|password|scram-sha-256 (got {})", other);
            std::process::exit(2);
        }
    };
    let users = if auth != AuthMode::Trust {
        match &auth_file {
            Some(f) => Users::load(f).unwrap_or_else(|e| {
                eprintln!("! {}", e);
                std::process::exit(2);
            }),
            None => {
                eprintln!("! --auth {} requires --auth-file <users.json>", auth_mode);
                std::process::exit(2);
            }
        }
    } else {
        Users::default()
    };
    let tls_cfg: Option<rustls::ServerConfig> = if let (Some(cp), Some(kp)) = (&tls_cert, &tls_key) {
        let certs: Vec<rustls::pki_types::CertificateDer> =
            rustls_pemfile::certs(&mut std::io::BufReader::new(
                std::fs::File::open(cp).unwrap_or_else(|e| {
                    eprintln!("! tls cert {}: {}", cp, e);
                    std::process::exit(2);
                }),
            ))
            .collect::<Result<_, _>>()
            .unwrap_or_else(|e| {
                eprintln!("! tls cert parse: {}", e);
                std::process::exit(2);
            });
        let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(
            std::fs::File::open(kp).unwrap_or_else(|e| {
                eprintln!("! tls key {}: {}", kp, e);
                std::process::exit(2);
            }),
        ))
        .unwrap_or_else(|e| {
            eprintln!("! tls key parse: {}", e);
            std::process::exit(2);
        })
        .unwrap_or_else(|| {
            eprintln!("! tls key file contains no private key");
            std::process::exit(2);
        });
        Some(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .unwrap_or_else(|e| {
                    eprintln!("! tls config: {}", e);
                    std::process::exit(2);
                }),
        )
    } else if tls_self_signed {
        // ephemeral self-signed cert: TLS everywhere without a CA dance
        // (clients use sslmode=require, which does not verify CAs)
        let ck = rcgen::generate_simple_self_signed(vec!["localhost".into(), "puddle".into()])
            .unwrap_or_else(|e| {
                eprintln!("! self-signed cert generation: {}", e);
                std::process::exit(2);
            });
        let key = rustls::pki_types::PrivatePkcs8KeyDer::from(ck.key_pair.serialize_der());
        Some(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![ck.cert.der().clone()], key.into())
                .unwrap_or_else(|e| {
                    eprintln!("! tls config: {}", e);
                    std::process::exit(2);
                }),
        )
    } else {
        None
    };
    let front = FrontConfig {
        auth,
        users: Arc::new(users),
        tls: tls_cfg.map(Arc::new),
        tls_required,
    };
    if tls_required && front.tls.is_none() {
        eprintln!("! --tls-required needs --tls or --tls-cert/--tls-key");
        std::process::exit(2);
    }

    eprintln!("* puddle {} — lightweight lakehouse (Postgres wire, S3-native, hardened)", VERSION);
    eprintln!("* storage: {}", data.as_deref().unwrap_or("memory (RAM only)"));
    eprintln!(
        "* auth: {} ({} users), tls: {}",
        match auth {
            AuthMode::Trust => "trust",
            AuthMode::Password => "password",
            AuthMode::Scram => "scram-sha-256",
        },
        front.users.by_name.len(),
        if front.tls.is_some() {
            if tls_required { "required" } else { "optional" }
        } else {
            "off"
        }
    );
    eprintln!("* listening on {}:{}", bind, port);

    let next_pid = Arc::new(AtomicU64::new(1));

    // optional Unix domain socket listener: same engine, same sessions,
    // no TCP stack (psql -h <dir> picks the socket up by name)
    if let Some(dir) = unix_sock {
        // libpq derives the socket name from the directory + port exactly
        // like PostgreSQL's default: <dir>/.s.PGSQL.<port>, so
        // `psql -h <dir> -p <port>` just works.
        let path = format!("{}/.s.PGSQL.{}", dir.trim_end_matches('/'), port);
        if let Some(dir) = std::path::Path::new(&path).parent() {
            if !dir.as_os_str().is_empty() {
                if let Err(e) = std::fs::create_dir_all(dir) {
                    eprintln!("! cannot create socket dir {}: {}", dir.display(), e);
                    std::process::exit(1);
                }
            }
        }
        // a stale socket file from a crashed server must not block us,
        // but a LIVE server must: probe it before unlinking
        if std::path::Path::new(&path).exists() {
            match std::os::unix::net::UnixStream::connect(&path) {
                Ok(_) => {
                    eprintln!("! unix socket {} is served by a live puddle", path);
                    std::process::exit(1);
                }
                Err(_) => {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
        let listener = match std::os::unix::net::UnixListener::bind(&path) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("! cannot bind unix socket {}: {}", path, e);
                std::process::exit(1);
            }
        };
        eprintln!("* listening on unix:{}", path);
        let db = db.clone();
        let next_pid = next_pid.clone();
        let front = front.clone();
        std::thread::Builder::new()
            .name("unix-accept".into())
            .spawn(move || {
                for stream in listener.incoming() {
                    match stream {
                        Ok(s) => {
                            let db = db.clone();
                            let next_pid = next_pid.clone();
                            let front = front.clone();
                            std::thread::Builder::new()
                                .stack_size(1024 * 1024)
                                .spawn(move || {
                                    let pid = next_pid.fetch_add(1, Ordering::Relaxed) as u32;
                                    let mut sess = Session::new(db, pid, front);
                                    let _ = sess.run(Conn::Unix(s));
                                })
                                .ok();
                        }
                        Err(e) => eprintln!("! unix accept error: {}", e),
                    }
                }
            })
            .ok();
    }
    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                let db = db.clone();
                let next_pid = next_pid.clone();
                let front = front.clone();
                std::thread::Builder::new()
                    .stack_size(1024 * 1024)
                    .spawn(move || {
                        let pid = next_pid.fetch_add(1, Ordering::Relaxed) as u32;
                        let mut sess = Session::new(db, pid, front);
                        let _ = sess.run(Conn::Plain(s));
                    })
                    .ok();
            }
            Err(e) => eprintln!("! accept error: {}", e),
        }
    }
}

// ------------------------------------------------------------- session state --

struct Prepared {
    stmt: sql::Stmt,
    nparams: i32,
}

struct Portal {
    stmt: sql::Stmt,
    params: Vec<Value>,
    result_formats: Vec<i16>,
    executed: bool,
    cols: Vec<(String, Ty)>,
    rows: Vec<Vec<Value>>,
    tag: String,
    pos: usize,
}

struct Txn {
    db: Db,
    observed: HashMap<String, u64>,
}

/// A connection: plain TCP or TLS-wrapped (M6). One code path for both —
/// wire.rs helpers are generic over Read+Write.
pub enum Conn {
    Plain(std::net::TcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ServerConnection, std::net::TcpStream>>),
    Unix(std::os::unix::net::UnixStream),
}

impl std::io::Read for Conn {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Conn::Plain(s) => s.read(buf),
            Conn::Tls(s) => s.read(buf),
            Conn::Unix(s) => s.read(buf),
        }
    }
}

impl std::io::Write for Conn {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Conn::Plain(s) => s.write(buf),
            Conn::Tls(s) => s.write(buf),
            Conn::Unix(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Conn::Plain(s) => s.flush(),
            Conn::Tls(s) => s.flush(),
            Conn::Unix(s) => s.flush(),
        }
    }
}

/// Wraps a plain TCP stream into TLS after replying 'S' to SSLRequest.
fn tls_wrap(
    mut tcp: std::net::TcpStream,
    cfg: &Arc<rustls::ServerConfig>,
) -> std::io::Result<Conn> {
    let mut conn = rustls::ServerConnection::new(cfg.clone())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    conn.complete_io(&mut tcp)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    Ok(Conn::Tls(Box::new(rustls::StreamOwned::new(conn, tcp))))
}

/// Auth + TLS configuration handed to every session.
#[derive(Clone)]
pub struct FrontConfig {
    pub auth: AuthMode,
    pub users: Arc<Users>,
    pub tls: Option<Arc<rustls::ServerConfig>>,
    pub tls_required: bool,
}

#[derive(Clone, Copy, PartialEq)]
pub enum AuthMode {
    Trust,
    Password,
    Scram,
}

struct Session {
    db: Arc<Db>,
    pid: u32,
    cfg: FrontConfig,
    user: String,
    database: String,
    stmts: HashMap<String, Prepared>,
    portals: HashMap<String, Portal>,
    txn: Option<Txn>,
    in_txn: bool,
    txn_failed: bool,
    failed: bool, // extended-protocol error: skip until Sync
}

impl Session {
    fn new(db: Arc<Db>, pid: u32, cfg: FrontConfig) -> Session {
        Session {
            db,
            pid,
            cfg,
            user: "puddle".into(),
            database: "puddle".into(),
            stmts: HashMap::new(),
            portals: HashMap::new(),
            txn: None,
            in_txn: false,
            txn_failed: false,
            failed: false,
        }
    }

    fn txn_char(&self) -> char {
        if self.txn_failed {
            'E'
        } else if self.in_txn {
            'T'
        } else {
            'I'
        }
    }

    /// Cleartext password round: 'R' code 3 -> client 'p' -> verify.
    fn auth_cleartext<S: std::io::Read + std::io::Write>(&self, stream: &mut S) -> bool {
        let mut b = Vec::new();
        wire::auth_cleartext_password(&mut b);
        if wire::flush(stream, &b).is_err() {
            return false;
        }
        let pkt = match wire::read_packet(stream) {
            Ok(Some(p)) if p.kind == b'p' => p,
            _ => return false,
        };
        let given = wire::password_message(&pkt.body);
        let given = given.trim_end_matches('\0');
        let ok = matches!(&self.cfg.users.by_name.get(&self.user),
            Some(e) if e.password.as_deref() == Some(given));
        if !ok {
            self.auth_failed(stream, "password");
            return false;
        }
        true
    }

    fn auth_failed<S: std::io::Write>(&self, stream: &mut S, how: &str) {
        let mut b = Vec::new();
        wire::error(
            &mut b,
            "28P01",
            &format!("password authentication failed for user \"{}\"", self.user),
        );
        let _ = wire::flush(stream, &b);
        eprintln!("! conn {} auth failed ({}, user {:?})", self.pid, how, self.user);
    }

    /// Full SCRAM-SHA-256 exchange (RFC 5802). Unknown users run against a
    /// dummy verifier so the wire behavior is indistinguishable.
    fn auth_scram<S: std::io::Read + std::io::Write>(
        &self,
        stream: &mut S,
        out: &mut Vec<u8>,
    ) -> bool {
        let fail = |s: &mut S| -> bool {
            self.auth_failed(s, "scram");
            false
        };
        let mut b = Vec::new();
        wire::auth_sasl(&mut b, &["SCRAM-SHA-256"]);
        if wire::flush(stream, &b).is_err() {
            return false;
        }
        let pkt = match wire::read_packet(stream) {
            Ok(Some(p)) if p.kind == b'p' => p,
            _ => return false,
        };
        let (mech, first) = match wire::parse_sasl_initial(&pkt.body) {
            Some(x) => x,
            None => return fail(stream),
        };
        if mech != "SCRAM-SHA-256" {
            return fail(stream);
        }
        let client_first = String::from_utf8_lossy(&first).into_owned();
        let verifier = match &self.cfg.users.by_name.get(&self.user) {
            Some(e) => match &e.scram {
                Some(v) => v.clone(),
                None => auth::dummy_verifier(),
            },
            None => auth::dummy_verifier(),
        };
        let srv = match auth::ScramServer::start(verifier, &client_first) {
            Ok(s) => s,
            Err(_) => return fail(stream),
        };
        let mut b = Vec::new();
        wire::auth_sasl_continue(&mut b, srv.server_first.as_bytes());
        if wire::flush(stream, &b).is_err() {
            return false;
        }
        let pkt = match wire::read_packet(stream) {
            Ok(Some(p)) if p.kind == b'p' => p,
            _ => return false,
        };
        let client_final = wire::password_message(&pkt.body);
        let client_final = client_final.trim_end_matches('\0');
        match srv.finish(client_final) {
            Ok(sig) => {
                let vsig = format!("v={}", auth::b64(&sig));
                wire::auth_sasl_final(out, vsig.as_bytes());
                wire::auth_ok(out);
                true
            }
            Err(_) => fail(stream),
        }
    }

    fn run(&mut self, mut stream: Conn) -> std::io::Result<()> {
        if let Conn::Plain(s) = &stream {
            s.set_nodelay(true)?;
        } // (Unix sockets have no Nagle to disable)
        loop {
            match wire::read_startup(&mut stream)? {
                wire::Startup::SslRequest => {
                    // TLS is a TCP-only feature: on a Unix socket (exactly
                    // like PostgreSQL) we always decline with 'N'; libpq
                    // sends SSLRequest there under sslmode=prefer and
                    // continues in the clear when it sees 'N'.
                    let offered: Option<&Arc<rustls::ServerConfig>> =
                        if matches!(stream, Conn::Unix(_)) { None } else { self.cfg.tls.as_ref() };
                    match offered {
                    Some(tls) => {
                        // accept the upgrade: 'S', then complete the handshake
                        if let Conn::Plain(mut tcp) = stream {
                            let _ = tcp.write_all(b"S");
                            let _ = tcp.flush();
                            stream = match tls_wrap(tcp, tls) {
                                Ok(c) => c,
                                Err(e) => {
                                    eprintln!("* conn {} tls handshake failed: {}", self.pid, e);
                                    return Ok(());
                                }
                            };
                            eprintln!("* conn {} tls established", self.pid);
                        } else {
                            return Ok(()); // nested SSLRequest: protocol violation
                        }
                    }
                    None => {
                        if self.cfg.tls_required {
                            let mut b = Vec::new();
                            wire::error(&mut b, "08006", "TLS is required");
                            let _ = wire::flush(&mut stream, &b);
                            return Ok(());
                        }
                        stream.write_all(b"N")?;
                    }
                    }
                },
                wire::Startup::GssencRequest => {
                    stream.write_all(b"N")?;
                }
                wire::Startup::CancelRequest => return Ok(()),
                wire::Startup::Params(params) => {
                    for (k, v) in &params {
                        match k.as_str() {
                            "user" => self.user = v.clone(),
                            "database" => self.database = v.clone(),
                            _ => {}
                        }
                    }
                    catalog::set_dbname(if self.database.is_empty() {
                        "x"
                    } else {
                        &self.database
                    });
                    if self.cfg.tls_required && matches!(stream, Conn::Plain(_)) {
                        // client never asked for TLS (--sslmode=disable):
                        // reject before any authentication happens
                        let mut b = Vec::new();
                        wire::error(&mut b, "08006", "TLS is required");
                        let _ = wire::flush(&mut stream, &b);
                        return Ok(());
                    }
                    break;
                }
            }
        }

        // ---- authentication (M6) ----
        let mut out = Vec::with_capacity(1024);
        match self.cfg.auth {
            AuthMode::Trust => {
                wire::auth_ok(&mut out);
            }
            AuthMode::Password => {
                let ok = self.auth_cleartext(&mut stream);
                if !ok {
                    return Ok(());
                }
                wire::auth_ok(&mut out);
            }
            AuthMode::Scram => {
                if !self.auth_scram(&mut stream, &mut out) {
                    return Ok(());
                }
            }
        }
        wire::param_status(&mut out, "server_version", &format!("16.9 (puddle {})", VERSION));
        wire::param_status(&mut out, "client_encoding", "UTF8");
        wire::param_status(&mut out, "DateStyle", "ISO, MDY");
        wire::param_status(&mut out, "integer_datetimes", "on");
        wire::param_status(&mut out, "standard_conforming_strings", "on");
        wire::param_status(&mut out, "application_name", "");
        wire::param_status(&mut out, "is_superuser", "on");
        wire::param_status(&mut out, "session_authorization", &self.user);
        wire::backend_key_data(&mut out, self.pid, 0x7075_6464);
        wire::ready_for_query(&mut out, 'I');
        wire::flush(&mut stream, &out)?;
        eprintln!("* conn {} ({}->{}) connected", self.pid, self.user, self.database);

        let mut out = Vec::with_capacity(4096);
        loop {
            out.clear();
            let pkt = match wire::read_packet(&mut stream)? {
                Some(p) => p,
                None => break,
            };
            match pkt.kind {
                b'Q' => {
                    let mut b = pkt.body.clone();
                    while b.last() == Some(&0) {
                        b.pop();
                    }
                    let q = String::from_utf8_lossy(&b).into_owned();
                    self.simple_query(&q, &mut out, &mut stream);
                    wire::ready_for_query(&mut out, self.txn_char());
                    if wire::flush(&mut stream, &out).is_err() {
                        break;
                    }
                }
                // stray CopyData/CopyDone/CopyFail: a client aborting a
                // failed COPY sends these after the ErrorResponse; drain them
                b'd' | b'c' | b'f' => {
                    continue;
                }
                b'P' => {
                    if self.failed {
                        continue;
                    }
                    self.ext_parse(&pkt.body, &mut out);
                    if wire::flush(&mut stream, &out).is_err() {
                        break;
                    }
                }
                b'B' => {
                    if self.failed {
                        continue;
                    }
                    self.ext_bind(&pkt.body, &mut out);
                    if wire::flush(&mut stream, &out).is_err() {
                        break;
                    }
                }
                b'D' => {
                    if self.failed {
                        continue;
                    }
                    self.ext_describe(&pkt.body, &mut out);
                    if wire::flush(&mut stream, &out).is_err() {
                        break;
                    }
                }
                b'E' => {
                    if self.failed {
                        continue;
                    }
                    self.ext_execute(&pkt.body, &mut out);
                    if wire::flush(&mut stream, &out).is_err() {
                        break;
                    }
                }
                b'C' => {
                    if self.failed {
                        continue;
                    }
                    self.ext_close(&pkt.body, &mut out);
                    if wire::flush(&mut stream, &out).is_err() {
                        break;
                    }
                }
                b'H' => {
                    if wire::flush(&mut stream, &out).is_err() {
                        break;
                    }
                }
                b'S' => {
                    self.failed = false;
                    wire::ready_for_query(&mut out, self.txn_char());
                    if wire::flush(&mut stream, &out).is_err() {
                        break;
                    }
                }
                b'X' => break,
                other => {
                    wire::error(
                        &mut out,
                        "0A000",
                        &format!("unsupported protocol message '{}'", other as char),
                    );
                    wire::ready_for_query(&mut out, self.txn_char());
                    if wire::flush(&mut stream, &out).is_err() {
                        break;
                    }
                }
            }
        }
        eprintln!("* conn {} disconnected", self.pid);
        Ok(())
    }

    // ------------------------------------------------- simple query protocol --

    /// COPY ... FROM STDIN wire flow: CopyInResponse('G'), collect
    /// CopyData('d') until CopyDone('c') / CopyFail('f'), then one atomic
    /// engine insert batch. Returns rows copied via the CommandComplete tag.
    fn copy_in_flow(
        &mut self,
        table: &sql::TableRef,
        cols: &[String],
        opts: &sql::CopyOpts,
        stream: &mut Conn,
        out: &mut Vec<u8>,
    ) -> Result<(), engine::PqError> {
        if self.txn.is_some() {
            return Err(engine::PqError::new(
                "0A000",
                "COPY FROM STDIN inside an explicit transaction is not supported",
            ));
        }
        // CopyInResponse: overall format 0 (text), 0 columns listed
        let mut body: Vec<u8> = Vec::new();
        body.push(0u8);
        body.extend_from_slice(&0i16.to_be_bytes());
        wire::msg(out, b'G', &body);
        if wire::flush(stream, out).is_err() {
            return Err(engine::PqError::new("08006", "connection lost during COPY"));
        }
        out.clear(); // the CopyInResponse was already sent; C/Z only from here
        let mut data: Vec<u8> = Vec::new();
        loop {
            let pkt = match wire::read_packet(stream) {
                Ok(Some(p)) => p,
                Ok(None) => {
                    return Err(engine::PqError::new("08006", "connection lost during COPY"))
                }
                Err(_) => {
                    return Err(engine::PqError::new(
                        "08P01",
                        "protocol error during COPY (bad packet)",
                    ))
                }
            };
            match pkt.kind {
                b'd' => data.extend_from_slice(&pkt.body),
                b'c' => break,
                b'f' => {
                    // CopyFail: client aborted; report its message
                    let mut b = pkt.body.clone();
                    while b.last() == Some(&0) {
                        b.pop();
                    }
                    let m = String::from_utf8_lossy(&b).into_owned();
                    self.mark_error();
                    wire::error(out, "57014", &format!("COPY from stdin failed: {}", m));
                    return Ok(());
                }
                other => {
                    self.mark_error();
                    wire::error(
                        out,
                        "08P01",
                        &format!("unexpected message type 0x{:02x} during COPY", other),
                    );
                    return Ok(());
                }
            }
        }
        let text = match String::from_utf8(data) {
            Ok(t) => t,
            Err(_) => {
                return Err(engine::PqError::new(
                    "22021",
                    "COPY data is not valid UTF-8",
                ))
            }
        };
        let n = self.db.copy_in(table, cols, opts, &text)?;
        wire::command_complete(out, &format!("COPY {}", n));
        Ok(())
    }

    /// COPY ... TO STDOUT wire flow: CopyOutResponse('H'), CopyData chunks,
    /// CopyDone('c'), CommandComplete.
    fn copy_out_flow(
        &mut self,
        src: &sql::CopySource,
        opts: &sql::CopyOpts,
        out: &mut Vec<u8>,
    ) -> Result<(), engine::PqError> {
        let (cols, rows) = self.db.copy_out(src, opts)?;
        // CopyOutResponse: format 0, per-column formats all 0
        let mut body: Vec<u8> = Vec::new();
        body.push(0u8);
        body.extend_from_slice(&(cols.len() as i16).to_be_bytes());
        for _ in &cols {
            body.extend_from_slice(&0i16.to_be_bytes());
        }
        wire::msg(out, b'H', &body);
        if opts.header {
            let hdr: Vec<engine::Value> =
                cols.iter().map(|(n, _)| engine::Value::Text(n.clone())).collect();
            let line = if opts.csv {
                copy::encode_csv_row(&hdr, opts.delimiter, opts.quote)
            } else {
                copy::encode_text_row(&hdr, opts.delimiter, &opts.null)
            };
            wire::msg(out, b'd', line.as_bytes());
        }
        let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
        for r in &rows {
            let line = if opts.csv {
                copy::encode_csv_row(r, opts.delimiter, opts.quote)
            } else {
                copy::encode_text_row(r, opts.delimiter, &opts.null)
            };
            buf.extend_from_slice(line.as_bytes());
            if buf.len() >= 64 * 1024 {
                let chunk = std::mem::take(&mut buf);
                wire::msg(out, b'd', &chunk);
            }
        }
        if !buf.is_empty() {
            wire::msg(out, b'd', &buf);
        }
        wire::msg(out, b'c', &[]);
        wire::command_complete(out, &format!("COPY {}", rows.len()));
        Ok(())
    }

    fn simple_query(&mut self, q: &str, out: &mut Vec<u8>, stream: &mut Conn) {
        let toks = match sql::lex(q) {
            Ok(t) => t,
            Err(e) => {
                self.mark_error();
                wire::error(out, "42601", &format!("syntax error: {}", e));
                return;
            }
        };
        let mut p = sql::Parser::new(toks);
        let mut any = false;
        loop {
            let stmt = match p.parse_one() {
                Ok(s) => s,
                Err(e) => {
                    self.mark_error();
                    wire::error(out, "42601", &format!("syntax error: {}", e));
                    return;
                }
            };
            let stmt = match stmt {
                Some(s) => s,
                None => break,
            };
            any = true;
            if self.txn_failed && !matches!(stmt, sql::Stmt::Commit | sql::Stmt::Rollback) {
                wire::error(
                    out,
                    "25P02",
                    "current transaction is aborted, commands ignored until end of transaction block",
                );
                return;
            }
            // COPY is wire-driven: needs the copy-data stream (simple
            // protocol only; extended protocol gets a clean 0A000)
            match &stmt {
                sql::Stmt::CopyIn { table, cols, opts } => {
                    match self.copy_in_flow(table, cols, opts, stream, out) {
                        Ok(()) => {}
                        Err(e) => {
                            self.mark_error();
                            wire::error(out, e.code, &e.msg);
                            return;
                        }
                    }
                }
                sql::Stmt::CopyOut { src, opts } => {
                    match self.copy_out_flow(src, opts, out) {
                        Ok(()) => {}
                        Err(e) => {
                            self.mark_error();
                            wire::error(out, e.code, &e.msg);
                            return;
                        }
                    }
                }
                _ => match self.exec_stmt(&stmt, &[]) {
                    Ok(res) => {
                        self.send_result(&res, &[], out);
                    }
                    Err(e) => {
                        self.mark_error();
                        wire::error(out, e.code, &e.msg);
                        return;
                    }
                },
            }
        }
        if !any {
            wire::empty_query_response(out);
        }
    }

    /// The database statements in this transaction see (snapshot or global).
    fn active_db(&self) -> &Db {
        match &self.txn {
            Some(t) => &t.db,
            None => &self.db,
        }
    }

    /// Executes one statement with transaction semantics.
    fn exec_stmt(&mut self, stmt: &sql::Stmt, params: &[Value]) -> Result<StmtResult, engine::PqError> {
        use sql::Stmt::*;
        match stmt {
            Begin => {
                if self.txn.is_none() {
                    let (snap, observed) = self.db.snapshot();
                    self.txn = Some(Txn { db: snap, observed });
                }
                self.in_txn = true;
                self.txn_failed = false;
                Ok(StmtResult::Tag("BEGIN".into()))
            }
            Commit => {
                if let Some(t) = self.txn.take() {
                    self.db.apply_txn(t.db, &t.observed)?;
                }
                self.in_txn = false;
                self.txn_failed = false;
                Ok(StmtResult::Tag("COMMIT".into()))
            }
            Rollback => {
                self.txn = None;
                self.in_txn = false;
                self.txn_failed = false;
                Ok(StmtResult::Tag("ROLLBACK".into()))
            }
            // CHECKPOINT is not transactional (like PostgreSQL): always global
            Checkpoint => self.db.execute(stmt, params),
            // snapshots pin durable state immediately; deferring them to
            // COMMIT would need meta-CAS compensation on rollback, so they
            // refuse inside explicit transactions (clean, never surprising)
            CreateSnapshot { .. } | DropSnapshot { .. } => {
                if self.txn.is_some() {
                    Err(engine::PqError::new(
                        "25001",
                        "CREATE/DROP SNAPSHOT is not allowed inside a transaction",
                    ))
                } else {
                    self.db.execute(stmt, params)
                }
            }
            other => self.active_db().execute(other, params),
        }
    }

    fn mark_error(&mut self) {
        if self.in_txn {
            self.txn_failed = true;
        }
    }

    fn send_result(&self, res: &StmtResult, formats: &[i16], out: &mut Vec<u8>) {
        match res {
            StmtResult::Rows { cols, rows, tag } => {
                let wire_cols: Vec<(String, u32, i16, i16)> = cols
                    .iter()
                    .enumerate()
                    .map(|(i, (n, t))| {
                        let fmt = if formats.len() == 1 {
                            formats[0]
                        } else {
                            formats.get(i).copied().unwrap_or(0)
                        };
                        (n.clone(), t.oid(), t.typlen(), fmt)
                    })
                    .collect();
                wire::row_description_fmt(out, &wire_cols);
                for r in rows {
                    self.send_row(r, &wire_cols, out);
                }
                wire::command_complete(out, tag);
            }
            StmtResult::Tag(tag) => wire::command_complete(out, tag),
        }
    }

    fn send_row(&self, row: &[Value], wire_cols: &[(String, u32, i16, i16)], out: &mut Vec<u8>) {
        let mut enc = Vec::with_capacity(row.len());
        for (i, v) in row.iter().enumerate() {
            let fmt = wire_cols[i].3;
            enc.push(encode_value(v, fmt));
        }
        wire::data_row_raw(out, &enc);
    }

    // ---------------------------------------------- extended query protocol --

    fn ext_parse(&mut self, body: &[u8], out: &mut Vec<u8>) {
        let mut rd = wire::Rd::new(body);
        let name = match rd.cstr() {
            Some(n) => n,
            None => return self.ext_err(out, "42601", "malformed Parse"),
        };
        let query = match rd.cstr() {
            Some(q) => q,
            None => return self.ext_err(out, "42601", "malformed Parse"),
        };
        let _nparam_oids = rd.i16(); // we infer from usage
        let toks = match sql::lex(&query) {
            Ok(t) => t,
            Err(e) => return self.ext_err(out, "42601", &format!("syntax error: {}", e)),
        };
        let stmts = {
            let mut p = sql::Parser::new(toks);
            match p.parse_all() {
                Ok(s) => s,
                Err(e) => return self.ext_err(out, "42601", &format!("syntax error: {}", e)),
            }
        };
        if stmts.len() > 1 {
            return self.ext_err(
                out,
                "42601",
                "cannot insert multiple commands into a prepared statement",
            );
        }
        match stmts.into_iter().next() {
            Some(stmt) => {
                let nparams = stmt_max_param(&stmt);
                self.stmts.insert(name, Prepared { stmt, nparams });
                wire::parse_complete(out);
            }
            None => self.ext_err(out, "42601", "empty prepared statement"),
        }
    }

    fn ext_bind(&mut self, body: &[u8], out: &mut Vec<u8>) {
        let mut rd = wire::Rd::new(body);
        let portal_name = match rd.cstr() {
            Some(n) => n,
            None => return self.ext_err(out, "42601", "malformed Bind"),
        };
        let stmt_name = match rd.cstr() {
            Some(n) => n,
            None => return self.ext_err(out, "42601", "malformed Bind"),
        };
        let nfmt = match rd.i16() {
            Some(n) => n.max(0) as usize,
            None => return self.ext_err(out, "42601", "malformed Bind"),
        };
        let mut fmts = Vec::with_capacity(nfmt);
        for _ in 0..nfmt {
            match rd.i16() {
                Some(f) => fmts.push(f),
                None => return self.ext_err(out, "42601", "malformed Bind"),
            }
        }
        let nparams = match rd.i16() {
            Some(n) => n.max(0) as usize,
            None => return self.ext_err(out, "42601", "malformed Bind"),
        };
        let mut params: Vec<Value> = Vec::with_capacity(nparams);
        for i in 0..nparams {
            let fmt = if fmts.len() == 1 {
                fmts[0]
            } else {
                fmts.get(i).copied().unwrap_or(0)
            };
            let len = match rd.i32() {
                Some(l) => l,
                None => return self.ext_err(out, "42601", "malformed Bind"),
            };
            if len == -1 {
                params.push(Value::Null);
                continue;
            }
            if len < 0 {
                return self.ext_err(out, "42601", "malformed Bind parameter length");
            }
            let bytes = match rd.take(len as usize) {
                Some(b) => b,
                None => return self.ext_err(out, "42601", "malformed Bind"),
            };
            if fmt != 0 {
                return self.ext_err(
                    out,
                    "0A000",
                    "binary parameter format is not supported",
                );
            }
            params.push(Value::Text(String::from_utf8_lossy(bytes).into_owned()));
        }
        let nresfmt = match rd.i16() {
            Some(n) => n.max(0) as usize,
            None => return self.ext_err(out, "42601", "malformed Bind"),
        };
        let mut resfmts = Vec::with_capacity(nresfmt);
        for _ in 0..nresfmt {
            match rd.i16() {
                Some(f) => resfmts.push(f),
                None => return self.ext_err(out, "42601", "malformed Bind"),
            }
        }

        let prep = match self.stmts.get(&stmt_name) {
            Some(p) => p,
            None => {
                return self.ext_err(
                    out,
                    "26000",
                    &format!("prepared statement \"{}\" does not exist", stmt_name),
                )
            }
        };
        if (prep.nparams as usize) != params.len() {
            return self.ext_err(
                out,
                "08P01",
                &format!(
                    "bind message supplies {} parameters, but prepared statement requires {}",
                    params.len(),
                    prep.nparams
                ),
            );
        }
        let portal = Portal {
            stmt: prep.stmt.clone(),
            params,
            result_formats: resfmts,
            executed: false,
            cols: Vec::new(),
            rows: Vec::new(),
            tag: String::new(),
            pos: 0,
        };
        self.portals.insert(portal_name, portal);
        wire::bind_complete(out);
    }

    fn ext_describe(&mut self, body: &[u8], out: &mut Vec<u8>) {
        let mut rd = wire::Rd::new(body);
        let kind = match rd.take(1) {
            Some(b) => b[0],
            None => return self.ext_err(out, "42601", "malformed Describe"),
        };
        let name = match rd.cstr() {
            Some(n) => n,
            None => return self.ext_err(out, "42601", "malformed Describe"),
        };
        match kind {
            b'S' => {
                let prep = match self.stmts.get(&name) {
                    Some(p) => p,
                    None => {
                        return self.ext_err(
                            out,
                            "26000",
                            &format!("prepared statement \"{}\" does not exist", name),
                        )
                    }
                };
                wire::parameter_description(
                    out,
                    &vec![0u32; prep.nparams.max(0) as usize],
                );
                if let sql::Stmt::Select(sel) = &prep.stmt {
                    match self.active_db().describe(sel) {
                        Ok(cols) => {
                            let wire_cols: Vec<(String, u32, i16, i16)> = cols
                                .iter()
                                .map(|(n, t)| (n.clone(), t.oid(), t.typlen(), 0))
                                .collect();
                            wire::row_description_fmt(out, &wire_cols);
                        }
                        Err(e) => self.ext_err(out, e.code, &e.msg),
                    }
                } else {
                    wire::no_data(out);
                }
            }
            b'P' => {
                let portal = match self.portals.get(&name) {
                    Some(p) => p,
                    None => {
                        return self.ext_err(
                            out,
                            "34000",
                            &format!("portal \"{}\" does not exist", name),
                        )
                    }
                };
                if let sql::Stmt::Select(sel) = &portal.stmt {
                    match self.active_db().describe(sel) {
                        Ok(cols) => {
                            let rf = &portal.result_formats;
                            let wire_cols: Vec<(String, u32, i16, i16)> = cols
                                .iter()
                                .enumerate()
                                .map(|(i, (n, t))| {
                                    let fmt = if rf.len() == 1 {
                                        rf[0]
                                    } else {
                                        rf.get(i).copied().unwrap_or(0)
                                    };
                                    (n.clone(), t.oid(), t.typlen(), fmt)
                                })
                                .collect();
                            wire::row_description_fmt(out, &wire_cols);
                        }
                        Err(e) => self.ext_err(out, e.code, &e.msg),
                    }
                } else if matches!(
                    portal.stmt,
                    sql::Stmt::Insert { returning: ref r, .. } if !r.is_empty()
                ) || matches!(
                    portal.stmt,
                    sql::Stmt::Update { returning: ref r, .. } if !r.is_empty()
                ) || matches!(
                    portal.stmt,
                    sql::Stmt::Delete { returning: ref r, .. } if !r.is_empty()
                ) {
                    // RETURNING columns: infer by executing later; describe as no data
                    // for now (drivers that need cols will Describe the portal again).
                    wire::no_data(out);
                } else {
                    wire::no_data(out);
                }
            }
            _ => self.ext_err(out, "42601", "malformed Describe kind"),
        }
    }

    fn ext_execute(&mut self, body: &[u8], out: &mut Vec<u8>) {
        let mut rd = wire::Rd::new(body);
        let name = match rd.cstr() {
            Some(n) => n,
            None => return self.ext_err(out, "42601", "malformed Execute"),
        };
        let maxrows = match rd.i32() {
            Some(m) if m > 0 => m as usize,
            _ => 0, // 0 = unlimited
        };
        if self.txn_failed && !matches!(self.portals.get(&name).map(|p| &p.stmt), Some(sql::Stmt::Commit) | Some(sql::Stmt::Rollback)) {
            return self.ext_err(
                out,
                "25P02",
                "current transaction is aborted, commands ignored until end of transaction block",
            );
        }

        // take the portal out to execute (we reinsert if not finished)
        let mut portal = match self.portals.remove(&name) {
            Some(p) => p,
            None => {
                return self.ext_err(
                    out,
                    "34000",
                    &format!("portal \"{}\" does not exist", name),
                )
            }
        };

        if !portal.executed {
            match self.exec_stmt(&portal.stmt, &portal.params) {
                Ok(StmtResult::Rows { cols, rows, tag }) => {
                    portal.cols = cols;
                    portal.rows = rows;
                    portal.tag = tag;
                }
                Ok(StmtResult::Tag(tag)) => {
                    portal.tag = tag;
                }
                Err(e) => {
                    self.mark_error();
                    return self.ext_err(out, e.code, &e.msg);
                }
            }
            portal.executed = true;
        }

        if portal.rows.is_empty() && portal.tag.is_empty() {
            // non-SELECT already tagged
            let tag = portal.tag.clone();
            wire::command_complete(out, &tag);
            return;
        }

        let rf = portal.result_formats.clone();
        let wire_cols: Vec<(String, u32, i16, i16)> = portal
            .cols
            .iter()
            .enumerate()
            .map(|(i, (n, t))| {
                let fmt = if rf.len() == 1 {
                    rf[0]
                } else {
                    rf.get(i).copied().unwrap_or(0)
                };
                (n.clone(), t.oid(), t.typlen(), fmt)
            })
            .collect();

        let total = portal.rows.len();
        let end = if maxrows == 0 {
            total
        } else {
            (portal.pos + maxrows).min(total)
        };
        for row in &portal.rows[portal.pos..end] {
            self.send_row(row, &wire_cols, out);
        }
        portal.pos = end;
        if end >= total {
            let tag = portal.tag.clone();
            wire::command_complete(out, &tag);
            // done: drop the portal (it cannot be re-executed per protocol)
        } else {
            wire::portal_suspended(out);
            self.portals.insert(name, portal);
        }
    }

    fn ext_close(&mut self, body: &[u8], out: &mut Vec<u8>) {
        let mut rd = wire::Rd::new(body);
        let kind = match rd.take(1) {
            Some(b) => b[0],
            None => return self.ext_err(out, "42601", "malformed Close"),
        };
        let name = match rd.cstr() {
            Some(n) => n,
            None => return self.ext_err(out, "42601", "malformed Close"),
        };
        match kind {
            b'S' => {
                self.stmts.remove(&name);
            }
            b'P' => {
                self.portals.remove(&name);
            }
            _ => return self.ext_err(out, "42601", "malformed Close kind"),
        }
        wire::close_complete(out);
    }

    fn ext_err(&mut self, out: &mut Vec<u8>, code: &str, msg: &str) {
        self.failed = true;
        self.mark_error();
        wire::error(out, code, msg);
    }
}

// ------------------------------------------------------------------ helpers --

fn stmt_max_param(stmt: &sql::Stmt) -> i32 {
    use sql::Stmt;
    fn e_of(e: &sql::Expr, m: &mut i32) {
        *m = (*m).max(e.max_param());
    }
    let mut m = 0i32;
    match stmt {
        Stmt::Select(s) => {
            for it in &s.items {
                if let sql::SelectItem::Expr(e, _) = it {
                    e_of(e, &mut m);
                }
            }
            for it in &s.from_items {
                for j in &it.joins {
                    if let Some(e) = &j.on {
                        e_of(e, &mut m);
                    }
                }
            }
            if let Some(e) = &s.where_ {
                e_of(e, &mut m);
            }
            for e in &s.group_by {
                e_of(e, &mut m);
            }
            if let Some(e) = &s.having {
                e_of(e, &mut m);
            }
            for (e, _, _) in &s.order_by {
                e_of(e, &mut m);
            }
            if let Some(e) = &s.limit {
                e_of(e, &mut m);
            }
            if let Some(e) = &s.offset {
                e_of(e, &mut m);
            }
        }
        Stmt::Insert { rows, query, conflict, .. } => {
            for row in rows {
                for e in row {
                    e_of(e, &mut m);
                }
            }
            if let Some(q) = query {
                m = m.max(stmt_max_param(q));
            }
            if let Some(oc) = conflict {
                if let sql::ConflictAction::Update { sets, guard } = &oc.action {
                    for (_, e) in sets {
                        e_of(e, &mut m);
                    }
                    if let Some(g) = guard {
                        e_of(g, &mut m);
                    }
                }
            }
        }
        Stmt::Update { sets, where_, .. } => {
            for (_, e) in sets {
                e_of(e, &mut m);
            }
            if let Some(e) = where_ {
                e_of(e, &mut m);
            }
        }
        Stmt::Delete { where_: Some(e), .. } => e_of(e, &mut m),
        _ => {}
    }
    m
}

/// Encodes a value for the wire in the requested format (0 = text, 1 = binary).
/// Binary temporal layouts are PG's: date = i32 days since 2000-01-01,
/// time = i64 micros since midnight, timestamp = i64 micros since
/// 2000-01-01, interval = [i64 micros][i32 days][i32 months].
fn encode_value(v: &Value, fmt: i16) -> Option<Vec<u8>> {
    // days between 1970-01-01 and 2000-01-01 (PG's binary epoch)
    const PG_EPOCH_DAYS: i32 = 10_957;
    match (v, fmt) {
        (Value::Null, _) => None,
        (v, 0) => engine::value_to_text(v).map(|s| s.into_bytes()),
        (Value::Bool(b), _) => Some(vec![*b as u8]),
        (Value::Int(i), _) => Some(i.to_be_bytes().to_vec()),
        (Value::Float(f), _) => Some(f.to_be_bytes().to_vec()),
        (Value::Text(s), _) => Some(s.as_bytes().to_vec()),
        (Value::Date(d), _) => Some((d - PG_EPOCH_DAYS).to_be_bytes().to_vec()),
        (Value::Time(t), _) => Some(t.to_be_bytes().to_vec()),
        (Value::Timestamp(us), _) => {
            let off = PG_EPOCH_DAYS as i64 * 86_400_000_000;
            Some((us - off).to_be_bytes().to_vec())
        }
        (Value::Interval { months, days, micros }, _) => {
            let mut out = Vec::with_capacity(16);
            out.extend_from_slice(&micros.to_be_bytes());
            out.extend_from_slice(&days.to_be_bytes());
            out.extend_from_slice(&months.to_be_bytes());
            Some(out)
        }
    }
}
