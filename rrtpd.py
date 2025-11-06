#!/usr/bin/env python3
"""
RRTP multithreaded server — corrected safe I/O and header handling.
Save as rrtp_server_full.py and run: python rrtp_server_full.py
"""
import socket
import threading
import os
import datetime
import json
import shutil
import traceback
import time
import select
import platform
import uuid

HOST = '0.0.0.0'
PORT = 8082
WWW_DIR = 'rrtp_www'
MAILBOX_DIR = 'mailbox'
USERS_FILE = 'users.json'
LOG_FILE = 'rrtp_server_full.log'
SERVER_NAME = "RRTP v3.0"

RECV_CHUNK = 8192
HEADER_POLL = 0.05
IDLE_TIMEOUT = 300

DEFAULT_USERS = {
    "admin": {"password": "alpine", "role": "admin"},
    "guest": {"password": "guest", "role": "user"}
}

os.makedirs(WWW_DIR, exist_ok=True)
os.makedirs(MAILBOX_DIR, exist_ok=True)

_clients_lock = threading.Lock()
clients = {}
connections = set()
shutdown_flag = threading.Event()

def log(msg: str):
    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{ts}] {msg}"
    try:
        print(line)
    except Exception:
        pass
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(line + "\n")
    except Exception:
        pass

def load_users():
    if not os.path.exists(USERS_FILE):
        try:
            with open(USERS_FILE, 'w', encoding='utf-8') as f:
                json.dump(DEFAULT_USERS, f, indent=2)
        except Exception:
            pass
    try:
        with open(USERS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return DEFAULT_USERS.copy()

users = load_users()

def normalize_path(base, path):
    if not path:
        return base
    if path.startswith('/'):
        candidate = os.path.join(WWW_DIR, path.lstrip('/'))
    else:
        candidate = os.path.join(base, path)
    candidate = os.path.normpath(candidate)
    try:
        if not os.path.abspath(candidate).startswith(os.path.abspath(WWW_DIR)):
            return base
    except Exception:
        return base
    return candidate

def relpath_from_root(abs_path):
    rp = os.path.relpath(abs_path, WWW_DIR)
    return '/' if rp == '.' else '/' + rp.replace('\\','/')

def mailbox_path(user):
    p = os.path.join(MAILBOX_DIR, user)
    os.makedirs(p, exist_ok=True)
    return p

def store_message(user, text):
    p = mailbox_path(user)
    idx = 1
    while True:
        fn = f"{idx:04d}.txt"
        full = os.path.join(p, fn)
        if not os.path.exists(full):
            break
        idx += 1
    with open(full, 'w', encoding='utf-8') as f:
        f.write(f"From: system\nDate: {datetime.datetime.now().isoformat()}\n\n")
        f.write(text)
    return fn

def list_mail(user):
    p = mailbox_path(user)
    if not os.path.exists(p):
        return []
    return sorted(os.listdir(p))

def read_mail(user, mid):
    p = mailbox_path(user)
    full = os.path.join(p, mid)
    if not os.path.exists(full):
        return None
    with open(full, 'r', encoding='utf-8') as f:
        return f.read()

def safe_sendall(conn, data: bytes):
    try:
        conn.sendall(data)
        return True
    except BrokenPipeError:
        return False
    except Exception as e:
        log(f"sendall error: {e}")
        return False

def respond(conn, status_line: str, headers: dict = None, body = b''):
    try:
        if headers is None:
            headers = {}
        if isinstance(body, str):
            body_bytes = body.encode('utf-8')
            headers.setdefault('Content-Type', 'text/plain; charset=utf-8')
        elif isinstance(body, bytes):
            body_bytes = body
        elif body is None:
            body_bytes = b''
        else:
            try:
                body_bytes = json.dumps(body).encode('utf-8')
                headers.setdefault('Content-Type', 'application/json; charset=utf-8')
            except Exception:
                body_bytes = str(body).encode('utf-8')
                headers.setdefault('Content-Type', 'text/plain; charset=utf-8')
        hdrs = dict(headers)
        hdrs['Content-Length'] = str(len(body_bytes))
        head_lines = [status_line] + [f"{k}: {v}" for k, v in hdrs.items()]
        head = ("\r\n".join(head_lines) + "\r\n\r\n").encode('utf-8')
        if not safe_sendall(conn, head):
            return False
        if body_bytes:
            return safe_sendall(conn, body_bytes)
        return True
    except Exception as e:
        log(f"respond exception: {e}")
        return False

def read_until_double_newline(conn, timeout=10):
    buf = b''
    start = time.time()
    while True:
        elapsed = time.time() - start
        if elapsed >= timeout:
            break
        try:
            r, _, _ = select.select([conn], [], [], HEADER_POLL)
        except Exception:
            time.sleep(0.01)
            continue
        if not r:
            continue
        try:
            part = conn.recv(RECV_CHUNK)
        except Exception:
            return None, b''
        if not part:
            return None, b''
        buf += part
        if b'\r\n\r\n' in buf or b'\n\n' in buf:
            break
    if not buf:
        return None, b''
    if b'\r\n\r\n' in buf:
        head, rest = buf.split(b'\r\n\r\n', 1)
    elif b'\n\n' in buf:
        head, rest = buf.split(b'\n\n', 1)
    else:
        head, rest = buf, b''
    try:
        head_text = head.decode('utf-8', errors='ignore')
    except Exception:
        head_text = head.decode('latin1', errors='ignore')
    return head_text, rest

class Session:
    def __init__(self, conn, addr):
        self.conn = conn
        self.addr = addr
        self.user = None
        self.role = None
        self.cwd = WWW_DIR
        self.history = []
        self.history_pos = -1
        self.last_active = time.time()
    def push_history(self, path):
        if not self.history or self.history[-1] != path:
            self.history.append(path)
        self.history_pos = len(self.history) - 1
    def back(self):
        if self.history_pos > 0:
            self.history_pos -= 1
            return self.history[self.history_pos]
        return None
    def home(self):
        if self.history:
            self.history_pos = 0
            return self.history[0]
        return None

def deliver_msg(to_user, from_user, text):
    sent_live = False
    with _clients_lock:
        conns = clients.get(to_user, [])
        for (c, a, s) in list(conns):
            try:
                payload = f"From: {from_user}\n\n{text}".encode('utf-8')
                respond(c, "RRTP/3.0 310 MESSAGE", {"Server": SERVER_NAME, "Content-Type": "text/plain; charset=utf-8"}, payload)
                sent_live = True
            except Exception:
                pass
    if not sent_live:
        fn = store_message(to_user, f"From: {from_user}\n\n{text}")
        return False, fn
    return True, None

peer_lock = threading.Lock()
peers = {}
seen_msgs = set()
config = {}

def load_node_config():
    global config
    default = {
        "node_id": str(uuid.uuid4()),
        "domain": "local.rrtp",
        "public_host": "127.0.0.1",
        "public_port": PORT,
        "peers": [],
        "gossip_ttl": 3
    }
    if os.path.exists('node_config.json'):
        try:
            with open('node_config.json', 'r', encoding='utf-8') as f:
                cfg = json.load(f)
        except Exception:
            cfg = default
    else:
        cfg = default
        try:
            with open('node_config.json', 'w', encoding='utf-8') as f:
                json.dump(cfg, f, indent=2)
        except Exception:
            pass
    config = cfg

def peer_address_str(host, port):
    return f"{host}:{port}"

def connect_to_peer(entry):
    host = entry.get('host')
    port = entry.get('port')
    addr = f"{host}:{port}"
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((host, int(port)))
        s.sendall(f"PEER {config.get('node_id')}\r\n\r\n".encode('utf-8'))
        with peer_lock:
            peers[addr] = {"host": host, "port": port, "conn": s, "last_seen": time.time(), "alive": True}
        return True
    except Exception:
        with peer_lock:
            if addr not in peers:
                peers[addr] = {"host": host, "port": port, "conn": None, "last_seen": 0, "alive": False}
        return False

def start_peer_connect_loop():
    def loop():
        while not shutdown_flag.is_set():
            plist = config.get('peers', [])
            for p in plist:
                if isinstance(p, str):
                    try:
                        host, pr = p.split(':')
                        entry = {"host": host, "port": int(pr)}
                    except Exception:
                        continue
                elif isinstance(p, dict):
                    entry = p
                else:
                    continue
                addr = f"{entry['host']}:{entry['port']}"
                with peer_lock:
                    info = peers.get(addr)
                    if info and info.get('alive'):
                        continue
                connect_to_peer(entry)
                time.sleep(0.05)
            time.sleep(5)
    t = threading.Thread(target=loop, daemon=True)
    t.start()

def forward_relay(msg_id, ttl, mtype, payload):
    line = f"RELAY {msg_id} {ttl} {mtype} {payload}\r\n\r\n".encode('utf-8')
    with peer_lock:
        items = list(peers.items())
    for addr, info in items:
        c = info.get('conn')
        if c:
            try:
                c.sendall(line)
                with peer_lock:
                    peers[addr]['last_seen'] = time.time()
                    peers[addr]['alive'] = True
            except Exception:
                with peer_lock:
                    peers[addr]['alive'] = False
        else:
            try:
                host = info.get('host')
                port = info.get('port')
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(5)
                s.connect((host, int(port)))
                s.sendall(line)
                s.close()
            except Exception:
                with peer_lock:
                    peers[addr]['alive'] = False

def is_local_domain(domain):
    return domain == config.get('domain')

def route_message_to_domain(to_user_with_domain, sender, text):
    if '@' in to_user_with_domain:
        local_user, domain = to_user_with_domain.split('@', 1)
    else:
        local_user, domain = to_user_with_domain, config.get('domain')
    if is_local_domain(domain):
        return deliver_msg(local_user, sender, text)
    msg_id = str(uuid.uuid4())
    payload = f"{to_user_with_domain}||{sender}||{text}"
    ttl = config.get('gossip_ttl', 3)
    forward_relay(msg_id, ttl, 'MSG', payload)
    return False, None

def handle_client(conn, addr):
    session = Session(conn, addr)
    connections.add(conn)
    log(f"Connection from {addr[0]}:{addr[1]}")
    try:
        respond(conn, f"RRTP/3.0 220 {SERVER_NAME}", {"Server": SERVER_NAME})
        first_peer_line = None
        while not shutdown_flag.is_set():
            head_text, rest_bytes = read_until_double_newline(conn, timeout=IDLE_TIMEOUT)
            if head_text is None:
                log(f"No data/connection closed from {addr[0]}:{addr[1]} — closing")
                break
            session.last_active = time.time()
            if not head_text.strip() and not rest_bytes:
                break
            safe_preview = head_text.replace('\r', '\\r').replace('\n', '\\n')
            log(f"Received from {addr[0]}:{addr[1]} -> {safe_preview}")
            lines = head_text.splitlines()
            if not lines:
                continue
            first = lines[0].strip()
            if not first:
                continue
            parts = first.split(maxsplit=2)
            cmd = parts[0].upper()
            arg = parts[1] if len(parts) > 1 else ""
            extra = parts[2] if len(parts) > 2 else None
            if cmd == 'PEER':
                peer_id = arg
                pstr = f"{addr[0]}:{addr[1]}"
                with peer_lock:
                    peers[pstr] = {"host": addr[0], "port": addr[1], "conn": conn, "last_seen": time.time(), "alive": True}
                respond(conn, "RRTP/3.0 200 PEER_OK")
                continue
            if cmd == 'RELAY':
                parts2 = first.split(None,4)
                if len(parts2) < 4:
                    respond(conn, "RRTP/3.0 400 BAD RELAY")
                    continue
                msg_id = parts2[1]
                try:
                    ttl = int(parts2[2])
                except:
                    ttl = 0
                mtype = parts2[3].upper() if len(parts2) > 3 else "MSG"
                payload = parts2[4] if len(parts2) > 4 else ''
                if msg_id in seen_msgs:
                    respond(conn, "RRTP/3.0 200 SEEN")
                    continue
                seen_msgs.add(msg_id)
                if mtype == 'MSG':
                    try:
                        to_user_with_domain, frm, text = payload.split("||",2)
                    except Exception:
                        to_user_with_domain, frm, text = payload, "unknown", ""
                    if '@' in to_user_with_domain:
                        local_user, domain = to_user_with_domain.split('@',1)
                    else:
                        local_user, domain = to_user_with_domain, config.get('domain')
                    if is_local_domain(domain):
                        deliver_msg(local_user, frm, text)
                    else:
                        if ttl > 0:
                            forward_relay(msg_id, ttl-1, 'MSG', payload)
                else:
                    if ttl > 0:
                        forward_relay(msg_id, ttl-1, mtype, payload)
                respond(conn, "RRTP/3.0 200 RELAY_OK")
                continue
            if cmd == 'USER':
                uname = arg
                if uname in users:
                    session.user = uname
                    session.role = users[uname].get('role', 'user')
                    with _clients_lock:
                        clients.setdefault(uname, []).append((conn, addr, session))
                    respond(conn, "RRTP/3.0 331 USER OK; need PASS", {"Server": SERVER_NAME})
                else:
                    session.user = None
                    session.role = None
                    respond(conn, "RRTP/3.0 430 UNKNOWN USER", {"Server": SERVER_NAME})
                continue
            if cmd == 'PASS':
                pwd = arg
                if session.user and users.get(session.user, {}).get('password') == pwd:
                    session.role = users[session.user].get('role', 'user')
                    respond(conn, "RRTP/3.0 230 LOGIN SUCCESS", {"Server": SERVER_NAME})
                else:
                    session.user = None
                    session.role = None
                    respond(conn, "RRTP/3.0 430 LOGIN FAILED", {"Server": SERVER_NAME})
                continue
            if cmd == 'LOGOUT':
                if session.user:
                    uname = session.user
                    session.user = None
                    session.role = None
                    with _clients_lock:
                        lst = clients.get(uname, [])
                        lst = [c for c in lst if c[0] != conn]
                        clients[uname] = lst
                    respond(conn, "RRTP/3.0 200 LOGOUT OK")
                else:
                    respond(conn, "RRTP/3.0 200 LOGOUT OK")
                continue
            if cmd == 'PING':
                respond(conn, "RRTP/3.0 200 PONG")
                continue
            if cmd == 'ECHO':
                txt = head_text.split(None,1)[1] if ' ' in head_text else ''
                respond(conn, "RRTP/3.0 200 OK", {"Content-Type":"text/plain; charset=utf-8"}, txt)
                continue
            if cmd == 'PWD':
                rp = relpath_from_root(session.cwd)
                respond(conn, "RRTP/3.0 257", {"Content-Type": "text/plain; charset=utf-8"}, rp)
                continue
            if cmd == 'CWD':
                target = arg or '/'
                newdir = normalize_path(session.cwd, target)
                if os.path.isdir(newdir):
                    session.cwd = newdir
                    rp = relpath_from_root(session.cwd)
                    respond(conn, "RRTP/3.0 250 CWD OK", {"Info": rp})
                else:
                    respond(conn, "RRTP/3.0 550 Directory not found")
                continue
            if cmd == 'LIST':
                target = arg or ''
                folder = normalize_path(session.cwd, target) if target else session.cwd
                if not os.path.isdir(folder):
                    respond(conn, "RRTP/3.0 550 Directory not found")
                    continue
                items = os.listdir(folder)
                body = ("\n".join(items)).encode('utf-8')
                respond(conn, "RRTP/3.0 200 OK", {"Content-Type": "text/plain; charset=utf-8"}, body)
                continue
            if cmd == 'MKDIR':
                if not arg:
                    respond(conn, "RRTP/3.0 400 BAD REQUEST")
                    continue
                dest = normalize_path(session.cwd, arg)
                try:
                    os.makedirs(dest, exist_ok=True)
                    respond(conn, "RRTP/3.0 250 MKDIR OK")
                except Exception as e:
                    respond(conn, "RRTP/3.0 550 MKDIR FAILED", {"Error": str(e)})
                continue
            if cmd == 'RMDIR':
                if not arg:
                    respond(conn, "RRTP/3.0 400 BAD REQUEST")
                    continue
                dest = normalize_path(session.cwd, arg)
                try:
                    os.rmdir(dest)
                    respond(conn, "RRTP/3.0 250 RMDIR OK")
                except Exception as e:
                    respond(conn, "RRTP/3.0 550 RMDIR FAILED", {"Error": str(e)})
                continue
            if cmd == 'INFO':
                if not arg:
                    respond(conn, "RRTP/3.0 400 BAD REQUEST")
                    continue
                fp = normalize_path(session.cwd, arg)
                if not os.path.exists(fp):
                    respond(conn, "RRTP/3.0 404 NOT FOUND")
                    continue
                stat = os.stat(fp)
                meta = {
                    "size": stat.st_size,
                    "last_modified": datetime.datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    "is_dir": os.path.isdir(fp)
                }
                body = json.dumps(meta).encode('utf-8')
                respond(conn, "RRTP/3.0 200 OK", {"Content-Type":"application/json; charset=utf-8"}, body)
                continue
            if cmd in ('GET', 'VIEW', 'DOWNLOAD', 'WEBGET'):
                if not arg:
                    respond(conn, "RRTP/3.0 400 BAD REQUEST")
                    continue
                fp = normalize_path(session.cwd, arg)
                if not os.path.exists(fp) or not os.path.isfile(fp):
                    respond(conn, "RRTP/3.0 404 NOT FOUND")
                    continue
                try:
                    size = os.path.getsize(fp)
                    ctype = 'text/rhtml; charset=utf-8' if fp.lower().endswith('.rhtml') else 'application/octet-stream'
                    headers = {"Content-Type": ctype, "Content-Length": str(size), "Server": SERVER_NAME}
                    head_lines = ["RRTP/3.0 200 OK"] + [f"{k}: {v}" for k, v in headers.items()]
                    head = ("\r\n".join(head_lines) + "\r\n\r\n").encode('utf-8')
                    if not safe_sendall(conn, head):
                        log(f"Client disconnected before GET header send: {addr[0]}:{addr[1]}")
                        continue
                    with open(fp, 'rb') as f:
                        while True:
                            chunk = f.read(RECV_CHUNK)
                            if not chunk:
                                break
                            if not safe_sendall(conn, chunk):
                                log(f"Client disconnected during GET: {addr[0]}:{addr[1]}")
                                break
                    try:
                        session.push_history(relpath_from_root(fp))
                    except Exception:
                        pass
                except Exception as e:
                    log(f"GET failed for {fp}: {e}")
                    respond(conn, "RRTP/3.0 550 READ FAILED", {"Content-Length": "0"})
                continue
            if cmd in ('PUT', 'UPLOAD'):
                if not arg:
                    respond(conn, "RRTP/3.0 400 BAD REQUEST")
                    continue
                remote = arg
                content_length = 0
                for ln in lines[1:]:
                    if ln.lower().startswith('content-length:'):
                        try:
                            content_length = int(ln.split(':',1)[1].strip())
                        except:
                            content_length = 0
                        break
                body = rest_bytes
                remaining = content_length - len(body)
                try:
                    conn.settimeout(10.0)
                    while remaining > 0:
                        chunk = conn.recv(min(RECV_CHUNK, remaining))
                        if not chunk:
                            break
                        body += chunk
                        remaining -= len(chunk)
                except Exception:
                    pass
                finally:
                    try:
                        conn.settimeout(None)
                    except Exception:
                        pass
                dest_fp = normalize_path(session.cwd, remote)
                os.makedirs(os.path.dirname(dest_fp), exist_ok=True)
                try:
                    with open(dest_fp, 'wb') as f:
                        if isinstance(body, bytes):
                            f.write(body[:content_length] if content_length else body)
                        else:
                            f.write((body or '').encode('utf-8'))
                    respond(conn, "RRTP/3.0 250 PUT OK", {"Path": relpath_from_root(dest_fp)})
                    try:
                        session.push_history(relpath_from_root(dest_fp))
                    except Exception:
                        pass
                except Exception as e:
                    respond(conn, "RRTP/3.0 550 PUT FAILED", {"Error": str(e)})
                continue
            if cmd in ('DEL', 'DELETE'):
                if not arg:
                    respond(conn, "RRTP/3.0 400 BAD REQUEST")
                    continue
                target = normalize_path(session.cwd, arg)
                if not os.path.exists(target):
                    respond(conn, "RRTP/3.0 404 NOT FOUND")
                    continue
                try:
                    if os.path.isdir(target):
                        respond(conn, "RRTP/3.0 550 NOT A FILE")
                    else:
                        os.remove(target)
                        respond(conn, "RRTP/3.0 250 DELETE OK")
                except Exception as e:
                    respond(conn, "RRTP/3.0 550 DELETE FAILED", {"Error": str(e)})
                continue
            if cmd in ('REN', 'RENAME'):
                sp = first.split(None, 2)
                if len(sp) < 3:
                    respond(conn, "RRTP/3.0 400 BAD REQUEST")
                    continue
                old = normalize_path(session.cwd, sp[1])
                new = normalize_path(session.cwd, sp[2])
                try:
                    os.makedirs(os.path.dirname(new), exist_ok=True)
                    os.rename(old, new)
                    respond(conn, "RRTP/3.0 250 RENAME OK")
                except Exception as e:
                    respond(conn, "RRTP/3.0 550 RENAME FAILED", {"Error": str(e)})
                continue
            if cmd == 'MOVE':
                sp = first.split(None, 2)
                if len(sp) < 3:
                    respond(conn, "RRTP/3.0 400 BAD REQUEST")
                    continue
                src = normalize_path(session.cwd, sp[1])
                dst = normalize_path(session.cwd, sp[2])
                try:
                    os.makedirs(os.path.dirname(dst), exist_ok=True)
                    shutil.move(src, dst)
                    respond(conn, "RRTP/3.0 250 MOVE OK")
                except Exception as e:
                    respond(conn, "RRTP/3.0 550 MOVE FAILED", {"Error": str(e)})
                continue
            if cmd == 'COPY':
                sp = first.split(None, 2)
                if len(sp) < 3:
                    respond(conn, "RRTP/3.0 400 BAD REQUEST")
                    continue
                src = normalize_path(session.cwd, sp[1])
                dst = normalize_path(session.cwd, sp[2])
                try:
                    os.makedirs(os.path.dirname(dst), exist_ok=True)
                    shutil.copy2(src, dst)
                    respond(conn, "RRTP/3.0 250 COPY OK")
                except Exception as e:
                    respond(conn, "RRTP/3.0 550 COPY FAILED", {"Error": str(e)})
                continue
            if cmd == 'MSG':
                subparts = first.split(None, 2)
                if len(subparts) < 2:
                    respond(conn, "RRTP/3.0 400 BAD REQUEST")
                    continue
                sub = subparts[1].upper()
                if sub == 'SEND':
                    rest = head_text.split(None, 3)
                    if len(rest) < 4:
                        respond(conn, "RRTP/3.0 400 BAD REQUEST")
                        continue
                    to_user = rest[2]
                    text = head_text.split(None, 3)[3].strip()
                    if text.startswith('"') and text.endswith('"'):
                        text = text[1:-1]
                    sender = session.user or f"{addr[0]}:{addr[1]}"
                    ok, fn = route_message_to_domain(to_user, sender, text)
                    if ok:
                        respond(conn, "RRTP/3.0 310 MESSAGE SENT")
                    else:
                        if fn:
                            respond(conn, "RRTP/3.0 310 MESSAGE STORED", {"File": fn})
                        else:
                            respond(conn, "RRTP/3.0 310 MESSAGE SENT")
                    continue
                elif sub == 'LIST':
                    if not session.user:
                        respond(conn, "RRTP/3.0 403 LOGIN REQUIRED")
                        continue
                    msgs = list_mail(session.user)
                    out = []
                    for fn in msgs[-20:]:
                        full = os.path.join(mailbox_path(session.user), fn)
                        try:
                            with open(full, 'r', encoding='utf-8') as f:
                                hdr = f.readline().strip()
                                date = f.readline().strip()
                                out.append(f"{fn} | {hdr} | {date}")
                        except Exception:
                            out.append(fn)
                    body = ("\n".join(out)).encode('utf-8')
                    respond(conn, "RRTP/3.0 200 OK", {"Content-Type":"text/plain; charset=utf-8"}, body)
                    continue
                else:
                    respond(conn, "RRTP/3.0 400 BAD REQUEST")
                    continue
            if cmd == 'WHO':
                with _clients_lock:
                    names = list(clients.keys())
                body = ("\n".join(names)).encode('utf-8')
                respond(conn, "RRTP/3.0 200 OK", {"Content-Type": "text/plain; charset=utf-8"}, body)
                continue
            if cmd == 'SYSINFO':
                info = {
                    "server": SERVER_NAME,
                    "host": HOST,
                    "port": PORT,
                    "time": datetime.datetime.now().isoformat(),
                    "python": platform.python_version(),
                    "platform": platform.platform(),
                    "node_id": config.get('node_id'),
                    "domain": config.get('domain')
                }
                body = json.dumps(info).encode('utf-8')
                respond(conn, "RRTP/3.0 200 OK", {"Content-Type":"application/json; charset=utf-8"}, body)
                continue
            if cmd == 'USERS':
                if session.role != 'admin':
                    respond(conn, "RRTP/3.0 403 FORBIDDEN")
                    continue
                try:
                    with open(USERS_FILE, 'r', encoding='utf-8') as f:
                        body = f.read().encode('utf-8')
                except Exception:
                    body = json.dumps(users).encode('utf-8')
                respond(conn, "RRTP/3.0 200 OK", {"Content-Type":"application/json; charset=utf-8"}, body)
                continue
            if cmd == 'SHUTDOWN':
                if session.role != 'admin':
                    respond(conn, "RRTP/3.0 403 FORBIDDEN")
                    continue
                respond(conn, "RRTP/3.0 200 OK", {"Info":"Server shutting down"})
                log("Admin requested shutdown.")
                shutdown_flag.set()
                break
            if cmd == 'LOGS':
                try:
                    n = int(arg) if arg else 50
                except Exception:
                    n = 50
                if os.path.exists(LOG_FILE):
                    with open(LOG_FILE, 'r', encoding='utf-8') as f:
                        lines = f.readlines()[-n:]
                    body = ''.join(lines).encode('utf-8')
                else:
                    body = b''
                respond(conn, "RRTP/3.0 200 OK", {"Content-Type": "text/plain; charset=utf-8"}, body)
                continue
            if cmd == 'BROADCAST':
                if session.role != 'admin':
                    respond(conn, "RRTP/3.0 403 FORBIDDEN")
                    continue
                text = head_text.split(None, 1)[1] if ' ' in head_text else ''
                sent = 0
                with _clients_lock:
                    for uname, conns in clients.items():
                        for (c, a, s) in conns:
                            try:
                                respond(c, "RRTP/3.0 300 BROADCAST", {"Server": SERVER_NAME, "Content-Type":"text/plain; charset=utf-8"}, text)
                                sent += 1
                            except:
                                pass
                respond(conn, f"RRTP/3.0 200 OK", {"Info": f"Sent to {sent} sessions"})
                continue
            if cmd == 'KICK':
                if session.role != 'admin':
                    respond(conn, "RRTP/3.0 403 FORBIDDEN")
                    continue
                target = arg
                kicked = 0
                with _clients_lock:
                    conns = clients.get(target, [])
                    for (c, a, s) in list(conns):
                        try:
                            respond(c, "RRTP/3.0 421 KICKED")
                            c.close()
                            kicked += 1
                        except:
                            pass
                    clients[target] = []
                respond(conn, f"RRTP/3.0 200 OK", {"Info": f"Kicked {kicked}"})
                continue
            respond(conn, "RRTP/3.0 500 UNKNOWN COMMAND")
    except Exception as e:
        log(f"Error handling {addr}: {e}\n{traceback.format_exc()}")
    finally:
        try:
            conn.close()
        except:
            pass
        connections.discard(conn)
        with _clients_lock:
            if session.user:
                lst = clients.get(session.user, [])
                lst = [c for c in lst if c[0] != conn]
                clients[session.user] = lst
        log(f"Connection closed {addr[0]}:{addr[1]}")

def peer_cleanup_loop():
    def loop():
        while not shutdown_flag.is_set():
            with peer_lock:
                now = time.time()
                for addr, info in list(peers.items()):
                    last = info.get('last_seen', 0)
                    if info.get('conn') and now - last > 120:
                        try:
                            c = info.get('conn')
                            c.close()
                        except:
                            pass
                        peers[addr]['conn'] = None
                        peers[addr]['alive'] = False
            time.sleep(30)
    t = threading.Thread(target=loop, daemon=True)
    t.start()

def start_server():
    load_node_config()
    start_peer_connect_loop()
    peer_cleanup_loop()
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen(64)
    log(f"{SERVER_NAME} listening on {HOST}:{PORT} domain={config.get('domain')} node_id={config.get('node_id')}")
    threads = []
    try:
        while not shutdown_flag.is_set():
            try:
                srv.settimeout(1.0)
                conn, addr = srv.accept()
            except socket.timeout:
                continue
            t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            t.start()
            threads.append(t)
    except KeyboardInterrupt:
        log("KeyboardInterrupt, shutting down.")
    finally:
        shutdown_flag.set()
        srv.close()
        for t in threads:
            t.join(timeout=0.1)
        log("Server stopped.")

if __name__ == '__main__':
    start_server()
