import socket
import threading
import queue
import time
import os
import sys
import random
import json

try:
    from playsound import playsound
except Exception:
    playsound = None

ENV_HOST = os.environ.get('RRTP_HOST', '167.17.181.201')
ENV_PORT = int(os.environ.get('RRTP_PORT', '8080'))

HOST = ENV_HOST
PORT = ENV_PORT

HOSTS_FILE = 'hosts.json'        # format: {"east.rrtp": "1.2.3.4:8080", "core.rrtp": "5.6.7.8:8080"}
BOOTSTRAP_FILE = 'bootstrap.json'  # format: ["core.rrtp:8080", "node2.rrtp:8080"]

DIALUP_BPS = 28800
BYTES_PER_SEC = max(1, DIALUP_BPS // 8)
JITTER = 0.05
CHUNK_SIZE = 4096

MODEM_SOUND_FILE = 'modem_sound.wav'

RECV_TIMEOUT = 10.0
HEADER_POLL_TIMEOUT = 0.1

SMALL_PAYLOAD_FAST_SEND = 2048
SMALL_FILE_NO_ANIM = 4096

response_q = queue.Queue(maxsize=64)
stop_event = threading.Event()
busy_event = threading.Event()

lock_print = threading.Lock()
def safe_print(*a, **kw):
    with lock_print:
        print(*a, **kw)

# ----- helpers for hosts/bootstrap -----
def load_hosts_map():
    try:
        if os.path.exists(HOSTS_FILE):
            with open(HOSTS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def load_bootstrap_list():
    try:
        if os.path.exists(BOOTSTRAP_FILE):
            with open(BOOTSTRAP_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception:
        pass
    return []

def parse_hostport(s, default_port=PORT):
    if ':' in s:
        h, p = s.rsplit(':', 1)
        try:
            return h, int(p)
        except Exception:
            return s, default_port
    return s, default_port

def connect_raw(host_or_ip, port, timeout=5):

    try:
        ip = host_or_ip
     
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((ip, port))
        s.settimeout(None)
        return s, ip, port
    except Exception as e:
        return None, None, None

def connect_with_fallback(target_host, target_port, bootstrap_list=None, timeout=5):
  
    hosts_map = load_hosts_map()

 
    mapped = hosts_map.get(target_host)
    if mapped:
        h, p = parse_hostport(mapped, target_port)
        s, used_ip, used_port = connect_raw(h, p, timeout=timeout)
        if s:
            safe_print(f"Connected to {target_host} via hosts.json -> {h} ({used_ip}):{used_port}")
            return s, used_ip or h, used_port or p
        else:
           
            safe_print(f"hosts.json mapping {mapped} for {target_host} failed to connect.")


    try:
        resolved_ip = socket.gethostbyname(target_host)
        s, used_ip, used_port = connect_raw(resolved_ip, target_port, timeout=timeout)
        if s:
            safe_print(f"Connected to {target_host} ({resolved_ip}):{target_port}")
            return s, used_ip or resolved_ip, used_port or target_port
        else:
            safe_print(f"DNS-resolved connect to {target_host} ({resolved_ip}) failed.")
    except Exception:
 
        safe_print(f"DNS resolution for {target_host} failed (will try bootstrap if available).")

    bl = bootstrap_list or load_bootstrap_list()
    if bl:
        safe_print("Trying bootstrap list...")
        for entry in bl:
            h, p = parse_hostport(entry, target_port)
        
            try:
                ip_try = socket.gethostbyname(h)
            except Exception:
                ip_try = h 
            s, used_ip, used_port = connect_raw(ip_try, p, timeout=timeout)
            if s:
                safe_print(f"Connected to bootstrap {entry} -> {used_ip}:{used_port}")
                return s, used_ip or ip_try, used_port or p
            else:
 
                safe_print(f"Bootstrap entry {entry} connection failed.")

    safe_print(f"All connection attempts failed for {target_host}:{target_port}.")
    return None, None, None


def read_response(sock, expect_body=True, save_to=None, emulate_rx=False, timeout=None):
    if timeout is None:
        timeout = HEADER_POLL_TIMEOUT
    try:
        sock.settimeout(timeout)
    except Exception:
        pass
    buf = b''
    try:
        while True:
            try:
                part = sock.recv(8192)
            except socket.timeout:
                break
            except Exception:
                return None, {}, b''
            if not part:
                break
            buf += part
            if b'\r\n\r\n' in buf or b'\n\n' in buf:
                break
    finally:
        try:
            sock.settimeout(None)
        except Exception:
            pass
    if not buf:
        return None, {}, b''
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
    lines = [ln for ln in head_text.splitlines() if ln.strip()!='']
    if not lines:
        return None, {}, b''
    status = lines[0]
    headers = {}
    for ln in lines[1:]:
        if ':' in ln:
            k, v = ln.split(':', 1)
            headers[k.strip().lower()] = v.strip()
    try:
        content_length = int(headers.get('content-length', '0') or 0)
    except Exception:
        content_length = 0
    body = rest
    if expect_body and content_length > 0:
        remaining = content_length - len(body)
        try:
            sock.settimeout(RECV_TIMEOUT)
        except Exception:
            pass
        if save_to:
            try:
                with open(save_to, 'wb') as f:
                    if body:
                        f.write(body)
                        if emulate_rx:
                            time.sleep(max(0, len(body) / BYTES_PER_SEC + random.uniform(-JITTER, JITTER)))
                    while remaining > 0:
                        try:
                            chunk = sock.recv(min(CHUNK_SIZE, remaining))
                        except socket.timeout:
                            continue
                        if not chunk:
                            break
                        f.write(chunk)
                        remaining -= len(chunk)
                        if emulate_rx:
                            time.sleep(max(0, len(chunk) / BYTES_PER_SEC + random.uniform(-JITTER, JITTER)))
                return status, headers, None
            finally:
                try:
                    sock.settimeout(None)
                except Exception:
                    pass
        else:
            try:
                while remaining > 0:
                    try:
                        part = sock.recv(min(CHUNK_SIZE, remaining))
                    except socket.timeout:
                        continue
                    if not part:
                        break
                    body += part
                    remaining -= len(part)
                    if emulate_rx:
                        time.sleep(max(0, len(part) / BYTES_PER_SEC + random.uniform(-JITTER, JITTER)))
            finally:
                try:
                    sock.settimeout(None)
                except Exception:
                    pass
    return status, headers, body

# helper: сохранить пришедший hosts.json и перезагрузить кэш
def save_hosts_from_server(body_bytes):
    try:
        if not body_bytes:
            return False
        with open(HOSTS_FILE, 'wb') as f:
            f.write(body_bytes)
        safe_print(f"[hosts] saved {HOSTS_FILE} (updated from server)")
        return True
    except Exception as e:
        safe_print("[hosts] failed to save hosts.json:", e)
        return False


def receiver_loop(sock):
    while not stop_event.is_set():
        if busy_event.is_set():
            time.sleep(0.02)
            continue
        status, headers, body = read_response(sock, expect_body=True, save_to=None, emulate_rx=False, timeout=HEADER_POLL_TIMEOUT)
        if status is None:
            time.sleep(0.1)
            continue

        if status.startswith("RRTP/3.0 260"):
            saved = save_hosts_from_server(body or b'')
      
            continue

        if status.startswith("RRTP/3.0 310") or status.startswith("RRTP/3.0 300") or status.startswith("RRTP/3.0 421") or status.startswith("RRTP/3.0 220"):
            safe_print()
            safe_print("-" * 40)
            safe_print("[PUSH] " + status)
            if headers:
                for k, v in headers.items():
                    safe_print(f"{k}: {v}")
            if body:
                try:
                    safe_print(body.decode('utf-8', errors='ignore'))
                except Exception:
                    safe_print("<binary data>")
            safe_print("-" * 40)
            safe_print()
        else:
            try:
                response_q.put((status, headers, body), timeout=0.1)
            except queue.Full:
                pass


class ModemAnimator(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self._active = threading.Event()
        self._stop = threading.Event()
        self._one_shot = threading.Event()
        self._patterns = [
            "[       ] dial...", "[=      ] dialing", "[==     ] handshake", "[===    ] hand.",
            "[ ====  ] sync", "[  ==== ] sync.", "[   === ] online", "[    == ] connected",
            "[     = ] transfer", "[      ] idle"
        ]
        self._idx = 0
        self.start()
    def start_anim(self):
        self._one_shot.clear()
        self._active.set()
        if playsound and os.path.exists(MODEM_SOUND_FILE):
            threading.Thread(target=lambda: playsound(MODEM_SOUND_FILE, block=True), daemon=True).start()
    def start_anim_once(self):
        self._idx = 0
        self._one_shot.set()
        self._active.set()
        if playsound and os.path.exists(MODEM_SOUND_FILE):
            threading.Thread(target=lambda: playsound(MODEM_SOUND_FILE, block=True), daemon=True).start()
    def stop_anim(self):
        self._active.clear()
    def stop_all(self):
        self._stop.set()
        self._active.clear()
        self._one_shot.clear()
    def run(self):
        while not self._stop.is_set():
            if self._active.is_set():
                txt = self._patterns[self._idx % len(self._patterns)]
                with lock_print:
                    sys.stdout.write('\r' + txt + '    ')
                    sys.stdout.flush()
                self._idx += 1
                time.sleep(0.25)
                if self._one_shot.is_set() and self._idx >= len(self._patterns):
                    self._one_shot.clear()
                    self._active.clear()
                    with lock_print:
                        sys.stdout.write('\r' + ' ' * 60 + '\r')
                        sys.stdout.flush()
            else:
                time.sleep(0.05)

def send_throttled(sock, data_bytes, emulate_modem=False):
    total = len(data_bytes)
    if not emulate_modem or total <= SMALL_PAYLOAD_FAST_SEND:
        try:
            sock.sendall(data_bytes)
            return
        except Exception:
            raise
    sent = 0
    while sent < total:
        tosend = data_bytes[sent:sent+CHUNK_SIZE]
        try:
            sock.sendall(tosend)
        except Exception:
            raise
        sent += len(tosend)
        sleep_time = max(0, len(tosend) / BYTES_PER_SEC + random.uniform(-JITTER, JITTER))
        time.sleep(sleep_time)

def print_progress_bar(stage, received, total, bar_len=30):
    if total:
        percent = received / total
        filled = int(bar_len * percent)
        bar = '=' * filled + ' ' * (bar_len - filled)
        with lock_print:
            sys.stdout.write(f'\r[{bar}] {percent*100:6.2f}% ({received}/{total} bytes) {stage}')
            sys.stdout.flush()
    else:
        with lock_print:
            sys.stdout.write(f'\r[{"?"*bar_len}] {received} bytes {stage}')
            sys.stdout.flush()

def send_command(sock, cmd_line, body_bytes=b'', expect_body=True, save_to=None, show_anim=False, read_timeout=None):
    if not cmd_line.endswith("\r\n\r\n"):
        cmd_line = cmd_line.rstrip('\r\n') + "\r\n\r\n"
    payload = cmd_line.encode('utf-8') + (body_bytes or b'')
    large_tx = bool(save_to) or len(payload) > SMALL_PAYLOAD_FAST_SEND or show_anim
    animator = None
    try:
        busy_event.set()
        animator = getattr(send_command, "_animator", None)
        if animator is None:
            animator = ModemAnimator()
            send_command._animator = animator
        emulate_modem_for_send = large_tx and bool(save_to or show_anim)
        send_throttled(sock, payload, emulate_modem=emulate_modem_for_send)
        if large_tx:
            time.sleep(0.02)
        hdr_rt = (read_timeout if read_timeout is not None else 2.0)
        status, headers, rest = read_response(sock, expect_body=False, save_to=None, emulate_rx=False, timeout=hdr_rt)
        if status is None:
            return None, {}, b''
        if not save_to or not expect_body:
            try:
                content_length = int(headers.get('content-length', '0') or 0)
            except Exception:
                content_length = 0
            if content_length > 0:
                remaining = content_length - len(rest)
                body = rest
                try:
                    sock.settimeout(RECV_TIMEOUT)
                except Exception:
                    pass
                try:
                    while remaining > 0:
                        try:
                            part = sock.recv(min(CHUNK_SIZE, remaining))
                        except socket.timeout:
                            continue
                        if not part:
                            break
                        body += part
                        remaining -= len(part)
                finally:
                    try:
                        sock.settimeout(None)
                    except Exception:
                        pass
                return status, headers, body
            else:
                return status, headers, rest
        try:
            content_length = int(headers.get('content-length', '0') or 0)
        except Exception:
            content_length = 0
        small_file = 0 < content_length <= SMALL_FILE_NO_ANIM
        if show_anim:
            animator.start_anim_once()
        received = 0
        try:
            sock.settimeout(RECV_TIMEOUT)
        except Exception:
            pass
        try:
            with open(save_to, 'wb') as f:
                if rest:
                    f.write(rest)
                    received += len(rest)
                    print_progress_bar("transfer", received, content_length)
                while content_length == 0 or received < content_length:
                    try:
                        torecv = CHUNK_SIZE
                        if content_length:
                            torecv = min(CHUNK_SIZE, content_length - received)
                        chunk = sock.recv(torecv)
                    except socket.timeout:
                        continue
                    except Exception:
                        break
                    if not chunk:
                        break
                    f.write(chunk)
                    received += len(chunk)
                    print_progress_bar("transfer", received, content_length)
                print_progress_bar("idle", received, content_length)
                with lock_print:
                    sys.stdout.write('\n')
                    sys.stdout.flush()
        finally:
            try:
                sock.settimeout(None)
            except Exception:
                pass
        return status, headers, None
    finally:
        if animator and animator._active.is_set():
            animator.stop_anim()
            time.sleep(0.05)
        if large_tx:
            busy_event.clear()

def do_auth(sock, username, password):
    st, hd, bd = send_command(sock, f"USER {username}", expect_body=True, show_anim=False, read_timeout=1.0)
    pretty_print_response((st, hd, bd))
    time.sleep(0.05)
    st, hd, bd = send_command(sock, f"PASS {password}", expect_body=True, show_anim=False, read_timeout=RECV_TIMEOUT)
    pretty_print_response((st, hd, bd))
    return st, hd, bd

def pretty_print_response(rv, auto_save_hint=False):
    if rv is None:
        safe_print("[no response]")
        return
    status, headers, body = rv
    safe_print("=" * 60)
    safe_print(status if status else "<no status>")
    if headers:
        for k, v in headers.items():
            safe_print(f"{k}: {v}")
    if body:
        ctype = headers.get('content-type', '').lower() if headers else ''
        try:
            clen = int(headers.get('content-length', '0') or 0)
        except Exception:
            clen = len(body)
        if ctype.startswith('application/octet') or clen > 1024 * 20:
            safe_print(f"(binary body {len(body)} bytes)")
        else:
            try:
                safe_print(body.decode('utf-8', errors='ignore'))
            except Exception:
                safe_print("<binary>")
    safe_print("=" * 60)

def repl(sock):
    global HOST, PORT
    try:
        peer = sock.getpeername()
        safe_print("RRTP Internet-ish Client")
        safe_print(f"Connected to {peer[0]}:{peer[1]}")
    except Exception:
        safe_print("RRTP Internet-ish Client")
        safe_print(f"Connected to {HOST}:{PORT}")
    safe_print("Type HELP for quick tips.")
    while True:
        try:
            cmd = input("rrtp> ").strip()
        except (EOFError, KeyboardInterrupt):
            safe_print("\nExiting...")
            break
        if not cmd:
            continue
        up = cmd.split(None, 1)[0].upper()
        if up in ("QUIT", "EXIT"):
            try:
                send_throttled(sock, (cmd.rstrip() + "\r\n\r\n").encode('utf-8'), emulate_modem=False)
            except Exception:
                pass
            break  
        if up == "HOSTS":
            rv = send_command(sock, "HOSTS", body_bytes=b'', expect_body=True, save_to=None, show_anim=False, read_timeout=2.0)
            pretty_print_response(rv)
            # Если сервер поддерживает 260, receiver_loop его сохранит автоматически.
            continue

        if up == "HELP":
            safe_print("CONNECT host[:port]  | AUTH user pass  | GET remote [local]  | PUT local [remote]  | MSG SEND user@domain \"hi\"")
            safe_print("Use hosts.json for domain->ip:port mapping and bootstrap.json for fallback servers.")
            continue
        if up == "CONNECT":
            parts = cmd.split(None,1)
            if len(parts) < 2:
                safe_print("Usage: CONNECT <host[:port]>")
                continue
            target = parts[1].strip()
            h, p = parse_hostport(target, PORT)
            try:
                s, used_host, used_port = connect_with_fallback(h, p)
                if s:
                    try:
                        peer = s.getpeername()
                        HOST, PORT = peer[0], peer[1]
                    except Exception:
                        HOST, PORT = used_host or h, used_port or p
                    safe_print(f"Rebooting local receiver on new socket... (connected to {HOST}:{PORT})")
                    try:
                        sock.close()
                    except Exception:
                        pass
                    return s
                else:
                    safe_print("Connect failed.")
            except Exception as e:
                safe_print("Connect error:", e)
            continue
        if up == "AUTH":
            parts = cmd.split(None, 2)
            if len(parts) < 3:
                safe_print("Usage: AUTH <user> <pass>")
                continue
            do_auth(sock, parts[1], parts[2])
            continue
        if up in ("USER", "PASS", "WHO", "LIST", "PWD", "CWD", "META", "LOGS", "STATUS", "PING", "VIEW", "INFO"):
            rv = send_command(sock, cmd, body_bytes=b'', expect_body=True, save_to=None, show_anim=False, read_timeout=2.0)
            pretty_print_response(rv)
            continue
        if up == "GET":
            parts = cmd.split(None, 2)
            if len(parts) < 2:
                safe_print("Usage: GET <remote> [localname]")
                continue
            remote = parts[1]
            local = parts[2] if len(parts) >= 3 else (os.path.basename(remote) or "download.bin")
            hdr = f"GET {remote}"
            rv = send_command(sock, hdr, body_bytes=b'', expect_body=True, save_to=local, show_anim=True, read_timeout=None)
            if rv:
                st, hd, bd = rv
                safe_print(f"Server: {st}")
                if st and st.startswith("RRTP/3.0 200"):
                    safe_print(f"Saved to: {local}")
                else:
                    pretty_print_response(rv)
            continue
        if up == "PUT":
            parts = cmd.split(None, 2)
            if len(parts) < 2:
                safe_print("Usage: PUT <localpath> [remote]")
                continue
            local_path = parts[1]
            if not os.path.exists(local_path):
                safe_print("Local file not found:", local_path)
                continue
            remote_path = parts[2] if len(parts) >= 3 else os.path.basename(local_path)
            try:
                with open(local_path, 'rb') as f:
                    data = f.read()
            except Exception as e:
                safe_print("Read error:", e)
                continue
            hdr = f"PUT {remote_path}\r\nContent-Length: {len(data)}"
            safe_print(f"Uploading {local_path} -> {remote_path} ({len(data)} bytes)...")
            rv = send_command(sock, hdr, body_bytes=data, expect_body=True, save_to=None, show_anim=True, read_timeout=None)
            pretty_print_response(rv)
            continue
        if cmd.upper().startswith("MSG "):
            rv = send_command(sock, cmd, body_bytes=b'', expect_body=True, save_to=None, show_anim=False, read_timeout=2.0)
            pretty_print_response(rv)
            continue
        rv = send_command(sock, cmd, body_bytes=b'', expect_body=True, save_to=None, show_anim=False, read_timeout=2.0)
        pretty_print_response(rv)
    stop_event.set()

def main():
    global HOST, PORT
    bootstrap = load_bootstrap_list()
    h, p = parse_hostport(HOST, PORT)
    sock = None
    try:
        sock, used_h, used_p = connect_with_fallback(h, p, bootstrap_list=bootstrap)
        if sock is None:
            safe_print("Initial connect failed. Trying bootstrap list only...")
            for entry in bootstrap:
                he, pe = parse_hostport(entry, PORT)
                sock, used_h, used_p = connect_with_fallback(he, pe, bootstrap_list=bootstrap)
                if sock:
                    break
        if sock is None:
            safe_print("Could not connect to any server. Check hosts.json / bootstrap.json or RRTP_HOST.")
            return
        try:
            peer = sock.getpeername()
            HOST, PORT = peer[0], peer[1]
        except Exception:
            if used_h:
                HOST, PORT = used_h, used_p or p
    except Exception as e:
        safe_print("Connection failed:", e)
        return
    t = threading.Thread(target=receiver_loop, args=(sock,), daemon=True)
    t.start()
    try:
        new_sock = repl(sock)
        while new_sock:
            sock = new_sock
            stop_event.clear()
            t = threading.Thread(target=receiver_loop, args=(sock,), daemon=True)
            t.start()
            new_sock = repl(sock)
    finally:
        try:
            sock.close()
        except Exception:
            pass
        stop_event.set()
        time.sleep(0.2)
        safe_print("Goodbye.")

if __name__ == "__main__":
    main()
