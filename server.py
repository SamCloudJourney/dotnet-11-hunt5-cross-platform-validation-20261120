import base64
import hashlib
import json
import socket
import struct
import time


def read_exact(conn, count):
    data = bytearray()
    while len(data) < count:
        chunk = conn.recv(count - len(data))
        if not chunk:
            raise EOFError("connection ended")
        data.extend(chunk)
    return bytes(data)


def one_connection(listener):
    conn, _ = listener.accept()
    conn.settimeout(40)
    conn.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    with conn:
        request = bytearray()
        while b"\r\n\r\n" not in request:
            request.extend(conn.recv(4096))
            if len(request) > 8192:
                raise ValueError("handshake too large")
        head = request.decode("ascii")
        path = head.split(" ", 2)[1]
        headers = {}
        for line in head.split("\r\n")[1:]:
            if ":" in line:
                key, value = line.split(":", 1)
                headers[key.lower()] = value.strip()
        key = headers["sec-websocket-key"]
        accept = base64.b64encode(hashlib.sha1(
            (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")
        ).digest()).decode("ascii")
        conn.sendall(("HTTP/1.1 101 Switching Protocols\r\n"
                      "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                      f"Sec-WebSocket-Accept: {accept}\r\n\r\n").encode("ascii"))

        if path.startswith("/pressure"):
            time.sleep(1)
            total = 0
            frames = 0
            while total < 2097152:
                opcode, _, payload = read_frame(conn)
                if opcode not in (0, 2) or not payload:
                    raise ValueError("unexpected inert data frame")
                total += len(payload)
                frames += 1
                if total > 2097152:
                    raise ValueError("inert data exceeded bound")
                time.sleep(0.05)
            print(json.dumps({"path": path, "data_bytes": total,
                              "data_frames": frames}), flush=True)

        opcode, _, payload = read_frame(conn)
        if opcode != 8:
            raise ValueError("expected close frame")
        status = struct.unpack("!H", payload[:2])[0]
        reason = payload[2:].decode("ascii")
        print(json.dumps({"path": path, "status": status,
                          "reason": reason}), flush=True)
        try:
            conn.sendall(b"\x88\x02\x03\xe8")
        except (BrokenPipeError, ConnectionResetError):
            pass


def read_frame(conn):
    first, second = read_exact(conn, 2)
    length = second & 0x7F
    if length == 126:
        length = struct.unpack("!H", read_exact(conn, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", read_exact(conn, 8))[0]
    if length > 65536:
        raise ValueError("frame exceeds inert bound")
    mask = read_exact(conn, 4) if second & 0x80 else b"\0" * 4
    payload = read_exact(conn, length)
    payload = bytes(value ^ mask[i % 4] for i, value in enumerate(payload))
    return first & 0x0F, first & 0x80, payload


with socket.socket() as listener:
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 18765))
    listener.listen(3)
    listener.settimeout(60)
    print("READY", flush=True)
    one_connection(listener)
    one_connection(listener)
    one_connection(listener)
