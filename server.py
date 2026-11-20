import base64
import hashlib
import json
import socket
import struct


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
    conn.settimeout(10)
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

        first, second = read_exact(conn, 2)
        length = second & 0x7F
        if length == 126:
            length = struct.unpack("!H", read_exact(conn, 2))[0]
        if length > 125:
            raise ValueError("unexpected frame length")
        mask = read_exact(conn, 4) if second & 0x80 else b"\0" * 4
        payload = read_exact(conn, length)
        payload = bytes(value ^ mask[i % 4] for i, value in enumerate(payload))
        if first & 0x0F != 8:
            raise ValueError("expected close frame")
        status = struct.unpack("!H", payload[:2])[0]
        reason = payload[2:].decode("ascii")
        print(json.dumps({"path": path, "status": status,
                          "reason": reason}), flush=True)
        conn.sendall(b"\x88\x02\x03\xe8")


with socket.socket() as listener:
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 18765))
    listener.listen(2)
    listener.settimeout(20)
    print("READY", flush=True)
    one_connection(listener)
    one_connection(listener)
