import json
import socket
import struct

def send_msg(sock, msg):
    """
    Sends a JSON message over a socket, prefixed with a 4-byte length header.
    """
    try:
        data = json.dumps(msg).encode('utf-8')
        length = len(data)
        # Pack length as a 4-byte big-endian unsigned integer
        header = struct.pack('!I', length)
        sock.sendall(header + data)
        return True
    except (socket.error, BrokenPipeError, ConnectionResetError):
        return False

def recv_msg(sock):
    """
    Receives a JSON message from a socket, prefixed with a 4-byte length header.
    Returns None if the connection is closed or an error occurs.
    """
    try:
        header = recv_all(sock, 4)
        if not header:
            return None
        length = struct.unpack('!I', header)[0]
        data = recv_all(sock, length)
        if not data:
            return None
        return json.loads(data.decode('utf-8'))
    except (socket.error, ConnectionResetError, json.JSONDecodeError):
        return None

def recv_all(sock, length):
    """
    Helper to receive exactly 'length' bytes from a socket.
    Returns None if EOF is reached before the requested size.
    """
    data = bytearray()
    while len(data) < length:
        packet = sock.recv(length - len(data))
        if not packet:
            return None
        data.extend(packet)
    return bytes(data)
