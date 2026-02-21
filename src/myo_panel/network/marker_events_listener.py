import errno
import socket
import struct
import threading
from typing import Any, Callable


class MarkEventsListener:
    """
    Listen on UDP for marker events.
    Markers are 4-byte float values in network byte order.
    The message_handler function is invoked with the float value as the only argument
    on every valid received message.
    """

    # for handling the network clients that send packets larger than the
    # expected message size and will raise an exception on Windows, set a larger
    # receive buffer
    _RCV_BUFFER_SIZE = 1024

    def __init__(self, listening_address="", listening_port=12390):
        self._address: str = listening_address
        self._port: int = listening_port
        self._socket: socket.socket | None = None
        self._listener_thread: threading.Thread | None = None
        self._stop_event: threading.Event = threading.Event()
        self._msg_handler: Callable[[float], Any] | None = None
        self._start_lock = threading.Lock()

    def _mark_event_listener(self):
        try:
            while not self._stop_event.is_set():
                try:
                    msg, addr = self._socket.recvfrom(self._RCV_BUFFER_SIZE)
                    if len(msg) != 4:
                        print(
                            f"[MarkEventsListener] Invalid message, expected 4 bytes for the "
                            f"mark_id: {msg}"
                        )
                        continue

                    if self._msg_handler is not None:
                        mark_id = struct.unpack("!f", msg)[0]
                        self._msg_handler(float(mark_id))
                    else:
                        print("[MarkEventsListener] UDP marker event received but there is no "
                              "message handler")

                except socket.timeout:
                    continue
                except socket.error as e:
                    if (e.errno == errno.EAGAIN
                            or (hasattr(errno, "WSAEMSGSIZE") and e.errno == errno.WSAEMSGSIZE)):
                        continue
                    else:
                        print(
                            f"[MarkEventsListener] Error receiving UDP data, stopping the mark "
                            f"event listener: {e}"
                        )
                        break
                except Exception as e:
                    print(f"[MarkEventsListener] Error handling the UDP message: {e}")
        finally:
            if self._socket is not None:
                self._socket.close()
            print("[MarkEventsListener] UDP listener thread stopped")

    def start(self, message_handler: Callable[[float], Any]) -> bool:
        with self._start_lock:
            if self.is_started():
                print("[MarkEventsListener] UDP mark event listener already started")
                return False

            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.bind((self._address, self._port))
                sock.settimeout(1.0)
                self._socket = sock
                self._msg_handler = message_handler

                self._stop_event.clear()
                self._listener_thread = threading.Thread(
                    target=self._mark_event_listener, daemon=True
                )
                self._listener_thread.start()
                print(
                    f"[MarkEventsListener] UDP mark event server listening on interface "
                    f"'{self._address}':{str(self._port)}"
                )
                return True
            except Exception as e:
                # in case bind succeeds but starting the thread fails
                if self._socket is not None:
                    self._socket.close()
                    self._socket = None
                self._msg_handler = None
                print(f"[MarkEventsListener] FAILED to start the UDP listener thread: {e}")
                return False


    def stop(self):
        with self._start_lock:
            self._stop_event.set()
            if self._listener_thread is not None:
                print(f"[MarkEventsListener] Stopping the UDP listener thread...")
                self._listener_thread.join()
                self._listener_thread = None
            self._msg_handler = None

    def is_started(self):
        return self._listener_thread is not None and self._listener_thread.is_alive()
