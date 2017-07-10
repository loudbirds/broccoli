"""
import zmq

context = zmq.Context()
socket = context.socket(zmq.SUB)
# We can connect to several endpoints if we desire, and receive from all.
socket.bind("tcp://*:4500")

# We must declare the socket as of type SUBSCRIBER, and pass a prefix filter.
# Here, the filter is the empty string, wich means we receive all messages.
# We may subscribe to several filters, thus receiving from all.
socket.setsockopt_string(zmq.SUBSCRIBE, "")

message = socket.recv_json()
print(message)
"""

import zmq

def main():
    """ main method """

    # Prepare our context and publisher
    print("__1__")
    context    = zmq.Context()
    print("__2__")
    subscriber = context.socket(zmq.SUB)
    print("__3__")
    subscriber.connect("tcp://localhost:5563")
    print("__4__")
    subscriber.setsockopt(zmq.SUBSCRIBE, b"B")
    print("__5__")
    while True:
        # Read envelope with address
        [address, contents] = subscriber.recv_multipart()
        print("[%s] %s" % (address, contents))
        print("__6__")

    # We never get here but clean up anyhow
    subscriber.close()
    context.term()


if __name__ == "__main__":
    main()