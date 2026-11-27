"""A gRPC server on grpcio, which runs gRPC C-core, for the protocols kind gate.

C-core rejects a request without `te: trailers`, so a call that succeeds here
proves the header survives frontline and the sidecar. Messages are raw bytes:
the server registers no serializers and needs no generated code.
"""

from concurrent import futures
import os
import signal
import threading

import grpc

SERVICE = "sleepypods.e2e.Echo"


def instance():
    return os.environ.get("SLEEPYPODS_E2E_INSTANCE", "unknown")


def unary(request, context):
    context.set_trailing_metadata((("x-e2e-instance", instance()),))
    return b"unary:" + instance().encode() + b":" + request


def server_stream(request, context):
    context.set_trailing_metadata((("x-e2e-instance", instance()),))
    for index in range(int(request.decode())):
        yield f"stream:{instance()}:{index}".encode()


def main():
    port = int(os.environ.get("PORT", "8080"))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                SERVICE,
                {
                    "Unary": grpc.unary_unary_rpc_method_handler(unary),
                    "ServerStream": grpc.unary_stream_rpc_method_handler(server_stream),
                },
            ),
        )
    )
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    print(f"grpcio {grpc.__version__} serving {SERVICE} on {port}", flush=True)

    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    stopped.wait()
    server.stop(grace=5).wait()


if __name__ == "__main__":
    main()
