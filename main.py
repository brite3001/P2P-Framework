import asyncio
import signal
import uvloop
import os
import random

from node import Node, PBPayload, SubscribeToPublisher, TestPubSub
from logs import get_logger

logging = get_logger("runner")


def get_node_port():
    node_port = os.getenv("NODE_ID")
    if node_port is None:
        raise ValueError("NODE_ID environment variable is not set")
    return int(node_port)


async def main():
    NUM_NODES = 10

    docker_node_id = get_node_port()

    router_list = []

    for i in range(NUM_NODES):
        if i != docker_node_id:
            router_list.append(f"tcp://127.0.0.1:{20001+i}")

    this_node = Node(
        id=str(docker_node_id),
        router_bind=f"tcp://127.0.0.1:{20001 + docker_node_id}",
        publisher_bind=f"tcp://127.0.0.1:{21001 + docker_node_id}",
    )

    logging.warning(f"Spinning up {docker_node_id}")
    await this_node.init_sockets()
    await this_node.start()

    logging.warning(f"Running peer discovery on {docker_node_id}...")
    await this_node.peer_discovery(router_list)

    # Wait til we find all our peers
    while len(list(this_node.peers.keys())) != len(router_list):
        logging.warning(
            f"Dont have all peers GOT: {len(list(this_node.peers.keys()))} NEED: {len(router_list)} "
        )

        await asyncio.sleep(1)

    # Wait til we cache all our peers sockets
    while len(list(this_node.sockets.keys())) != len(router_list):
        logging.warning(
            f"Dont have all sockets GOT: {len(list(this_node.sockets.keys()))} NEED: {len(router_list)} "
        )

        await asyncio.sleep(1)

    logging.warning(
        f"All nodes ready {len(list(this_node.peers.keys()))} / {len(router_list)} "
    )

    this_node.command(TestPubSub("TestPubSub", "test"))
    this_node.command(TestPubSub("TestPubSub", "PBPayload"))

    await asyncio.sleep(1)

    this_node.command(TestPubSub("TestPubSub", "test"))
    this_node.command(TestPubSub("TestPubSub", "PBPayload"))

    await asyncio.sleep(5)

    await this_node.subscribe_to_all_peers_and_topics()
    this_node.command(SubscribeToPublisher("test"))
    this_node.command(SubscribeToPublisher("PBCertificate"))

    for _ in range(16):
        if this_node.id == "0":
            this_node.command(PBPayload("PBPayload", "PBPayload", this_node.id))
            await asyncio.sleep(1)


async def shutdown(signal, loop):
    logging.info(f"Received exit signal {signal.name}...")

    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

    for task in tasks:
        task.cancel()

    logging.info("Cancelling outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()


if __name__ == "__main__":
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)

    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(s, lambda s=s: asyncio.create_task(shutdown(s, loop)))

    try:
        loop.create_task(main())
        loop.run_forever()
    finally:
        logging.info("Successfully shutdown service")
        loop.close()
