import asyncio
import signal
import uvloop
import time
import os
import random

from .node import Node
from .logs import get_logger

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

    await asyncio.sleep(5)

    pad = 10**935
    for i in range(1, 1000):
        # Randomize the process of sending commands with a certain probability
        if random.random() < 0.5:  # Adjust probability as needed
            # logging.error(f"Node {docker_node_id} sending commands at iteration {i}")
            for _ in range(random.randint(5, 15)):
                gos = Gossip(
                    message_type="Gossip", timestamp=int(time.time()), padding=pad
                )
                this_node.command(gos)

        await asyncio.sleep(random.randint(1, 2))

    # this_node.scheduler.pause_job(this_node.increase_job_id)
    # this_node.scheduler.pause_job(this_node.decrease_job_id)

    # await asyncio.sleep(15)

    # url = "http://localhost:8000/current_latency/"
    # r = requests.post(url, json={"data": this_node.current_latency_metadata})
    # print(r.status_code)

    # url = "http://localhost:8000/delivered_latency/"
    # print(this_node.delivered_msg_metadata)
    # r = requests.post(url, json={"data": this_node.delivered_msg_metadata})
    # print(r.status_code)


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
