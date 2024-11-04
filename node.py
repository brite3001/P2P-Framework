from attrs import define, field, asdict, frozen, validators
import asyncio
import aiozmq
import zmq
import json
import random
import time
import math

from logs import get_logger


@frozen
class PeerInformation:
    router_address: str = field(validator=[validators.instance_of(str)])
    publisher_address: str = field(validator=[validators.instance_of(str)])


@define
class PeerSocket:
    router_address: str = field(validator=[validators.instance_of(str)])
    socket: aiozmq.ZmqStream = field(
        validator=[validators.instance_of(aiozmq.ZmqStream)]
    )


@frozen
class SubscribeToPublisher:
    peer_id: str = field(validator=[validators.instance_of(str)])
    topic: str = field(validator=[validators.instance_of(str)])


@frozen
class UnsubscribeFromTopic:
    topic: str = field(validator=[validators.instance_of(str)])


@frozen
class PublishMessage:
    message_type: str = field(validator=[validators.instance_of(str)])
    topic: str = field(validator=[validators.instance_of(str)])
    message: str = field(validator=[validators.instance_of(str)])


@frozen
class DirectMessage:
    message_type: str = field(validator=[validators.instance_of(str)])
    message: str = field(validator=[validators.instance_of(str)])


@frozen
class PeerDiscovery(DirectMessage):
    router_address: str = field(validator=[validators.instance_of(str)])
    publisher_address: str = field(validator=[validators.instance_of(str)])


@define
class Node:
    router_bind: str = field(validator=[validators.instance_of(str)])
    publisher_bind: str = field(validator=[validators.instance_of(str)])
    id: str = field(init=False)
    my_logger = field(init=False)

    # Info about our peers
    peers: dict[str, PeerInformation] = field(factory=dict)  # str == ECDSA ID
    sockets: dict[str, PeerSocket] = field(factory=dict)  # str == ECDSA ID

    # AIOZMQ Sockets
    _subscriber: aiozmq.stream.ZmqStream = field(init=False)
    _publisher: aiozmq.stream.ZmqStream = field(init=False)
    _router: aiozmq.stream.ZmqStream = field(init=False)
    connected_subscribers: set = field(factory=set)  # stores peer_ids
    subscribed_topics: set = field(factory=set)  # stores topics as bytes
    rep_lock = field(factory=lambda: asyncio.Lock())

    running: bool = field(factory=bool)

    ####################
    # Inbox            #
    ####################
    async def inbox(self, message):
        print(f"[{self.id}] Inbox Received Message {message}")

    ####################
    # Listeners        #
    ####################
    async def router_listener(self):
        self.my_logger.info("Starting Router")

        while True:
            recv = await self._router.read()

            msg = recv[2].decode()
            router_response = b"OK"

            asyncio.create_task(self.inbox(msg))

            self._router.write([recv[0], b"", router_response])

    async def subscriber_listener(self):
        self.my_logger.debug("Starting Subscriber")
        while True:
            recv = await self._subscriber.read()

            msg = recv.decode()

            asyncio.create_task(self.inbox(msg))

    ####################
    # Message Sending  #
    ####################
    async def publish_message(self, pub: dict):
        message = asdict(pub).encode()

        self._publisher.write([pub.topic.encode(), message])

    async def direct_message(self, message: dict, receiver: str):
        req = await aiozmq.create_zmq_stream(zmq.REQ)

        attempts = 0
        while attempts < 10:
            try:
                async with asyncio.timeout(1):
                    await req.transport.connect(receiver)
                    self.my_logger.info(f"Successfully connected to {receiver}")
                    break
            except asyncio.TimeoutError:
                self.my_logger.warning(
                    f"Couldnt connect to {receiver}, attempt: {attempts}"
                )
                req.close()
                req = await aiozmq.create_zmq_stream(zmq.REQ)
                attempts += 1
                await asyncio.sleep(1)
        else:
            self.my_logger.error(
                f"Failed to connect to {receiver} after {attempts} attempts"
            )

        message = json.dumps(message).encode()

        async with self.rep_lock:
            req.write([message])

            attempts = 0
            while attempts < 50:
                try:
                    async with asyncio.timeout(1):
                        await req.read()
                        self.my_logger.info(f"Received response from {receiver}")
                        break
                except asyncio.TimeoutError:
                    self.my_logger.warning(
                        f"Didnt receive response from {receiver}, attempt: {attempts}"
                    )
                    attempts += 1
                    await asyncio.sleep(1)
            else:
                self.my_logger.error(
                    f"No reponse received from {receiver} after {attempts} attempts"
                )

        req.close()

    ####################
    # Node Message Bus #
    ####################
    def command(self, command_obj, receiver=""):
        if isinstance(command_obj, SubscribeToPublisher):
            asyncio.create_task(self.subscribe(command_obj))
        elif isinstance(command_obj, UnsubscribeFromTopic):
            asyncio.create_task(self.unsubscribe(command_obj))
        elif issubclass(type(command_obj), DirectMessage):
            asyncio.create_task(self.direct_message(command_obj, receiver))
        elif isinstance(command_obj, PublishMessage):
            asyncio.create_task(self.publish_message(command_obj))
        else:
            self.my_logger.error(f"Unrecognised command object: {command_obj}")

    ####################
    # Helper Functions #
    ####################

    def calculate_uniform_params(self):
        num_nodes = len(self.peers)
        mean = (num_nodes - 1) / 2
        std_dev = math.sqrt(num_nodes)
        return mean, std_dev

    def select_nodes(self, num_nodes_to_select: int) -> set:
        selected_nodes = random.sample(list(self.peers), num_nodes_to_select)

        return set(selected_nodes)

    async def peer_discovery(self, routers: list):
        pd = PeerDiscovery(
            message_type="PeerDiscovery",
            router_address=self.router_bind,
            publisher_address=self.publisher_bind,
        )

        random.shuffle(routers)

        # Send the PD message to all peers
        for ip in routers:
            await self.unsigned_direct_message(pd, ip)

    async def subscribe_to_all_peers_and_topics(self):
        # peer_id is a key from the self.peers dict

        for peer_id in self.peers:
            self._subscriber.transport.connect(self.peers[peer_id].publisher_address)

        self._subscriber.transport.subscribe(b"")

    async def subscribe(self, s2p: SubscribeToPublisher):
        # peer_id is a key from the self.peers dict

        if s2p.topic not in self.subscribed_topics:
            self.subscribed_topics.add(s2p.topic)

    async def unsubscribe(self, s2p: UnsubscribeFromTopic):
        # peer_id is a key from the self.peers dict

        if s2p.topic in self.subscribed_topics:
            self.subscribed_topics.remove(s2p.topic)

    async def init_sockets(self):
        self._subscriber = await aiozmq.create_zmq_stream(zmq.SUB)

        import os

        node_port = int(os.getenv("NODE_ID"))

        self._publisher = await aiozmq.create_zmq_stream(
            zmq.PUB, bind=f"tcp://*:{21001 + node_port}"
        )
        self._router = await aiozmq.create_zmq_stream(
            zmq.ROUTER, bind=f"tcp://*:{20001 + node_port}"
        )

        self.my_logger = get_logger(self.id)

        self.my_logger.info("Started PUB/SUB Sockets")

    async def start(self):
        self.running = True
        asyncio.create_task(self.router_listener())
        asyncio.create_task(self.subscriber_listener())

        await asyncio.sleep(random.randint(1, 3))

        self.my_logger.info("Node x started")
