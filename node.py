from attrs import define, field, asdict, frozen, validators
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
import aiozmq
import zmq
import random
import msgpack

from logs import get_logger


@frozen
class HealthCheck:
    message_type: str = field(validator=[validators.instance_of(str)])


@frozen
class PeerInformation:
    id: str = field(validator=[validators.instance_of(str)])
    router_address: str = field(validator=[validators.instance_of(str)])
    publisher_address: str = field(validator=[validators.instance_of(str)])


@frozen
class SubscribeToPublisher:
    peer_id: str = field(validator=[validators.instance_of(str)])
    topic: str = field(validator=[validators.instance_of(str)])


@frozen
class UnsubscribeFromTopic:
    topic: str = field(validator=[validators.instance_of(str)])


@frozen
class PeerDiscovery:
    message_type: str = field(validator=[validators.instance_of(str)])
    id: str = field(validator=[validators.instance_of(str)])
    router_address: str = field(validator=[validators.instance_of(str)])
    publisher_address: str = field(validator=[validators.instance_of(str)])


@define
class Node:
    router_bind: str = field(validator=[validators.instance_of(str)])
    publisher_bind: str = field(validator=[validators.instance_of(str)])
    id: str = field(validator=[validators.instance_of(str)])
    my_logger = field(init=False)

    # Info about our peers
    peers: dict[str, PeerInformation] = field(factory=dict)  # str == ECDSA ID
    sockets: dict[str, aiozmq.stream.ZmqStream] = field(factory=dict)  # str == ECDSA ID

    # AIOZMQ Sockets
    _subscriber: aiozmq.stream.ZmqStream = field(init=False)
    _publisher: aiozmq.stream.ZmqStream = field(init=False)
    _router: aiozmq.stream.ZmqStream = field(init=False)
    subscribed_topics: set = field(factory=set)  # stores topics as bytes
    rep_lock = field(factory=lambda: asyncio.Lock())

    # Scheduler
    scheduler = field(factory=lambda: AsyncIOScheduler())

    # Failure Modes
    is_peer_alive: dict[str, bool] = field(factory=dict)
    crash_fail = field(factory=bool)

    ####################
    # Inbox            #
    ####################
    async def inbox(self, message):
        if message["message_type"] == "PeerDiscovery":
            message = PeerDiscovery(**message)

            # Save peer info
            pi = PeerInformation(
                message.id, message.router_address, message.publisher_address
            )
            self.peers[message.id] = pi

            # Init peer socket and cache
            req = await aiozmq.create_zmq_stream(zmq.REQ)
            await req.transport.connect(message.router_address)
            self.sockets[message.id] = req

            self.is_peer_alive[message.id] = True
        elif message["message_type"] == "HealthCheck":
            pass
        else:
            self.my_logger.warning(f"Received unknown message type {message}")

    ####################
    # Listeners        #
    ####################
    async def router_listener(self):
        self.my_logger.info("Starting Router")

        while True:
            recv = await self._router.read()

            if self.crash_fail:
                print("am crashed teehee")
            else:
                msg = msgpack.unpackb(recv[2])
                router_response = b"OK"

                asyncio.create_task(self.inbox(msg))

                self._router.write([recv[0], b"", router_response])

    async def subscriber_listener(self):
        self.my_logger.debug("Starting Subscriber")
        while True:
            recv = await self._subscriber.read()

            if self.crash_fail:
                pass
            else:
                msg = recv.decode()
                asyncio.create_task(self.inbox(msg))

    ####################
    # Message Sending  #
    ####################
    async def publish_message(self, pub: dict):
        message = msgpack.packb(asdict(pub))

        self._publisher.write([pub.topic.encode(), message])

    async def naive_direct_message(self, message, receiver: str):
        success = False

        try:
            async with asyncio.timeout(5):
                req = await aiozmq.create_zmq_stream(zmq.REQ)
                await req.transport.connect(receiver)
                self.my_logger.info(f"Successfully connected to {receiver}")

                message = msgpack.packb(asdict(message))

                async with self.rep_lock:
                    req.write([message])

                success = True
        except asyncio.TimeoutError:
            self.my_logger.info(f"Didnt receive response from {receiver}")

        return success

    async def robust_direct_message(self, message, id: str) -> bool:
        success = False

        if self.is_peer_alive[id]:
            message = msgpack.packb(asdict(message))

            async with self.rep_lock:
                self.sockets[id].write([message])

                attempts = 0
                while attempts < 10:
                    try:
                        async with asyncio.timeout(1):
                            await self.sockets[id].read()
                            self.my_logger.info(f"Received response from {id}")
                            success = True
                            break
                    except asyncio.TimeoutError:
                        self.my_logger.info(
                            f"Didnt receive response from {id}, attempt: {attempts}"
                        )
                        attempts += 1
                        await asyncio.sleep(1)
                else:
                    self.my_logger.warning(
                        f"No reponse received from {id} after {attempts} attempts"
                    )

                    self.is_peer_alive[id] = False

        return success

    ####################
    # Node Message Bus #
    ####################
    def command(self, command_obj, receiver=""):
        if isinstance(command_obj, SubscribeToPublisher):
            asyncio.create_task(self.subscribe(command_obj))
        elif isinstance(command_obj, UnsubscribeFromTopic):
            asyncio.create_task(self.unsubscribe(command_obj))
        else:
            self.my_logger.error(f"Unrecognised command object: {command_obj}")

    ####################
    # Scheduled Tasks  #
    ####################
    async def health_check_task(self):
        offline_peers = [
            node_id for node_id in list(self.peers) if not self.is_peer_alive[node_id]
        ]

        for peer_id in offline_peers:
            if not self.is_peer_alive[peer_id]:
                hc = HealthCheck("HealthCheck")
                success = await self.naive_direct_message(
                    hc, self.peers[peer_id].router_address
                )
                print(success)
                if success:
                    self.my_logger.error(f"Node {peer_id} is back online")
                    req = await aiozmq.create_zmq_stream(zmq.REQ)
                    await req.transport.connect(self.peers[peer_id].router_address)
                    self.sockets[peer_id] = req
                    self.is_peer_alive[peer_id] = True
                else:
                    self.my_logger.error(f"Node {peer_id} is still offline")
        else:
            self.my_logger.info("All nodes online")

    ####################
    # Helper Functions #
    ####################

    async def peer_discovery(self, routers: list):
        pd = PeerDiscovery(
            id=self.id,
            message_type="PeerDiscovery",
            router_address=self.router_bind,
            publisher_address=self.publisher_bind,
        )

        random.shuffle(routers)

        # Send the PD message to all peers
        for ip in routers:
            await self.naive_direct_message(pd, ip)

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

        self._publisher = await aiozmq.create_zmq_stream(
            zmq.PUB, bind=f"tcp://*:{21001 + int(self.id)}"
        )
        self._router = await aiozmq.create_zmq_stream(
            zmq.ROUTER, bind=f"tcp://*:{20001 + int(self.id)}"
        )

        self.my_logger = get_logger(self.id)

        self.my_logger.info("Started PUB/SUB Sockets")

    async def start(self):
        asyncio.create_task(self.router_listener())
        asyncio.create_task(self.subscriber_listener())

        self.scheduler.add_job(
            self.health_check_task,
            trigger="interval",
            seconds=20,
        )

        self.scheduler.start()

        await asyncio.sleep(random.randint(1, 3))

        self.my_logger.info("STARTED")
