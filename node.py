from attrs import define, field, asdict, frozen, validators
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from collections import defaultdict
import asyncio
import aiozmq
import zmq
import random
import msgpack
import string

from logs import get_logger


@frozen
class HealthCheck:
    message_type: str = field(validator=[validators.instance_of(str)])


@frozen
class TestReqRep:
    message_type: str = field(validator=[validators.instance_of(str)])
    payload: str = field(
        factory=lambda: "".join(
            random.choices(string.ascii_letters + string.digits, k=250)
        )
    )


@frozen
class TestPubSub:
    message_type: str = field(validator=[validators.instance_of(str)])
    topic: str = field(validator=[validators.instance_of(str)])
    payload: str = field(
        factory=lambda: "".join(
            random.choices(string.ascii_letters + string.digits, k=250)
        )
    )


@frozen
class PBPayload:
    message_type: str = field(validator=[validators.instance_of(str)])
    topic: str = field(validator=[validators.instance_of(str)])
    creator: str = field(validator=[validators.instance_of(str)])
    payload: str = field(
        factory=lambda: "".join(
            random.choices(string.ascii_letters + string.digits, k=250)
        )
    )


@frozen
class PBCertificate:
    message_type: str = field(validator=[validators.instance_of(str)])
    topic: str = field(validator=[validators.instance_of(str)])
    creator: str = field(validator=[validators.instance_of(str)])
    pb_payload_hash: int = field(validator=[validators.instance_of(int)])
    certificate_number: int = field(validator=[validators.instance_of(int)])


@frozen
class GatherSet:
    message_type: str = field(validator=[validators.instance_of(str)])
    topic: str = field(validator=[validators.instance_of(str)])
    creator: str = field(validator=[validators.instance_of(str)])
    message_hashes: list = field(validator=[validators.instance_of(list)])
    gather_round: int = field(validator=[validators.instance_of(int)])
    gather_instance: int = field(validator=[validators.instance_of(int)])


@frozen
class PeerInformation:
    id: str = field(validator=[validators.instance_of(str)])
    router_address: str = field(validator=[validators.instance_of(str)])
    publisher_address: str = field(validator=[validators.instance_of(str)])


@frozen
class SubscribeToPublisher:
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

    # Consensus Variables
    n = 9
    f = 2

    # Gather Agreement - ACS
    gather_instance: int = field(factory=int)
    set_s: list = field(factory=list)  # round 2
    set_t: list = field(factory=list)  # round 3
    completed_gather: set = field(factory=set)  # list of hashes

    gather_lock: asyncio.Lock = field(
        factory=asyncio.Lock
    )  # only want 1 gather instance at a time
    set_s_flag: asyncio.Event = field(factory=asyncio.Event)
    set_t_flag: asyncio.Event = field(factory=asyncio.Event)

    # Provable Broadcast
    created_pb_payloads: set = field(factory=set)  # list of hashes
    received_pb_payloads: set = field(factory=set)  # list of hashes
    delivered_pb_payloads: set = field(factory=set)  # list of hashes
    failed_pb_payloads: set = field(factory=set)  # list of hashes
    pb_payloads: set = field(factory=set)  # pb payloads themselves
    cert_0: dict[int, list[PBCertificate]] = field(factory=lambda: defaultdict(list))
    cert_1: dict[int, list[PBCertificate]] = field(factory=lambda: defaultdict(list))
    cert_2: dict[int, list[PBCertificate]] = field(factory=lambda: defaultdict(list))
    cert_3: dict[int, list[PBCertificate]] = field(factory=lambda: defaultdict(list))

    proof_flags: dict[int, asyncio.Event] = field(factory=dict)
    cert_1_flags: dict[int, asyncio.Event] = field(factory=dict)
    cert_2_flags: dict[int, asyncio.Event] = field(factory=dict)
    cert_3_flags: dict[int, asyncio.Event] = field(factory=dict)

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
        elif message["message_type"] == "PBPayload":
            """
            Party i:
                For first <v, proof> received from s:
                    If EV_1(v,proof) then send <v, proof>_i to s

                For first <v, cert_1(v)> received from s:
                    If EV_2(v, cert_1(v)) then send <v, cert_1(v)>_i to s
                    
                For first <v, cert_2(v)> received from s:
                    If EV_3(v, cert_2(v)) then send <v, cert_2(v)>_i to s

                For first <v, cert_3(v)> received from s:
                    If EV_4(v, cert_3(v)) then 
                        Deliver v; and
                        send <v, cert_3(v)>_i to s
            """
            # TODO: add signatures + sig checks
            message = PBPayload(**message)
            message_hash = hash(message)
            self.pb_payloads.add(message)
            self.received_pb_payloads.add(message_hash)
            self.my_logger.info(f"PBPayload received {message_hash}")
            pbc = PBCertificate(
                "PBCertificate", "PBCertificate", self.id, message_hash, 0
            )

            self.command(pbc, message.creator)
        elif message["message_type"] == "PBCertificate":
            # TODO: add signature checks
            # TODO: add external validity checks
            message = PBCertificate(**message)
            message_hash = hash(message)
            if message.pb_payload_hash in self.created_pb_payloads:
                # This part is for PBPayload creators
                self.my_logger.info(f"got cert {message.certificate_number}")
                if message.certificate_number == 0:
                    self.cert_0[message.pb_payload_hash].append(message)
                    if len(self.cert_0[message.pb_payload_hash]) >= 9:
                        self.my_logger.info(len(self.cert_0[message.pb_payload_hash]))
                        self.proof_flags[message.pb_payload_hash].set()
                elif message.certificate_number == 1:
                    self.cert_1[message.pb_payload_hash].append(message)
                    if len(self.cert_1[message.pb_payload_hash]) >= 9:
                        self.my_logger.info(len(self.cert_1[message.pb_payload_hash]))
                        self.cert_1_flags[message.pb_payload_hash].set()
                elif message.certificate_number == 2:
                    self.cert_2[message.pb_payload_hash].append(message)
                    if len(self.cert_2[message.pb_payload_hash]) >= 9:
                        self.my_logger.info(len(self.cert_2[message.pb_payload_hash]))
                        self.cert_2_flags[message.pb_payload_hash].set()
                elif message.certificate_number == 3:
                    self.cert_3[message.pb_payload_hash].append(message)
                    if len(self.cert_3[message.pb_payload_hash]) >= 9:
                        self.my_logger.info(len(self.cert_3[message.pb_payload_hash]))
                        self.cert_3_flags[message.pb_payload_hash].set()

            elif message.pb_payload_hash in self.received_pb_payloads:
                # This part for PBPayload Receivers
                if message.certificate_number == 1:
                    self.command(message, message.creator)
                elif message.certificate_number == 2:
                    self.command(message, message.creator)
                elif message.certificate_number == 3:
                    self.command(message, message.creator)
                    self.delivered_pb_payloads.add(message.pb_payload_hash)

                    # Gather Round 2
                    # Once Si contains n−f pairs send Si to every party
                    if (
                        len(self.delivered_pb_payloads - self.completed_gather)
                        >= self.n - self.f
                    ):
                        if not self.gather_lock.locked():
                            asyncio.create_task(self.gather())

        elif message["message_type"] == "GatherSet":
            message = GatherSet(**message)

            # If you're the original sender of messagers, you'll wait to get a
            # robust certificate before delivering a message, however, receivers will
            # only wait for a delivery certificate. This accomidates the delay.
            retry = 0
            while retry < 5:
                if set(message.message_hashes).issubset(self.delivered_pb_payloads):
                    break
                else:
                    self.my_logger.info("Not all messages in subset... waiting...")
                    await asyncio.sleep(1)
                    retry += 1

            if message.gather_round == 3:
                # When receiving a message Sj from party, accept the message after receiving the broadcast xk
                # from k for every (k,xk)∈Sj

                if set(message.message_hashes).issubset(self.delivered_pb_payloads):
                    # When receiving a message Sj from party, accept the message after receiving
                    # the broadcast xk from k for every (k,xk)∈Sj
                    self.set_s.append(set(message.message_hashes))
                else:
                    self.my_logger.error("Failed to get all messages in subset")

                if len(self.set_s) >= self.n - self.f:
                    self.set_s_flag.set()
            elif message.gather_round == 4:
                if set(message.message_hashes).issubset(self.delivered_pb_payloads):
                    self.set_t.append(set(message.message_hashes))
                else:
                    self.my_logger.warning("Failed to get all messages in subset")

                if len(self.set_t) >= self.n - self.f:
                    self.set_t_flag.set()

        elif message["message_type"] == "TestReqRep":
            self.my_logger.info("Test payload")
        elif message["message_type"] == "TestPubSub":
            self.my_logger.info("Test publish")
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
                message = msgpack.unpackb(recv[2])
                router_response = b"OK"

                asyncio.create_task(self.inbox(message))

                self._router.write([recv[0], b"", router_response])

    async def subscriber_listener(self):
        self.my_logger.debug("Starting Subscriber")
        while True:
            recv = await self._subscriber.read()

            if self.crash_fail:
                pass
            else:
                message = msgpack.unpackb(recv[1])
                asyncio.create_task(self.inbox(message))

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

                self.my_logger.info(f"Received response from {receiver}")

                await req.read()

                success = True
        except asyncio.TimeoutError:
            self.my_logger.info(f"Didnt receive response from {receiver}")

        return success

    async def robust_direct_message(self, message, id: str) -> bool:
        success = False

        if self.is_peer_alive[id]:
            message = msgpack.packb(asdict(message))
            attempts = 0

            async with self.rep_lock:
                self.sockets[id].write([message])

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
    # Broadcast Algorithms #
    ####################

    async def provable_broadcast(self, message: PBPayload):
        """
        send <v, proof> to all
        send <v, cert_1(v)> to all when you obtain cert_1(v)
        send <v, cert_2(v)> to all when you obtain cert_2(v)
        send <v, cert_3(v)> to all when you obtain cert_3(v)

        Cert_1(v), "key-certificate for v":
            n-f distinct signers on <v, proof>

        Cert_2(v), "lock-certificate for v":
            n-f distinct signers on <v, cert_1(v)>

        Cert_3(v), "delivery-certifiacte for v":
            n-f distinct signers on <v, cert_2(v)>

        Cert_4(v), "robust-certificate for v":
            n-f distinct signers on <v, cert_3(v)>

        """
        message_hash = hash(message)

        # Broadcasting PBPayload
        self.proof_flags[message_hash] = asyncio.Event()

        self.created_pb_payloads.add(hash(message))
        asyncio.create_task(self.publish_message(message))

        self.my_logger.info("waiting to get all proofs...")

        await self.proof_flags[message_hash].wait()

        # Cert 1
        self.cert_1_flags[message_hash] = asyncio.Event()
        pbc = PBCertificate("PBCertificate", "PBCertificate", self.id, message_hash, 1)
        asyncio.create_task(self.publish_message(pbc))

        self.my_logger.info("waiting to get all cert_1...")

        try:
            await asyncio.wait_for(self.cert_1_flags[message_hash].wait(), timeout=5)
        except TimeoutError:
            self.my_logger.error(f"Failed to get cert 1 for message: {message_hash}")
            self.failed_pb_payloads.add(message_hash)
            return

        self.my_logger.info("got all cert_1!")

        # Cert 2
        self.cert_2_flags[message_hash] = asyncio.Event()
        pbc = PBCertificate("PBCertificate", "PBCertificate", self.id, message_hash, 2)
        asyncio.create_task(self.publish_message(pbc))

        self.my_logger.info("waiting to get all cert_2...")

        try:
            await asyncio.wait_for(self.cert_2_flags[message_hash].wait(), timeout=5)
        except TimeoutError:
            self.my_logger.error(f"Failed to get cert 2 for message: {message_hash}")
            self.failed_pb_payloads.add(message_hash)
            return

        self.my_logger.info("got all cert_2!")

        # Cert 3
        self.cert_3_flags[message_hash] = asyncio.Event()
        pbc = PBCertificate("PBCertificate", "PBCertificate", self.id, message_hash, 3)
        asyncio.create_task(self.publish_message(pbc))

        self.my_logger.info("waiting to get all cert_3...")

        try:
            await asyncio.wait_for(self.cert_3_flags[message_hash].wait(), timeout=5)
        except TimeoutError:
            self.my_logger.error(f"Failed to get cert 3 for message: {message_hash}")
            self.failed_pb_payloads.add(message_hash)
            return

        self.delivered_pb_payloads.add(message_hash)

        self.my_logger.error(f"PB Success for {message_hash}")

        if len(self.delivered_pb_payloads - self.completed_gather) >= self.n - self.f:
            if not self.gather_lock.locked():
                asyncio.create_task(self.gather())

    ####################
    # Agreement Algorithms #
    ####################

    async def gather_cleanup(self):
        self.set_s = [x for x in self.set_s if not x.issubset(self.completed_gather)]
        self.set_t = [x for x in self.set_t if not x.issubset(self.completed_gather)]

        self.set_s_flag.clear()
        self.set_t_flag.clear()
        self.gather_instance += 1

    async def gather(self):
        async with self.gather_lock:
            si = list(self.delivered_pb_payloads - self.completed_gather)

            gs = GatherSet(
                "GatherSet",
                "GatherSet",
                self.id,
                si,
                3,
                self.gather_instance,
            )

            print(f"Si {self.gather_instance}: {si}")

            # Once Si contains n−f pairs send Si to every party
            self.command(gs)

            try:
                await asyncio.wait_for(self.set_s_flag.wait(), timeout=5)
            except TimeoutError:
                self.my_logger.error("Failed to n-f values in set_s")
                self.gather_cleanup()
                return

            ti = set()

            for individual_set in self.set_s:
                for individual_element in individual_set:
                    ti.add(individual_element)

            # After accepting n−f sets Sj, send Ti=∪Sj to all parties.
            gs = GatherSet(
                "GatherSet", "GatherSet", self.id, list(ti), 4, self.gather_instance
            )
            self.command(gs)

            try:
                await asyncio.wait_for(self.set_t_flag.wait(), timeout=5)
            except TimeoutError:
                self.my_logger.error("Failed to n-f values in set_t")
                self.gather_cleanup()
                return

            # After accepting n−f sets Tj, output Ui=∪Tj.
            ui = set()
            for individual_set in self.set_t:
                for individual_element in individual_set:
                    ui.add(individual_element)

            print(f"self.set_t {self.gather_instance}: {self.set_t}")

            self.my_logger.warning(
                f"Successfully Gathered {self.gather_instance}: {ui}"
            )

            self.completed_gather.update(ui)

            print(f"Completed gather {self.gather_instance}: {self.completed_gather}")

            await self.gather_cleanup()

    ####################
    # Node Message Bus #
    ####################
    def command(self, command_obj, receiver=""):
        if isinstance(command_obj, SubscribeToPublisher):
            asyncio.create_task(self.subscribe(command_obj))
        elif isinstance(command_obj, UnsubscribeFromTopic):
            asyncio.create_task(self.unsubscribe(command_obj))
        elif isinstance(command_obj, GatherSet):
            asyncio.create_task(self.publish_message(command_obj))
        elif isinstance(command_obj, PBPayload):
            asyncio.create_task(self.provable_broadcast(command_obj))
        elif isinstance(command_obj, PBCertificate):
            asyncio.create_task(self.robust_direct_message(command_obj, receiver))
        elif isinstance(command_obj, PeerDiscovery):
            asyncio.create_task(self.naive_direct_message(command_obj, receiver))
        elif isinstance(command_obj, HealthCheck):
            asyncio.create_task(self.naive_direct_message(command_obj, receiver))
        elif isinstance(command_obj, TestReqRep):
            asyncio.create_task(self.robust_direct_message(command_obj, receiver))
        elif isinstance(command_obj, TestPubSub):
            asyncio.create_task(self.publish_message(command_obj))
        else:
            self.my_logger.error(f"Unrecognised command object: {command_obj}")

    # Broadcast Protocols
    # https://decentralizedthoughts.github.io/2022-09-10-provable-broadcast/
    # https://decentralizedthoughts.github.io/2024-08-08-vid/
    # RBC

    # ACS implementation
    # maybe just Gather?
    # https://decentralizedthoughts.github.io/2021-03-26-living-with-asynchrony-the-gather-protocol/

    # Forecasting

    # TimesFMForecaster
    # TinyTimeMixerForecaster
    # Chronos

    ####################
    # Scheduled Tasks  #
    ####################
    async def health_check_task(self):
        offline_peers = [
            node_id for node_id in list(self.peers) if not self.is_peer_alive[node_id]
        ]

        if len(offline_peers) > 0:
            for peer_id in offline_peers:
                if not self.is_peer_alive[peer_id]:
                    hc = HealthCheck("HealthCheck")
                    success = await self.naive_direct_message(
                        hc, self.peers[peer_id].router_address
                    )
                    if success:
                        self.my_logger.error(f"Node {peer_id} is back online")
                        req = await aiozmq.create_zmq_stream(zmq.REQ)
                        await req.transport.connect(self.peers[peer_id].router_address)
                        self.sockets[peer_id] = req
                        self.is_peer_alive[peer_id] = True
                    else:
                        self.my_logger.error(f"Node {peer_id} is offline")
        else:
            self.my_logger.error("All nodes online")

    ####################
    # Helper Functions #
    ####################

    async def external_validity():
        return True

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
            self.command(pd, ip)

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

        # self.scheduler.add_job(
        #     self.health_check_task,
        #     trigger="interval",
        #     seconds=20,
        # )

        # self.scheduler.start()

        await asyncio.sleep(random.randint(1, 3))

        self.my_logger.info("STARTED")
