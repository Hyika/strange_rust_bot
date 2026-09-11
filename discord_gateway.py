# Python 내장 라이브러리
import asyncio
from dataclasses import asdict, dataclass
from enum import IntEnum
import json
from typing import TYPE_CHECKING, Self

# 외부 라이브러리
import websockets

# 사용자 라이브러리
if TYPE_CHECKING:
    from discord_client import Client


@dataclass
class IdentifyProperties: 
    os: str
    browser: str 
    device: str

@dataclass
class IdentifyData: 
    token: str
    intents: int
    properties: IdentifyProperties

@dataclass 
class ResumeData: 
    token: str 
    session_id: str 
    seq: int

class GatewayOpcode(IntEnum):
    DISPATCH = 0
    HEARTBEAT = 1
    IDENTIFY = 2
    PRESENCE_UPDATE = 3
    VOICE_STATE_UPDATE = 4
    RESUME = 6
    RECONNECT = 7
    REQUEST_GUILD_MEMBERS = 8
    INVALID_SESSION = 9
    HELLO = 10
    HEARTBEAT_ACK = 11
    REQUEST_SOUNDBOARD_SOUNDS = 31 

@dataclass
class GatewayPayload: 
    op: GatewayOpcode
    d: any
    s: int | None = None
    t: str | None = None
    
    def to_dict(self): 
        return asdict(self)

    @classmethod
    def from_dict(cls, data:dict): 
        return cls(
            op=GatewayOpcode(data['op']), 
            d=data.get('d'), 
            s=data.get('s'), 
            t=data.get('t')
        )

    @classmethod
    def identify(cls, identify_data: IdentifyData) -> Self:
         return cls(
             op=GatewayOpcode.IDENTIFY, 
             d=identify_data
         ) 

    @classmethod
    def heartbeat(cls, sequence_number: int | None) -> Self: 
        return cls(
            op=GatewayOpcode.HEARTBEAT, 
            d=sequence_number
        )

    @classmethod
    def resume(cls, resume_data: ResumeData) -> Self:
        return cls(
            op=GatewayOpcode.RESUME, 
            d=resume_data
        )


class Gateway: 
    def __init__(self, token: str, url: str, client: Client): 
        self.token = token
        self.websocket = None
        self.url = url
        self.resume_url: str | None = None
        self.session_id: str | None = None
        self.sequence_number: int | None = None

        self._heartbeat_task = None
        self._can_resume = True

        self.client = client


    async def send(self, payload: GatewayPayload): 
        message = json.dumps(payload.to_dict())
        await self.websocket.send(message)

    async def receive(self)-> GatewayPayload: 
        message = await self.websocket.recv()
        payload = GatewayPayload.from_dict(json.loads(message))
        return payload

    async def heartbeat_lifecycle(self, ms_interval):
        try:
            while True:
                await asyncio.sleep(ms_interval/1000)

                await self.send(GatewayPayload.heartbeat(sequence_number=self.sequence_number))
                print("OP 1 | Heartbeat. ")
        except asyncio.CancelledError: 
            print("Heartbeak lifecycle cancelled. ")
            raise

    async def close(self, code: int = None, reason: str = None): 
        if self.websocket:
            if self._heartbeat_task:
                self._heartbeat_task.cancel() 
                self._heartbeat_task = None

            await self.websocket.close(code=code, reason=reason)
            self.websocket = None
        else:
            print("Websocket is not found. ")
        
    async def connect(self): 
        while self._can_resume: 
            if self.websocket:
                raise Exception("Websocket is already created.")
            else:
                self.websocket = await websockets.connect(self.resume_url or self.url)

            while True: 
                payload = await self.receive()

                match payload.op:
                    case GatewayOpcode.DISPATCH: 
                        print(f"OP 0 | Dispatch received. ")
                        self.sequence_number = payload.s
                        await self.client.dispatch(data=payload.d, event_name=payload.t)
                        
                    case GatewayOpcode.RECONNECT: 
                        print(f"OP 7 | Reconnect received. ")
                        await self.close(code=4000, reason="Reconnecting")
                        break

                    case GatewayOpcode.INVALID_SESSION: 
                        print(f"OP 9 | Invalid session received. ")
                        if payload.d: 
                            await self.close(code=4000, reason="Reconnecting")
                            self._can_resume = True
                            break
                        else: 
                            await self.close()
                            self._can_resume = False
                            raise Exception("Can't resume. ")

                    case GatewayOpcode.HELLO: 
                        print(f"OP 10 | Hello received. ")

                        self._heartbeat_task = asyncio.create_task(self.heartbeat_lifecycle(payload.d['heartbeat_interval']))

                        if self.resume_url:
                            await self.send(
                                GatewayPayload.resume(
                                    resume_data=ResumeData(
                                        token=self.token, 
                                        session_id=self.session_id, 
                                        seq=self.sequence_number
                                    )
                                )
                            )
                        else: 
                            await self.send(
                                 GatewayPayload.identify(
                                     identify_data=IdentifyData(
                                         self.token, 
                                         intents=33281, 
                                         properties=IdentifyProperties(
                                             os='linux', 
                                             browser='titti', 
                                             device='titti'
                                         )
                                     )
                                 )
                             )
                    case GatewayOpcode.HEARTBEAT_ACK: 
                        print(f"OP 11 | Heartbeat ACK received. ")
