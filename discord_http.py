# Python 공식 라이브러리
from dataclasses import asdict, dataclass
from enum import IntEnum

# 외부 라이브러리
import httpx


class ApplicationCommandTypes(IntEnum):
    CHAT_INPUT = 1
    USER = 2
    MESSAGE = 3
    PRIMARY_ENTRY_POINT = 4


class ApplicationCommandOptionType(IntEnum):
    SUB_COMMAND = 1
    SUB_COMMAND_GROUP = 2
    STRING = 3
    INTEGER = 4
    BOOLEAN = 5
    USER = 6
    CHANNEL = 7
    ROLE = 8
    MENTIONABLE = 9
    NUMBER = 10
    ATTACHMENT = 11


@dataclass
class ApplicationCommandOption:
    name: str
    description: str
    type: ApplicationCommandOptionType
    required: bool = False
    options: list[ApplicationCommandOption] | None = None
    
    def to_dict(self): 
        return asdict(self)
    
@dataclass
class ApplicationCommandInput:
    name: str
    description: str
    type: ApplicationCommandTypes = ApplicationCommandTypes.CHAT_INPUT
    options: list[ApplicationCommandOption] | None = None
    
    def to_dict(self): 
        return asdict(self)
    
@dataclass
class ApplicationCommand:
    id: str
    application_id: str
    version: str
    name: str
    description: str
    type: ApplicationCommandTypes
    options: list[ApplicationCommandOption] | None = None
        
    def to_dict(self): 
        return asdict(self)

class InteractionCallbackType(IntEnum):
    PONG = 1
    CHANNEL_MESSAGE_WITH_SOURCE = 4 
    DEFERRED_CHANNEL_MESSAGE_WITH_SOURCE = 5 
    DEFERRED_UPDATE_MESSAGE = 6 
    UPDATE_MESSAGE = 7 
    APPLICATION_COMMAND_AUTOCOMPLETE_RESULT = 8 
    MODAL = 9 
    PREMIUM_REQUIRED = 10 
    LAUNCH_ACTIVITY = 12

@dataclass
class InteractionResponse: 
    type: int 
    data: dict | None

class Http: 
    DISCORD_BASE_URL = 'https://discord.com/api/v10' 
    OPENAI_BASE_URL = 'http://127.0.0.1:8080'

    def __init__(self, token: str): 
        self.token = token
        self.application_id: str | None = None
        self.session = None
        self.headers: dict = {
            'Authorization': f'Bot {self.token}', 
            'User-Agent': 'DiscordBot (https://github.com, v1.0)', 
            'Content-Type': 'application/json'
        }

    async def start(self): 
        if self.session:
            print("Session is already started. ")
        else: 
            self.session = httpx.AsyncClient(headers=self.headers)

    async def close(self): 
        if self.session: 
            await self.session.aclose()
            self.session = None
        else:
            print("Session is not found")

    async def get_gateway_url(self)-> dict:
        if not self.session: 
            raise Exception("Session is not found. ")
        
        url = f'{self.DISCORD_BASE_URL}/gateway/bot'
        response = await self.session.get(url, headers=self.headers)

        response.raise_for_status()

        data = response.json()
        gateway_url = f"{data['url']}/?v=10&encoding=json"
        return gateway_url
        
    async def send_message(self, channel_id: int, message: str) -> dict: 
        if not self.session: 
            raise Exception("Session is not found. ")

        url = f'{self.DISCORD_BASE_URL}/channels/{channel_id}/messages'
        json = {'content': message}

        response = await self.session.post(url=url, headers=self.headers, json=json)
        response.raise_for_status()
        return response.json()

    async def interaction_callback(self, interaction_id: int, interaction_token: str, data: str): 
        url = f'{self.DISCORD_BASE_URL}/interactions/{interaction_id}/{interaction_token}/callback'
        json = InteractionResponse(
            type=InteractionCallbackType.CHANNEL_MESSAGE_WITH_SOURCE, 
            data=data
        )

        response = await self.session.post(url=url, headers=self.headers, json=json)
        response.raise_for_status()
        return response.json()
    
    async def make_a_global_cmd(self) -> dict: 
        if not self.session: 
            raise Exception("Session is not found. ")
        if not self.application_id:
            raise Exception("Application ID is not found. ")

        url = f'{self.DISCORD_BASE_URL}/applications/{self.application_id}/guilds/{1542493954296127540}/commands'

        json = ApplicationCommandInput(
            name='llm', 
            type=ApplicationCommandTypes.CHAT_INPUT, 
            description='Run LLM in Discord. Select one whether you use local model or OpenAI-compatible API. ', 
            options=[
                ApplicationCommandOption(
                    name='local-model', 
                    description='Run by using local model. ', 
                    type=ApplicationCommandOptionType.SUB_COMMAND, 
                    options=[
                        ApplicationCommandOption(
                            name='url', 
                            description='llama.cpp address ', 
                            type=ApplicationCommandOptionType.STRING,  
                        )
                    ]
                ), 
                ApplicationCommandOption(
                    name='open-ai-compatible',
                    description='Run by using OpenAI-compatible API. ', 
                    type=ApplicationCommandOptionType.SUB_COMMAND, 
                    options=[
                                            
                    ]
                ), 
            ]
        ).to_dict()

        response = await self.session.post(url, headers=self.headers, json=json)
        response.raise_for_status()
        return response.json()