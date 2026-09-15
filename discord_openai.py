# Python 공식 라이브러리
from dataclasses import dataclass
from enum import Enum

# 사용자 라이브러리
from discord_http import Http


class LLMProvider(str, Enum): 
    LOCAL_MODEL = "local_model" 
    API = "api"


class OpenAI: 
    def __init__(self, http: Http, provider, base_url, openai_token=None, model_name=None): 
        self.http = http
        self.provider: LLMProvider = provider
        self.base_url = f'{base_url}/v1/chat/completions'

        self.openai_token = openai_token
        self.model_name = model_name

        self.messages = []

    async def send_message(self, prompt): 
        self.messages.append({'role': 'user', 'content': prompt})

        content = {
            'model' : self.model_name, 
            'messages' : self.messages,
        }

        response = await self.http.session.post(
            url=self.base_url, 
            headers=self.http.headers, 
            json=content, 
        )

        return response['choices'][0]['message']['content']