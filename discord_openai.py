# Python 공식 라이브러리
from enum import Enum
from dataclasses import dataclass


class LLMProvider(str, Enum): 
    LOCAL_MODEL = "local_model" 
    API = "api"


class OpenAI: 
    def __init__(self): 
        self.provider: LLMProvider = None
        self.model_name: str | None = None

