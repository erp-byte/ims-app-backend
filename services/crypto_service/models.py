from pydantic import BaseModel


class EncryptedRequest(BaseModel):
    payload: str


class EncryptedResponse(BaseModel):
    payload: str
