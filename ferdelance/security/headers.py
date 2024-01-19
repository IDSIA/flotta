from pydantic import BaseModel


class SignedHeaders(BaseModel):
    # who sent the request
    source_id: str
    # who is the final receiver of the payload
    target_id: str
    # checksum of the unencrypted payload
    checksum: str
    # signature of the request
    signature: str
    # encryption algorithm to use
    encryption: str
    # extra key-value pairs
    extra: dict[str, str]
