from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.asymmetric.padding import PSS, MGF1
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey

import base64


def sign(private_key: RSAPrivateKey, data: str | bytes, encoding: str = "utf8") -> str:
    if isinstance(data, str):
        data = data.encode(encoding)

    signature = private_key.sign(
        data,
        PSS(mgf=MGF1(SHA256()), salt_length=PSS.MAX_LENGTH),
        SHA256(),
    )

    return base64.b64encode(signature).decode(encoding)


def verify(public_key: RSAPublicKey, data: str | bytes, signature: str | bytes, encoding: str = "utf8") -> None:
    """Verify that the given message has been signed with the private part of
    the given public key, meaning that the signature is valid.

    Args:
        public_key (RSAPublicKey):
            Public key to use.
        message (str):
            Received message to verify.
        signature (str):
            Received signature to compare with.
        encoding (str, optional):
            Encodign to use.
            Defaults to "utf8".

        Raises:
            InvalidSignature if the signature is not valid.
    """
    if isinstance(data, str):
        data = data.encode(encoding)

    if isinstance(signature, str):
        signature = signature.encode(encoding)

    signature = base64.b64decode(signature)

    public_key.verify(
        signature,
        data,
        PSS(mgf=MGF1(SHA256()), salt_length=PSS.MAX_LENGTH),
        SHA256(),
    )
