from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.asymmetric.padding import PSS, MGF1
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey


def sign(private_key: RSAPrivateKey, content: str, encoding: str = "utf8") -> str:
    signature = private_key.sign(
        content.encode(encoding),
        PSS(mgf=MGF1(SHA256()), salt_length=PSS.MAX_LENGTH),
        SHA256(),
    )

    return signature.decode(encoding)


def verify(public_key: RSAPublicKey, message: str, signature: str, encoding: str = "utf8") -> None:
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
    public_key.verify(
        signature.encode(encoding),
        message.encode(encoding),
        PSS(mgf=MGF1(SHA256()), salt_length=PSS.MAX_LENGTH),
        SHA256(),
    )
