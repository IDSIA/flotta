__all__ = [
    "Algorithm",
    "DecryptionAlgorithm",
    "EncryptionAlgorithm",
    "NoDecryptionAlgorithm",
    "NoEncryptionAlgorithm",
    "HybridDecryptionAlgorithm",
    "HybridEncryptionAlgorithm",
]

from enum import Enum

from .core import DecryptionAlgorithm, EncryptionAlgorithm
from .plain import NoDecryptionAlgorithm, NoEncryptionAlgorithm
from .hybrid import HybridDecryptionAlgorithm, HybridEncryptionAlgorithm

from flotta.security.keys import PrivateKey, PublicKey


class Algorithm(Enum):
    NO_ENCRYPTION = (NoEncryptionAlgorithm, NoDecryptionAlgorithm)
    HYBRID = (HybridEncryptionAlgorithm, HybridDecryptionAlgorithm)

    def encrypted(self) -> bool:
        return self.name != "NO_ENCRYPTION"

    def enc(self, public_key: PublicKey, encoding: str = "utf8") -> EncryptionAlgorithm:
        return self.value[0](public_key, encoding=encoding)

    def dec(self, private_key: PrivateKey, encoding: str = "utf8") -> DecryptionAlgorithm:
        return self.value[1](private_key, encoding=encoding)
