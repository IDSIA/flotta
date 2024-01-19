from __future__ import annotations

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

import os
import pickle


class SymmetricKey:
    def __init__(
        self,
        key: bytes | None = None,
        iv: bytes | None = None,
        key_size: int = 32,
        iv_size: int = 16,
        data: bytes | None = None,
    ) -> None:
        """Generates a new random key, initialization vector, and cipher for
        a symmetric encryption algorithm.

        :param key:
            If set, uses these bytes as key, otherwise generates a new key.
        :param iv:
            If set, uses these bytes as initialization vector, otherwise generates a new vector.
        :param key_size:
            Size of the key to generate.
        :param iv_size:
            Size of the initialization vector.
        """

        self.key: bytes
        self.iv: bytes

        if data is not None:
            d: dict[str, bytes] = pickle.loads(data)

            self.key = d["key"]
            self.iv = d["iv"]

        else:
            self.key = key if key else os.urandom(key_size)
            self.iv = iv if iv else os.urandom(iv_size)

        self.cipher: Cipher = Cipher(algorithms.AES(self.key), modes.CTR(self.iv), backend=default_backend())

    def bytes(self) -> bytes:
        return pickle.dumps(
            {
                "key": self.key,
                "iv": self.iv,
            }
        )

    def encryptor(self):
        """Get a encryptor from the cipher."""
        return self.cipher.encryptor()

    def decryptor(self):
        """Get a decryptor from the cipher."""
        return self.cipher.decryptor()
