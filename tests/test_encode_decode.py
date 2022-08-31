from ferdelance_shared.generate import (
    generate_asymmetric_key,
    RSAPrivateKey,
    RSAPublicKey
)
from ferdelance_shared.encode import (
    encode_to_transfer,
    encrypt,
    stream_encrypt_file,
)
from ferdelance_shared.decode import (
    decode_from_transfer,
    decrypt,
    decrypt_stream,
)

import logging
import random
import string
import os

LOG = logging.getLogger(__name__)


class TestEncodeDecode:

    def setup_method(self):
        LOG.info('setting up')

        random.seed(42)

    def _random_string(self, length: int) -> str:
        return ''.join(random.choice(string.ascii_letters) for _ in range(length))

    def test_transfer(self):
        """Test an encoding and decoding of a string. This is just a change of encoding and not an encrypting."""
        content: str = self._random_string(10000)

        content_encoded: str = encode_to_transfer(content)
        content_decoded: str = decode_from_transfer(content_encoded)

        assert content == content_decoded

    def test_encrypt_decrypt(self):
        """Uses a pair of asymmetric keys to encrypt and decrypt a small message."""
        private_key: RSAPrivateKey = generate_asymmetric_key()
        public_key: RSAPublicKey = private_key.public_key()

        content: str = self._random_string(250)

        content_encrypted: str = encrypt(public_key, content)
        content_decrypted: str = decrypt(private_key, content_encrypted)

        assert content == content_decrypted

    def test_stream(self):
        """Test the encrypting and decrypting of a stream of bytes from a file"""
        private_key: RSAPrivateKey = generate_asymmetric_key()
        public_key: RSAPublicKey = private_key.public_key()

        content = self._random_string(7637)
        path_content = 'file.txt'

        with open(path_content, 'w') as f:
            f.write(content)

        chunks_encrypted: list[bytes] = [chunk for chunk in stream_encrypt_file(path_content, public_key)]
        chunks_decrypted: list[bytes] = [chunk for chunk in decrypt_stream(chunks_encrypted, private_key)]

        os.remove(path_content)

        assert type(chunks_encrypted[0]) == bytes
        assert type(chunks_decrypted[0]) == bytes

        assert content == b''.join(chunks_decrypted).decode('utf8')
