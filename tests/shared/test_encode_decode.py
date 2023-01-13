from ferdelance.shared.generate import (
    generate_asymmetric_key,
    RSAPrivateKey,
    RSAPublicKey
)
from ferdelance.shared.encode import (
    encode_to_transfer,
    encrypt,
    generate_hybrid_encryption_key,
    HybridEncrypter,
)
from ferdelance.shared.decode import (
    decode_from_transfer,
    decrypt,
    decrypt_hybrid_key,
    HybridDecrypter,
)

import logging
import random
import string
import os

LOG = logging.getLogger(__name__)


class TestEncodeDecode:

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

    def test_symmetric_key_generation(self):
        """Test the encrypting and decrypting of the symmetric key"""
        private_key: RSAPrivateKey = generate_asymmetric_key()
        public_key: RSAPublicKey = private_key.public_key()

        data, key_enc = generate_hybrid_encryption_key(public_key)

        key_dec = decrypt_hybrid_key(data, private_key)

        assert key_enc.iv == key_dec.iv
        assert key_enc.key == key_dec.key

    def test_stream_from_memory(self):
        """Test the encrypting and decrypting of a stream of bytes from a content in memory."""
        private_key: RSAPrivateKey = generate_asymmetric_key()
        public_key: RSAPublicKey = private_key.public_key()

        content = self._random_string(10000)

        enc = HybridEncrypter(public_key)
        dec = HybridDecrypter(private_key)

        chunks_encrypted: list[bytes] = []
        for chunk in enc.encrypt_to_stream(content):
            chunks_encrypted.append(chunk)

        dec_content = dec.decrypt_stream(iter(chunks_encrypted))

        assert content == dec_content

    def test_stream_from_file(self):
        """Test the encrypting and decrypting of a stream of bytes from a file."""
        private_key: RSAPrivateKey = generate_asymmetric_key()
        public_key: RSAPublicKey = private_key.public_key()

        enc = HybridEncrypter(public_key)
        dec = HybridDecrypter(private_key)

        content_from: str = self._random_string(7637)

        path_content_from: str = os.path.join('.', 'file_in.txt')
        path_content_to: str = os.path.join('.', 'file_out.txt')

        with open(path_content_from, 'w') as f:
            f.write(content_from)

        chunks_encrypted: list[bytes] = []
        for chunk in enc.encrypt_file_to_stream(path_content_from):
            chunks_encrypted.append(chunk)

        dec.decrypt_stream_to_file(iter(chunks_encrypted), path_content_to)

        with open(path_content_to, 'rb') as f:
            content_to = f.read().decode('utf8')

        os.remove(path_content_from)
        os.remove(path_content_to)

        assert content_from == content_to

    def test_hybrid_encryption(self):
        private_key: RSAPrivateKey = generate_asymmetric_key()
        public_key: RSAPublicKey = private_key.public_key()

        content: str = self._random_string(1234)

        enc = HybridEncrypter(public_key)
        dec = HybridDecrypter(private_key)

        secret: bytes = enc.encrypt(content)
        message: str = dec.decrypt(secret)

        assert content == message
