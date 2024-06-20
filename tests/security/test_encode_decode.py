from flotta.logging import get_logger
from flotta.security.keys.asymmetric import PrivateKey, PublicKey
from flotta.security.keys.symmetric import SymmetricKey
from flotta.security.utils import decode_from_transfer, encode_to_transfer

from tests.utils import random_string


LOG = get_logger(__name__)


def test_transfer():
    """Test an encoding and decoding of a string. This is just a change of encoding and not an encrypting."""
    content: str = random_string(10000)

    content_encoded: str = encode_to_transfer(content)
    content_decoded: str = decode_from_transfer(content_encoded)

    assert content == content_decoded


def test_encrypt_decrypt():
    """Uses a pair of asymmetric keys to encrypt and decrypt a small message."""
    private_key: PrivateKey = PrivateKey()
    public_key: PublicKey = private_key.public_key()

    content: str = random_string(250)

    content_encrypted: bytes = public_key.encrypt(content)
    content_decrypted: bytes = private_key.decrypt(content_encrypted)

    assert content == content_decrypted.decode()


def test_symmetric_key_generation():
    """Test the encrypting and decrypting of the symmetric key"""
    private_key: PrivateKey = PrivateKey()
    public_key: PublicKey = private_key.public_key()

    key: SymmetricKey = SymmetricKey()

    key_enc = public_key.encrypt(key.bytes())
    key_dec = SymmetricKey(data=private_key.decrypt(key_enc))

    assert key.iv == key_dec.iv
    assert key.key == key_dec.key
