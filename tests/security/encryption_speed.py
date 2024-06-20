from flotta.security.keys import PrivateKey, PublicKey
from flotta.security.algorithms import HybridDecryptionAlgorithm, HybridEncryptionAlgorithm

import numpy as np

import string
import time


if __name__ == "__main__":
    private_key: PrivateKey = PrivateKey()
    public_key: PublicKey = private_key.public_key()

    iterations = 10

    CHARS = np.array(list(string.printable))

    for size in (1, 5, 10, 50, 100, 250, 500, 1000):
        mean_enc_time = 0.0
        mean_dec_time = 0.0

        content = "".join(np.random.choice(CHARS, size=1000 * 1000 * size))

        print(f"testing for size {size}MB")
        for _ in range(iterations):
            enc = HybridEncryptionAlgorithm(public_key)
            dec = HybridDecryptionAlgorithm(private_key)

            start_time = time.time()
            secret: bytes = enc.encrypt(content)
            end_time_1 = time.time()
            message: str = dec.decrypt(secret).decode()
            end_time_2 = time.time()

            mean_enc_time += end_time_1 - start_time
            mean_dec_time += end_time_2 - end_time_1

            assert content == message

        mean_enc_time /= iterations
        mean_dec_time /= iterations

        print(f"Encryption time mean: {mean_enc_time:.4}s")
        print(f"Decryption time mean: {mean_dec_time:.4}s")
