from typing import AsyncGenerator, Iterator
from abc import ABC, abstractmethod

from pathlib import Path

import aiofiles


class EncryptionAlgorithm(ABC):
    def encrypt(self, content: str | bytes) -> bytes:
        """Encrypt the whole content.

        :param content:
            Data to encrypt.
        :return:
            Encrypted input data in byte format.
        """
        enc_content: bytearray = bytearray()

        enc_content += self.start()
        enc_content += self.update(content)
        enc_content += self.end()

        return bytes(enc_content)

    def encrypt_file(self, path_in: Path, path_out: Path) -> str:
        """Encrypt a file from disk to another file on disk. This file can be decrypted
        only with a private key.

        :param path_in:
            Source file to encrypt.
        :param path_out:
            Destination path of the decrypted file.
        :raise:
            ValueError if there is no public key available.

        :return:
            Checksum of the original file.
        """
        with open(path_out, "wb") as w:
            w.write(self.start())
            with open(path_in, "rb") as r:
                while content := r.read():
                    w.write(self.update(content))
                w.write(self.end())

        return self.get_checksum()

    async def encrypt_file_a(self, path_in: Path, path_out: Path) -> str:
        """Async variant of `encrypt_file`.

        Encrypt a file from disk to another file on disk. This file can be decrypted
        only with a private key.

        :param path_in:
            Source file to encrypt.
        :param path_out:
            Destination path of the decrypted file.
        :raise:
            ValueError if there is no public key available.

        :return:
            Checksum of the original file.
        """
        async with aiofiles.open(path_out, "wb") as w:
            await w.write(self.start())
            async with aiofiles.open(path_in, "rb") as r:
                while content := await r.read():
                    await w.write(self.update(content))
                await w.write(self.end())

        return self.get_checksum()

    def encrypt_file_to_stream(self, in_path: Path, CHUNK_SIZE: int = 4096) -> Iterator[bytes]:
        """Generator function that encrypt an input file read from disk.

        :param in_path:
            Path on disk of the file to stream.
        :param CHUNK_SIZE:
            Size in bytes of each chunk transmitted to the client.
        :return:

        """
        yield self.start()

        with open(in_path, "rb") as f:
            while chunk := f.read(CHUNK_SIZE):
                yield self.update(chunk)

        yield self.end()

    async def encrypt_file_to_stream_a(self, in_path: Path, CHUNK_SIZE: int = 4096) -> AsyncGenerator[bytes, None]:
        """Async variant of `encrypt_file_to_stream`.

        Generator function that encrypt an input file read from disk.

        :param in_path:
            Path on disk of the file to stream.
        :param CHUNK_SIZE:
            Size in bytes of each chunk transmitted to the client.
        :return:

        """
        yield self.start()

        async with aiofiles.open(in_path, "rb") as f:
            while chunk := await f.read(CHUNK_SIZE):
                yield self.update(chunk)

        yield self.end()

    def encrypt_content_to_stream(self, content: str | bytes, CHUNK_SIZE: int = 4096) -> Iterator[bytes]:
        """Generator function that streams the given content.

        :param content:
            Content to stream in string format.
        :param CHUNK_SIZE:
            Size in bytes of each chunk transmitted to the client.
        :return:
            A stream of bytes
        """
        yield self.start()

        n = len(content)

        start: int = 0
        end: int = CHUNK_SIZE

        while True:
            chunk = content[start:end]

            if len(chunk) == 0:
                break

            yield self.update(chunk)

            start = end
            end = min(end + CHUNK_SIZE, n)

        yield self.end()

    async def encrypt_content_to_stream_a(
        self,
        content: str | bytes,
        CHUNK_SIZE: int = 4096,
    ) -> AsyncGenerator[bytes, None]:
        """Async variant of `encrypt_content_to_stream`.

        Generator function that streams the given content.

        :param content:
            Content to stream in string format.
        :param CHUNK_SIZE:
            Size in bytes of each chunk transmitted to the client.
        :return:
            A stream of bytes
        """
        yield self.start()

        n = len(content)

        start: int = 0
        end: int = CHUNK_SIZE

        while True:
            chunk = content[start:end]

            if len(chunk) == 0:
                break

            yield self.update(chunk)

            start = end
            end = min(end + CHUNK_SIZE, n)

        yield self.end()

    @abstractmethod
    def start(self) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def update(self, content: str | bytes) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def end(self) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def get_checksum(self) -> str:
        raise NotImplementedError()


class DecryptionAlgorithm(ABC):
    def decrypt(self, content: bytes) -> bytes:
        """Decrypt the whole content.

        :param content:
            Data to decrypt.
        :return:
           Decrypted input data in string format.
        """
        data: bytearray = bytearray()

        data.extend(self.start())
        data.extend(self.update(content))
        data.extend(self.end())

        return bytes(data)

    def decrypt_file(self, path_in: Path, path_out: Path) -> str:
        """Decrypt a file from disk to another file on disk. This file can must be
        encrypted with a valid public key.

        :param path_in:
            Source file to decrypt.
        :param path_out:
            Destination path of the decrypted file.

        :return:
            Checksum of the output file.
        """

        with open(path_out, "wb") as w:
            w.write(self.start())
            with open(path_in, "rb") as r:
                while content := r.read():
                    w.write(self.update(content))
                w.write(self.end())

        return self.get_checksum()

    async def decrypt_file_a(self, path_in: Path, path_out: Path) -> str:
        """Async variant of `decrypt_file`.

        Decrypt a file from disk to another file on disk. This file can must be
        encrypted with a valid public key.

        :param path_in:
            Source file to decrypt.
        :param path_out:
            Destination path of the decrypted file.

        :return:
            Checksum of the output file.
        """

        async with aiofiles.open(path_out, "wb") as w:
            await w.write(self.start())
            async with aiofiles.open(path_in, "rb") as r:
                while content := await r.read():
                    await w.write(self.update(content))
                await w.write(self.end())

        return self.get_checksum()

    def decrypt_stream_to_file(self, stream: Iterator[bytes], path_out: Path) -> str:
        """Consumer method takes an iterable of chunks produced by an Encryptor object.

        Decrypted data are save to disk at the given location.

        :param out_path:
            Location on disk where to save the decrypted content.
        :param stream:
            Iterable of bytes chunks (could be a list or an iterator or a stream) generated by
            the `encrypt_file_to_stream()` or `encrypt_to_stream()` method, to be decoded.

        :return:
            Checksum of the decrypted file.
        """
        with open(path_out, "wb") as f:
            data = self.start()
            f.write(data)

            for chunk in stream:
                data = self.update(chunk)
                f.write(data)

            data = self.end()
            f.write(data)

        return self.get_checksum()

    async def decrypt_stream_to_file_a(self, stream: AsyncGenerator[bytes, None], path_out: Path) -> str:
        """Asynchronous variant of `decrypt_stream_to_file`.

        Consumer method takes an iterable of chunks produced by an Encryptor object.

        Decrypted data are save to disk at the given location.

        :param out_path:
            Location on disk where to save the decrypted content.
        :param stream:
            Iterable of bytes chunks (could be a list or an iterator or a stream) generated by
            the `encrypt_file_to_stream()` or `encrypt_to_stream()` method, to be decoded.

        :return:
            Checksum of the decrypted file.
        """
        async with aiofiles.open(path_out, "wb") as f:
            await f.write(self.start())
            async for content in stream:
                await f.write(self.update(content))
            await f.write(self.end())

        return self.get_checksum()

    def decrypt_stream(self, stream: Iterator[bytes]) -> bytes:
        """Consumer method that takes an iterable of chunks produced by an Encryptor object.

        Decrypted data is stored in memory.

        :param stream:
            Iterable of bytes chunks (could be a list or an iterator or a stream) generated by
            the `encrypt_file_to_stream()` or `encrypt_to_stream()` method, to be decoded.
        :return:
            Decrypted content received.
        """
        data: bytearray = bytearray()

        data.extend(self.start())
        for chunk in stream:
            data.extend(self.update(chunk))
        data.extend(self.end())

        return data

    async def decrypt_stream_a(self, stream: AsyncGenerator[bytes, None]) -> bytes:
        """Asynchronous variant of `decrypt_stream`.

        Consumer method that takes an iterable of chunks produced by an Encryptor object.

        Decrypted data is stored in memory.

        :param stream:
            Iterable of bytes chunks (could be a list or an iterator or a stream) generated by
            the `encrypt_file_to_stream()` or `encrypt_to_stream()` method, to be decoded.
        :return:
            Decrypted content received.
        """
        data: bytearray = bytearray()

        data.extend(self.start())
        async for chunk in stream:
            data.extend(self.update(chunk))
        data.extend(self.end())

        return data

    @abstractmethod
    def start(self) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def update(self, content: bytes) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def end(self) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def get_checksum(self) -> str:
        raise NotImplementedError()
