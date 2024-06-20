==============================
Security and Identification
==============================

Data encryption
==============================

Exchange of data between two nodes is encrypted.
In this case, *data* is not limited to resources and model parameters but refers also to all the communication and exchange of metadata that happens between two nodes.

Encryption is done using two algorithms.
The first is an encryption algorithm with asymmetric keys.
Each node, included the workbench, at startup search for a private key that will be used to sign and encrypt data.
The signature of data is used to ensure that the author of a data payload has been sent by the real author of the content, while the encryption algorithm make sure that only the real receiver of a package can read the data.

.. Note:
   It is possible to create and use OpenSSH private keys, since this is the format used by the framework.

The second encryption algorithm is used when there is the need to exchange a substantial quantity of data. This algorithm still uses the asymmetric algorithm to encrypt a symmetric key that will be used to encrypt the data. 
In this ways there is no limit to the amount of data that can be exchanged between two nodes.

.. Note:
   A new symmetric key is generated at each exchange.

When a new node connect to the network for the first time, it shares its public key to all other component of the network.
To join a network, a node requires the base url of a join node.
Each node offers the ``/key`` endpoint where the public key is freely available.


Node Identification
==============================

.. image:: ../_static/images/flotta_exchange_full.png
   :scale: 60 %
   :align: center
   :target: https://www.idsia.ch/

The above image show how the Identification of the author and the security mechanism works.
Note that the exchange between node ``A`` and ``B`` works in both direction: not only the requests are encrypted and checked using this workflow, but also the responses.
