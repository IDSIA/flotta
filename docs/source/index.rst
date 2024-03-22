.. image:: _static/images/hero-bg-blue.png
   	:scale: 7 %
   	:align: left

==========================================================
Ferdelance: A Federated Learning Framework
==========================================================


*Ferdelance* is a **distributed framework** intended to be used both as a workbench to develop new distributed algorithm within a Federated Learning based environment, and perform distributed statistical analysis on private data.

Federated Learning is a Machine Learning approach that allows for training models across decentralized devices or servers while keeping the data localized, increasing the privacy of data holders.
Instead of collecting data from various sources and centralizing it in one location for training, federated learning enables model training directly on the devices where the data resides.
In federated learning the training of models is distributed across a series of data holders (client nodes) that have direct and exclusive access to their data.
The particularity of this approach is that the training data never leave these nodes, while only aggregated data, such as model parameters, are exchanged to build an aggregated model.

The current implementation support both a centralized setup, where model's parameters are sent from the client nodes to an aggregation node, and distributed setup, where a model is shared across multiple nodes and multiple model aggregation can happen on different nodes.

The intent of this framework is to develop a solution that enable researcher to develop and test new machine learning models in a federated learning context without interacting directly with the data.
The framework wraps a familiar set of Python packages and libraries, such as Scikit-Learn and Pandas.
This allows researchers to quickly setup data extraction pipeline, following the *Extract-Transform-Load* paradigm, and build models or analyze data.


.. .. toctree::

..   Home <self>

.. toctree::
   :includehidden:
   :maxdepth: 1
   :caption: Quick Start

   quickstart/quickstart

.. toctree::
   :includehidden:
   :maxdepth: 1
   :caption: Structure

   structure/topology
   structure/security
   structure/modes
   structure/node
   structure/workbench

.. toctree::
   :includehidden:
   :maxdepth: 1
   :caption: Workflow

   workflow/overview
   workflow/scheduling
   workflow/entities

.. toctree::
   :includehidden:
   :maxdepth: 1
   :caption: Development

   development/development
   development/testing

.. toctree::
   :includehidden:
   :maxdepth: 1
   :caption: Other

   other/contacts


.. Indices and tables
  ==================

  * :ref:`genindex`
  * :ref:`modindex`
  * :ref:`search`
  

.. role:: bash(code)
   :language: bash
.. role:: python(code)
   :language: python3