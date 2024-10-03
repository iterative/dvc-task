dvc-task
========

|PyPI| |Status| |Python Version| |License|

|Tests| |Codecov| |pre-commit| |Black|

.. |PyPI| image:: https://img.shields.io/pypi/v/dvc-task.svg
   :target: https://pypi.org/project/dvc-task/
   :alt: PyPI
.. |Status| image:: https://img.shields.io/pypi/status/dvc-task.svg
   :target: https://pypi.org/project/dvc-task/
   :alt: Status
.. |Python Version| image:: https://img.shields.io/pypi/pyversions/dvc-task
   :target: https://pypi.org/project/dvc-task
   :alt: Python Version
.. |License| image:: https://img.shields.io/pypi/l/dvc-task
   :target: https://opensource.org/licenses/Apache-2.0
   :alt: License
.. |Tests| image:: https://github.com/iterative/dvc-task/actions/workflows/tests.yml/badge.svg
   :target: https://github.com/iterative/dvc-task/actions?workflow=Tests
   :alt: Tests
.. |Codecov| image:: https://codecov.io/gh/iterative/dvc-task/branch/main/graph/badge.svg
   :target: https://app.codecov.io/gh/iterative/dvc-task
   :alt: Codecov
.. |pre-commit| image:: https://img.shields.io/badge/pre--commit-enabled-lreen?logo=pre-commit&logoColor=white
   :target: https://github.com/pre-commit/pre-commit
   :alt: pre-commit
.. |Black| image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black
   :alt: Black


dvc-task is a library for queuing, running and managing background jobs
(processes) from standalone Python applications. dvc-task is built on Celery_,
but does not require a full AMQP messaging server (or any other "heavy" servers
which are traditionally used as Celery brokers).


Features
--------

* ``dvc_task.proc`` module for running and managing background processes in
  Celery tasks
* Preconfigured Celery app intended for use in standalone desktop
  applications

  * Uses Kombu_ filesystem transport as the message broker, and the standard
    filesystem Celery results backend
  * Allows standalone applications to make use of Celery without the use of
    additional messaging and results backend servers
* Preconfigured "temporary" Celery worker which will automatically terminate
  itself when the Celery queue is empty

  * Allows standalone applications to start Celery workers as needed directly
    from Python code (as opposed to requiring a "run-forever" daemonized
    CLI ``celery`` worker)


Requirements
------------

* Celery 5.3 or later
* Kombu 5.3 or later

Note: Windows is not officially supported in Celery, but dvc-task is tested on
Windows (and used in DVC on Windows).


Installation
------------

You can install *dvc-task* via pip_ from PyPI_:

.. code:: console

   $ pip install dvc-task


Usage
-----

Processes (``dvc_task.proc``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The process module provides a simple API for managing background processes in
background tasks. Background processes are run in Celery tasks, but process
state is stored separately from Celery, so information about managed processes
can be accessed from outside of the Celery producer or consumer application.

After you have configured a Celery application, jobs can be queued (and run) via
``ProcessManager.run`` (which returns a signature for the ``proc.tasks.run``
Celery task):

.. code-block:: python

    from dvc_task.proc import ProcessManager

    @app.task
    def my_task():
        manager = ProcessManager(wdir=".")
        manager.run(["echo", "hello world"], name="foo").delay()

The ``ProcessManager`` will create a subdirectory in ``wdir`` for each managed process.

.. code-block:: none

    $ tree .
    .
    └── 25mYD6MyLNewXXdMVYCCr3
        ├── 25mYD6MyLNewXXdMVYCCr3.json
        ├── 25mYD6MyLNewXXdMVYCCr3.out
        └── 25mYD6MyLNewXXdMVYCCr3.pid
    1 directory, 3 files

At a minimum, the directory will contain ``<id>.pid`` and ``<id>.json`` files.

* ``<id>.json``: A JSON file describing the process containing the following dictionary keys:
    * ``pid``: Process PID
    * ``stdout``: Redirected stdout file path for the process (redirected to
      ``<id>.out`` by default)
    * ``stderr``: Redirected stderr file path for the process (stderr is
      redirected to ``stdout`` by default)
    * ``stdin``: Redirected stdin file path for the process (interactive
      processes are not yet supported, stdin is currently always ``null``)
    * ``returncode``: Return code for the process (``null`` if the process
      has not exited)
* ``<id>.pid``: A standard pidfile containing only the process PID

``ProcessManager`` instances can be created outside of a Celery task to manage
and monitor processes as needed:

.. code-block:: python

    >>> from dvc_task.proc import ProcessManager
    >>> manager = ProcessManager(wdir=".")
    >>> names = [name for name, _info in manager.processes()]
    ['25mYD6MyLNewXXdMVYCCr3']
    >>> for line in manager.follow(names[0]):
    ...     print(line)
    ...
    hello world

Celery Workers (``dvc_task.worker``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

dvc-task includes a pre-configured Celery worker (``TemporaryWorker``) which
can be started from Python code. The ``TemporaryWorker`` will consume Celery
tasks until the queue is empty. Once the queue is empty, the worker will wait
up until a specified timeout for new tasks to be added to the queue. If the
queue remains empty after the timeout expires, the worker will exit.

To instantiante a worker with a 60-second timeout, with the Celery worker name
``my-worker-1``:

.. code-block:: python

    >>> from dvc_task.worker import TemporaryWorker
    >>> worker = TemporaryWorker(my_app, timeout=60)
    >>> worker.start("my-worker-1")

Note that ``worker.start`` runs the Celery worker within the calling thread.

Celery Applications (``dvc_task.app``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

dvc-task includes a pre-configured Celery application (``FSApp``) which uses
the Kombu filesystem transport as the Celery broker along with the Celery
filesystem results storage backend. ``FSApp`` is intended to be used in
standalone Python applications where a traditional Celery producer/consumer
setup (with the appropriate messaging and storage backends) is unavailable.

.. code-block:: python

    >>> from dvc_task.app import FSApp
    >>> my_app = FSApp(wdir=".")

``FSApp`` provides iterators for accessing Kombu messages which are either
waiting in the queue or have already been processed. This allows the caller
to access Celery task information without using the Celery ``inspect`` API
(which is only functional when a Celery worker is actively running).

.. code-block:: python

    >>> for msg in my_app.iter_processed():
    ...     msg
    <Message object at 0x102e7f0d0 with details {'state': 'RECEIVED', 'content_type': 'application/json', 'delivery_tag': '0244c11a-1bcc-47fc-8587-66909a55fdc6', ...}>
    <Message object at 0x1027fd4c0 with details {'state': 'RECEIVED', 'content_type': 'application/json', 'delivery_tag': '491415d1-9527-493a-a5d7-88ed355da77c', ...}>
    <Message object at 0x102e6f160 with details {'state': 'RECEIVED', 'content_type': 'application/json', 'delivery_tag': 'ea6ab7a4-0398-42ab-9f12-8da1f8e12a8a', ...}>
    <Message object at 0x102e6f310 with details {'state': 'RECEIVED', 'content_type': 'application/json', 'delivery_tag': '77c4a335-2102-4bee-9cb8-ef4d8ef9713f', ...}>

Contributing
------------

Contributions are very welcome.
To learn more, see the `Contributor Guide`_.


License
-------

Distributed under the terms of the `Apache 2.0 license`_,
*dvc-task* is free and open source software.


Issues
------

If you encounter any problems,
please `file an issue`_ along with a detailed description.


.. _0282e14: https://github.com/celery/kombu/commit/0282e1419fad98da5ae956ff38c7e87e539889ac
.. _Apache 2.0 license: https://opensource.org/licenses/Apache-2.0
.. _Celery: https://github.com/celery/celery
.. _Kombu: https://github.com/celery/kombu
.. _PyPI: https://pypi.org/
.. _file an issue: https://github.com/iterative/dvc-task/issues
.. _pip: https://pip.pypa.io/
.. github-only
.. _Contributor Guide: CONTRIBUTING.rst
