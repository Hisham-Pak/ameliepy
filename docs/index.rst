.. ameliepy documentation master file, created by
   sphinx-quickstart on Sat Nov 22 09:26:39 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

ameliepy documentation
======================

Python database driver for `AmelieDB <https://amelielabs.io/docs/>`__.


Installation
============

Instructions to install the ameliepy driver.

.. code-block:: bash

   pip install ameliepy

Usage
=====

Basic usage examples for connecting to AmelieDB and executing queries.

.. code-block:: python

   import amelie

   with amelie.connect(host="http://localhost:3485") as conn:
      with conn.cursor() as cursor:
         cursor.execute("SELECT 1")
         row = cursor.fetchone()
         print(row)
         # Will output: 1

Passing Arguments to Queries
-----------------------------

You can pass arguments to your SQL queries using placeholders in the query string using
`format or pyformat <https://peps.python.org/pep-0249/#paramstyle>`__ styles.

Format Style
^^^^^^^^^^^^
.. code-block:: python

   import amelie

   with amelie.connect(host="http://localhost:3485") as conn:
      with conn.cursor() as cursor:
         cursor.execute("SELECT * FROM users WHERE id = %s", (1,))
         row = cursor.fetchone()
         print(row)  # Will output the user with id 1

Pyformat Style
^^^^^^^^^^^^^^
.. code-block:: python

   import amelie

   with amelie.connect(host="http://localhost:3485") as conn:
      with conn.cursor() as cursor:
         cursor.execute("SELECT * FROM users WHERE id = %(user_id)s", {'user_id': 1})
         row = cursor.fetchone()
         print(row)  # Will output the user with id 1

.. danger::
   SQL Injection: Only use the methods above to pass arguments only in second parameter
   of ``.execute(query, params)``.

   NEVER DO THIS:

   .. code-block:: python

      cursor.execute("SELECT * FROM users WHERE id = %s" % 1)
