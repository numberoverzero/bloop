.. _streams:

Streams
^^^^^^^

Configure the Stream:

.. code-block:: python

    class User(BaseModel):
        class Meta:
            stream = {
                "include": ["new", "old"]
            }
        id = Column(UUID, hash_key=True)
        email = Column(String)
        verified = Column(Boolean)

`StreamViewType`__ maps to ``"include"`` and has four possible values:

.. code-block:: python

    {"keys"}
    {"new"}
    {"old"}
    {"new", "old"}

__ http://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/API_StreamDescription.html#DDB-Type-StreamDescription-StreamViewType

You can optionally check the `StreamLabel`__ of an existing Stream:

.. code-block:: python

    class Meta:
        stream = {
            "include": ["new", "old"],
            "label": "2016-08-29T03:26:22.376"
        }

__ http://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/API_StreamDescription.html#DDB-Type-StreamDescription-StreamLabel

If the table's StreamLabel doesn't match then ``Engine.bind`` will raise ``TableMismatch``.
If you don't provide ``"label"``, it won't be checked against an existing Stream.
