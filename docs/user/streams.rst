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
