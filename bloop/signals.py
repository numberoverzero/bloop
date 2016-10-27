from .util import signal

before_create_table = signal("before_create_table")
before_create_table.__doc__ = """:class:`~blinker.base.Signal` sent before a model's backing table is created.

:param engine: :class:`~bloop.engine.Engine` creating the model's table.
:param model: The :class:`~bloop.models.BaseModel` **class** to create a table for.
"""

table_validated = signal("table_validated")
table_validated.__doc__ = """:class:`~blinker.base.Signal` sent after a model's expected table is validated.

This signal is sent before :data:`~bloop.signals.model_validated` and :data:`~bloop.signals.model_bound`.

:param model: The :class:`~bloop.models.BaseModel` **class** that was validated.
:param actual_description: sanitized dict from DynamoDB.
:param expected_description: dict the model expects.  May be a subset of ``actual_description``.
"""

object_loaded = signal("object_loaded")
object_loaded.__doc__ = """:class:`~blinker.base.Signal` sent after an object is loaded from DynamoDB.


:param engine: The :class:`~bloop.engine.Engine` that loaded the object.
:param obj: The `~bloop.models.BaseModel` loaded from DynamoDB.
"""

object_saved = signal("object_saved")
object_saved.__doc__ = """:class:`~blinker.base.Signal` sent after an object is saved to DynamoDB.


:param engine: The :class:`~bloop.engine.Engine` that saved the object.
:param obj: The `~bloop.models.BaseModel` saved to DynamoDB.
"""

object_deleted = signal("object_deleted")
object_deleted.__doc__ = """:class:`~blinker.base.Signal` sent after an object is deleted from DynamoDB.


:param engine: The :class:`~bloop.engine.Engine` that deleted the object.
:param obj: The `~bloop.models.BaseModel` deleted from DynamoDB.
"""

object_modified = signal("object_modified")
object_modified.__doc__ = """:class:`~blinker.base.Signal` sent after an object's attribute is set or deleted.

When the attribute's ``__del__`` is called, this is always sent; even if delete raised an exception.
When the attribute's ``__set__`` is called, this is only sent if an exception isn't raised.
These aren't symmetric because calling ``__del__`` signals intent to remove, which can't otherwise be expressed
on a column that wasn't loaded (for example, from a query on a keys-only projection).

:param column: The `~bloop.models.Column` that corresponds to the modified attribute.
:param obj: The `~bloop.models.BaseModel` that was modified.
:param value: The new value of the attribute.
"""


model_bound = signal("model_bound")
model_bound.__doc__ = """:class:`~blinker.base.Signal` sent after a model has been bound to an
:class:`~bloop.engine.Engine`.

This signal is sent after :data:`~bloop.signals.table_validated` and :data:`~bloop.signals.model_validated`.

:param engine: The :class:`~bloop.engine.Engine` that the model was bound to.
:param model: The :class:`~bloop.models.BaseModel` **class** that was bound.
"""


model_created = signal("model_created")
model_created.__doc__ = """:class:`~blinker.base.Signal` sent after a new model is defined.

:param model: The subclass of :class:`~bloop.models.BaseModel` that was created.
"""

model_validated = signal("model_validated")
model_validated.__doc__ = """:class:`~blinker.base.Signal` sent after a model is validated.

This signal is sent after :data:`~bloop.signals.table_validated` and before :data:`~bloop.signals.model_bound`.

:param engine: The :class:`~bloop.engine.Engine` that validated the model.
:param model: The :class:`~bloop.models.BaseModel` **class** that was validated.
"""
