from .util import signal

# Last chance to modify the model before its backing table is created
# sender: Engine
# model: Model class
before_create_table = signal("before_create_table")

# After a Model's expected table has been validated against the
#   actual table in DynamoDB
# sender: SessionWrapper
# model: Model class
# actual_description: dict
#   Unmodified table dict from DynamoDB
# expected_description: dict
#   The dict that Bloop expects for the Model
table_validated = signal("table_validated")

# After an object is loaded from DynamoDB
#   (Engine.load, Engine.query, Engine.scan)
# sender: Engine
# obj: Model instance
object_loaded = signal("object_loaded")

# After an object is saved to DynamoDB (Engine.save)
# sender: Engine
# obj: Model instance
object_saved = signal("object_saved")

# After an object is deleted in DynamoDB (Engine.delete)
# sender: Engine
# obj: Model instance
object_deleted = signal("object_deleted")

# After an attribute of an object is modified (__set__, __delete__)
# sender: None
# obj: Model instance
# column: Column instance
# value: Any
#   The new value of this column on the object
object_modified = signal("object_modified")


# After a model has been bound to its expected backing table in DynamoDB.
# sender: Engine
# model: Model class
model_bound = signal("model_bound")


# After a new Model is defined.
# sender: None
# model: Model class
model_created = signal("model_created")

# After a Model is validated, as part of the Engine.bind process.
# sender: Engine
# model: Model class
model_validated = signal("model_validated")
