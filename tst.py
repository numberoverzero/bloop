from bloop.engine import Engine
from bloop.column import DirtyColumn
from bloop.types import StringType, NumberType, BooleanType

engine = Engine()

class Person(engine.model):
    first = DirtyColumn(StringType)
    last = DirtyColumn(StringType)
    age = DirtyColumn(NumberType)
    alive = DirtyColumn(BooleanType)

    def __str__(self):
        fmt = "Person(first={}, last={}, age={}, alive={})"
        return fmt.format(self.first, self.last, self.age, self.alive)


person = Person(first="John", last="Smith", age=25, alive=True)
wire = engine.dump(Person, person)
same_person = engine.load(Person, wire)

print("\nDynamo wire: {}\nPython model: {}\n".format(wire, same_person))
