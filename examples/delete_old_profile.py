from bloop import (Column, ConstraintViolation,
                   DateTime, Engine, Integer,
                   String)
import arrow
engine = Engine()


class UserProfile(engine.model):
    id = Column(String, hash_key=True)
    score = Column(Integer)
    last_login = Column(DateTime)
engine.bind()


def delete_old_profile(profile_id, after_load):
    """
    Force a constraint violation by changing last_login in the after_load
    function, or don"t modify it to see profile successfully deleted.
    """
    profile = UserProfile(id=profile_id)
    engine.load(profile)
    two_years_ago = arrow.now().replace(years=-2)
    if profile.last_login <= two_years_ago:
        print("Last logged in more than two years ago, trying to delete")
        after_load(profile_id)
        # WARNING: without a condition, someone could log in after we enter
        # this block and we'd delete their account immediately after they
        # logged in.
        condition = UserProfile.last_login <= two_years_ago
        try:
            engine.delete(profile, condition=condition)
        except ConstraintViolation:
            # We caught a race condition!  The profile's last_login no longer
            # meets the criteria we expected
            print("User not deleted (ConstraintViolation)")
        else:
            print("User deleted")


def login(profile_id):
    print("USER LOGGED IN")
    profile = UserProfile(id=profile_id)
    engine.load(profile)
    profile.last_login = arrow.now()
    engine.save(profile)


def noop(profile_id):
    pass


# Creatue user
print("Creating user")
engine.save(UserProfile(
    id="numberoverzero", last_login=arrow.now().replace(years=-3)))
# Modify the last_login in the middle of loading
delete_old_profile("numberoverzero", login)

# Re-create the user since last_login is now very recent
print("\nCreating user")
engine.save(UserProfile(
    id="numberoverzero", last_login=arrow.now().replace(years=-3)))
delete_old_profile("numberoverzero", noop)
