# Verify that the package was built correctly so it can be imported.
# We use the same build setting in all other Python packages. This is why we're doing this check here.
from tests import constants

assert constants.i_am_true
