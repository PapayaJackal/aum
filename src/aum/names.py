"""Human-readable name generator for job IDs.

Produces jellybean/ice-cream inspired ``adjective_flavour`` names with a
short random suffix to avoid collisions (e.g. ``fuzzy_pistachio_a3f``).
"""

import random
import secrets
import string

_LEFT: list[str] = [
    "bitter", "bright", "bubbly", "buttery", "candied", "chunky", "creamy",
    "crispy", "crunchy", "crystal", "dipped", "dreamy", "fizzy", "fluffy",
    "frosty", "frozen", "fruity", "fudgy", "fuzzy", "glazed", "gooey",
    "golden", "icy", "jelly", "juicy", "layered", "malted", "melted",
    "minty", "nutty", "peachy", "puffy", "rich", "rippled", "roasted",
    "salty", "silky", "smooth", "soft", "spiced", "sticky", "sugary",
    "sunny", "swirled", "tangy", "toasty", "tropical", "velvety", "warm",
    "whipped", "wild", "zesty",
]

_RIGHT: list[str] = [
    "almond", "apricot", "banana", "berry", "biscuit", "blueberry",
    "brownie", "butterscotch", "caramel", "cashew", "cherry", "chestnut",
    "chocolate", "cinnamon", "clementine", "cobbler", "coconut", "coffee",
    "cookie", "cranberry", "custard", "espresso", "fig", "fudge",
    "ganache", "ginger", "guava", "hazelnut", "honeycomb", "lemon",
    "licorice", "lychee", "macaron", "mango", "maple", "marshmallow",
    "meringue", "mocha", "nougat", "orange", "passionfruit", "peach",
    "peanut", "pecan", "peppermint", "pistachio", "plum", "praline",
    "pumpkin", "raspberry", "rhubarb", "sorbet", "strawberry", "sundae",
    "toffee", "truffle", "vanilla", "waffle", "walnut",
]

_SUFFIX_LEN = 3


def generate_name() -> str:
    """Return a unique human-readable name like ``fuzzy_pistachio_a3f``."""
    adj = random.choice(_LEFT)  # noqa: S311
    noun = random.choice(_RIGHT)  # noqa: S311
    suffix = "".join(secrets.choice(string.ascii_lowercase + string.digits) for _ in range(_SUFFIX_LEN))
    return f"{adj}_{noun}_{suffix}"
