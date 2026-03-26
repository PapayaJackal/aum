from aum.names import generate_name, _LEFT, _RIGHT


class TestGenerateName:
    def test_format(self):
        name = generate_name()
        parts = name.split("_")
        assert len(parts) == 3, f"Expected adjective_noun_suffix, got {name}"
        adj, noun, suffix = parts
        assert adj in _LEFT
        assert noun in _RIGHT
        assert len(suffix) == 3

    def test_uniqueness(self):
        names = {generate_name() for _ in range(200)}
        assert len(names) == 200, "Expected 200 unique names"
