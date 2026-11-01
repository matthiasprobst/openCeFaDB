"""Guard the installed distribution's compatibility boundaries."""
from importlib.metadata import distribution

import pytest
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from packaging.version import Version


def is_bounded_below(requirement, boundary):
    boundary = Version(boundary)
    for spec in requirement.specifier:
        if "*" in spec.version:
            continue
        version = Version(spec.version)
        if spec.operator == "<" and version <= boundary:
            return True
        if spec.operator in {"<=", "=="} and version < boundary:
            return True
        if spec.operator == "~=":
            prefix = list(version.release[:-1])
            prefix[-1] += 1
            if Version(".".join(map(str, prefix))) <= boundary:
                return True
    return False


PACKAGE = 'opencefadb'
BOUNDARIES = [('ontolutils', '0.28.0'), ('h5rdmtoolbox', '3.0.0'), ('ssnolib', '3.0.0'), ('gldb', '3.0.0'), ('streamlit', '2.0.0')]


@pytest.mark.parametrize("dependency,boundary", BOUNDARIES)
def test_installed_dependency_has_finite_compatibility_boundary(dependency, boundary):
    metadata = distribution(PACKAGE)
    requirements = [Requirement(value) for value in metadata.requires or []]
    matches = [req for req in requirements
               if canonicalize_name(req.name) == canonicalize_name(dependency)]
    assert matches, f"Missing declared dependency: {dependency}"
    for req in matches:
        assert req.url is None, f"Floating provider reference: {req}"
        assert is_bounded_below(req, boundary), f"Unbounded API family: {req}"


def test_optional_feature_declares_its_dependency():
    requirements = [Requirement(value) for value in distribution(PACKAGE).requires or []]
    req = next(req for req in requirements if canonicalize_name(req.name) == 'streamlit')
    assert req.marker is not None
    assert req.marker.evaluate({"extra": 'viewer'})
    assert not req.marker.evaluate({"extra": ""})
