import pytest
from uvicorn.protocols.http import flow_control
from uvicorn.protocols.http import h11_impl
from uvicorn.protocols.http import httptools_impl

from hippius_s3.uvicorn_tuning import raise_receive_high_water


MODULES = (flow_control, h11_impl, httptools_impl)


@pytest.fixture(autouse=True)
def _restore_stock_limits():
    originals = [(m, m.HIGH_WATER_LIMIT) for m in MODULES]
    yield
    for m, v in originals:
        m.HIGH_WATER_LIMIT = v


def test_the_limit_lands_in_all_three_module_namespaces() -> None:
    """Both protocol impls from-import their own copy of the constant, so patching
    only flow_control changes nothing at request time — this pins all three."""
    raise_receive_high_water(1048576)
    assert [m.HIGH_WATER_LIMIT for m in MODULES] == [1048576, 1048576, 1048576]


def test_zero_leaves_uvicorn_stock() -> None:
    stock = [m.HIGH_WATER_LIMIT for m in MODULES]
    raise_receive_high_water(0)
    assert [m.HIGH_WATER_LIMIT for m in MODULES] == stock


def test_negative_leaves_uvicorn_stock() -> None:
    stock = [m.HIGH_WATER_LIMIT for m in MODULES]
    raise_receive_high_water(-1)
    assert [m.HIGH_WATER_LIMIT for m in MODULES] == stock


def test_the_impls_read_the_name_at_request_time() -> None:
    """The whole patch rests on the impls doing a module-global lookup per request
    rather than binding the value at class definition; if uvicorn ever changes
    that, this test breaks before production does."""
    import inspect

    for impl in (h11_impl, httptools_impl):
        src = inspect.getsource(impl)
        assert "HIGH_WATER_LIMIT" in src.split("import", 1)[1].split("\n", 1)[1], (
            f"{impl.__name__} no longer references HIGH_WATER_LIMIT outside its imports; "
            "raise_receive_high_water is silently dead"
        )
