from dask_gateway_htcondor.htcondor import (
    htcondor_create_execution_script,
    htcondor_memory_format,
)

import pytest


def test_htcondor_create_execution_script(mocker):
    mock_open = mocker.mock_open()
    mocker.patch("builtins.open", mock_open)

    htcondor_create_execution_script(
        execution_script="test.sh",
        setup_command="init_test.sh",
        execution_command="execution_test.sh",
    )

    mock_open().write.assert_called_once_with(
        "#!/bin/sh\ninit_test.sh\nexecution_test.sh"
    )


def test_htcondor_memory_format():
    assert 1 == htcondor_memory_format(1048576)
    assert 1 == htcondor_memory_format(1000)
    assert 10 == htcondor_memory_format(10485760)

    with pytest.raises(AssertionError, match=r"^Memory should be a positive value$"):
        htcondor_memory_format(-1)

    with pytest.raises(TypeError):
        htcondor_memory_format("invalid")
