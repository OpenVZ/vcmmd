import mock
import pytest

import vcmmd.util.misc
import psutil


@pytest.mark.parametrize('as_json,expected_result', [
    (True, '{\n    "31": false, \n    "1": 3\n}'),
    (False, "{31: False, '1': 3}"),
])
def test_print_dict(as_json, expected_result):
    input_dict = {'1': 3, 31: False}

    assert vcmmd.util.misc.print_dict(input_dict, j=as_json) == expected_result


def test_print_dict():
    d = {'1': 3, 31: False}
    expected_json = '{\n    "31": false, \n    "1": 3\n}'
    expected_dict = "{31: False, '1': 3}"

    rv_json = vcmmd.util.misc.print_dict(d, j=True)
    rv_dict = vcmmd.util.misc.print_dict(d)

    assert rv_json == expected_json
    assert rv_dict == expected_dict


@pytest.mark.parametrize('input,expected_result', [
    ('1-9', [1, 2, 3, 4, 5, 6, 7, 8, 9]),
    ('0-1', [0, 1]),
    ('1-1', [1]),
    ('9-11', [9, 10, 11]),
    ('12-10', [10, 11, 12]),
    ('2901', [2901]),
    ('   1-2\n', [1, 2]),
    ('', []),
])
def test_parse_range_ok(input, expected_result):
    assert vcmmd.util.misc.parse_range(input) == expected_result


@pytest.mark.parametrize('input', ['-1-9', '2-', '1-2-3', '-'])
def test_parse_range_bad_input(input):
    with pytest.raises(ValueError):
        vcmmd.util.misc.parse_range(input)


@pytest.mark.parametrize('input,expected_result', [
    ('1-2,5-9', [1, 2, 5, 6, 7, 8, 9]),
    ('0,1,2,3', [0, 1, 2, 3]),
    ('9-11,12-10,2901', [9, 10, 11, 12, 2901]),
    ('', []),
])
def test_parse_range_list_ok(input, expected_result):
    assert vcmmd.util.misc.parse_range_list(input) == expected_result


@mock.patch('psutil.process_iter')
def test_get_cs_num_no_cs_processes(mock_process_iter):
    mock_process_iter.return_value = [mock.MagicMock(), mock.MagicMock()]

    cs_num = vcmmd.util.misc.get_cs_num()

    assert cs_num == 0


@mock.patch('psutil.process_iter')
def test_get_cs_num_one_cs_process(mock_process_iter):
    pr1 = mock.MagicMock()
    pr1.cmdline = mock.MagicMock(return_value=['/usr/bin/csd'])
    mock_process_iter.return_value = [pr1, mock.MagicMock()]

    cs_num = vcmmd.util.misc.get_cs_num()

    assert cs_num == 1


@mock.patch('psutil.process_iter')
def test_get_cs_num_psutil_raises_exception(mock_process_iter):
    pr1 = mock.MagicMock()
    pr2 = mock.MagicMock()
    pr1.cmdline = mock.MagicMock(return_value=['ls'], side_effect=psutil.NoSuchProcess(1, msg='fake_error'))
    pr2.cmdline = mock.MagicMock(return_value=['/usr/bin/csd'])
    mock_process_iter.return_value = [pr1, pr2]

    cs_num = vcmmd.util.misc.get_cs_num()

    assert cs_num == 1
