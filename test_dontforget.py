import pytest
from pathlib import Path
import dontforget as df
from string import ascii_letters
import random

@pytest.yield_fixture(scope='function')
def tmpdir_dontforget(tmpdir):
    df.set_storage_directory(str(tmpdir))
    yield df

def test_does_not_repeat_function_call_with_same_arguments(tmpdir_dontforget, mocker):
    m = mocker.Mock()

    @tmpdir_dontforget.cached
    def function(n):
        m.call()
        return 42

    assert function(5) == 42
    assert function(5) == 42

    m.call.assert_called_once()


def test_does_not_repeat_function_that_returns_none(tmpdir_dontforget, mocker):
    m = mocker.Mock()

    @tmpdir_dontforget.cached
    def function():
        m.call()
        return None

    assert function() is None
    assert function() is None

    m.call.assert_called_once()


def test_change_of_hash_seed_is_encorporated_into_the_key(tmpdir_dontforget, mocker):
    m = mocker.Mock()

    @tmpdir_dontforget.cached
    def function():
        m.call()
        return None

    assert function() is None
    tmpdir_dontforget.set_hash_customization(b'newseed')
    assert function() is None

    assert len(m.method_calls) == 2, \
        'Expected function() to be called again after changing the hash customization'


def test_saves_large_objects_out_to_separate_files(tmpdir_dontforget):

    @tmpdir_dontforget.cached
    def func_with_long_return():
        # A string with a high degree of entropy; compression won't be very effective
        return ''.join(random.choice(ascii_letters) for _ in range(10000))
    
    func_with_long_return()

    assert len([l for l in Path(tmpdir_dontforget._cache_root).iterdir()]) > 1, \
        'Expected a large return value to be spilled to disk'
    

def test_compresses_data_before_saving_out(tmpdir_dontforget):

    @tmpdir_dontforget.cached
    def func_with_long_return():
        # A string with a low degree of entropy; compression will be very effective
        return 'a' * 10000
    
    func_with_long_return()

    assert len([l for l in Path(tmpdir_dontforget._cache_root).iterdir()]) == 1, \
        'Expected a return value that compresses well to become small'
    