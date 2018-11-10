import pytest
from pathlib import Path
import dontforget as df
from string import ascii_letters
import random

@pytest.yield_fixture(scope='function')
def tmpdir_dontforget(tmpdir):
    df.set_storage_directory(str(tmpdir))
    df.set_hash_customization(bytearray(random.getrandbits(8) for _ in range(16)))
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
    

def test_same_function_reloaded_retains_cache_key(tmpdir_dontforget, mocker):
    m = mocker.Mock()
    function_definition = "\n".join((
        "@dontforget.cached",
        "def func():",
        "    m.call()",
        "    return 42",
        "",
        "func()"))
    
    exec(function_definition, {
        'm': m, 
        'dontforget': tmpdir_dontforget
    })

    exec(function_definition, {
        'm': m, 
        'dontforget': tmpdir_dontforget
    })

    m.call.assert_called_once()


def test_function_body_change_busts_cache(tmpdir_dontforget, mocker):
    m = mocker.Mock()
    function_definition = "\n".join((
        "@dontforget.cached",
        "def func():",
        "    m.call()",
        "    return 42",
        "",
        "func()"))

    def run(body):
        exec(body, {
            'm': m, 
            'dontforget': tmpdir_dontforget
        })
    
    run(function_definition)

    run(function_definition.replace("42", "43"))

    assert len(m.method_calls) == 2, \
        'Expected func() to require re-evaluation after a constant changed'

    run(function_definition.replace("return 42", "i=42\n    return 42"))

    assert len(m.method_calls) == 3, \
        'Expected func() to require re-evaluation after the function body changed'

    run(function_definition.replace("def func():", "def func(foo=1):"))

    assert len(m.method_calls) == 4, \
        'Expected func() to require re-evaluation after the default args changed'

    run(function_definition.replace("def func():", "def func(*, foo=1):"))

    assert len(m.method_calls) == 5, \
        'Expected func() to require re-evaluation after the default keyword-only args'