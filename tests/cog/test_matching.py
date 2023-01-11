# pylint: disable=redefined-outer-name,missing-function-docstring,unused-argument
"""
Test matching module.
"""
import cog.matching


def test_substr_ind():
    assert cog.util.substr_ind('ale', 'alex') == [0, 3]
    assert cog.util.substr_ind('ALEX'.lower(), 'Alexander'.lower()) == [0, 4]
    assert cog.util.substr_ind('nde', 'Alexander') == [5, 8]

    assert not cog.util.substr_ind('ALe', 'Alexander')
    assert not cog.util.substr_ind('not', 'alex')
    assert not cog.util.substr_ind('longneedle', 'alex')

    assert cog.util.substr_ind('16 cyg', '16 c y  gni', skip_spaces=True) == [0, 9]
