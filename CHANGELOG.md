# Changelog

## 1.4.0

* Support currency conversion based on exchange rates provided by the Bank of
  France.

## 1.3.0

* Upgrade to Python 3.10
* Upgrade to [Pandas 1.3.5](https://github.com/pandas-dev/pandas/blob/1.3.x/setup.py)
  because Pands 1.2.x does not support Python 3.10 ([source
  code](https://github.com/pandas-dev/pandas/blob/1.2.x/setup.py#L182-L186)),
  the versions of numpy and cython are also upgraded accordingly based on their
  setup files (`setup.{cfg,py}`)
* Upgrade to pytest 6.2.5 to fix test error:
  _"TypeError: required field "lineno" missing from alias"_

## 1.2.0

* Upgrade to Python 3.9
* Use environment variables to better debug

## 1.1.0

* Upgrade to Python 3.8

## 1.0.0

* Bump version to 1.0.0
