# Environment Integration Testing

The tests here are a start at a comprehensive system integration test,
to be run against a deployed Indexify environment via a GitHub action.

Since these tests are not intended for normal system testing, they are
omitted by default from pytest test discovery.

To run these tests locally, use `pytest -m integration`.  It may be
useful to override the options in `conftest.py`.
