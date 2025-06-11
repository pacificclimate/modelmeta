class Fake:
    """Class for faking other classes. Keyword params provided in the
    instantiation are converted to class attributes.

    Typical usage: Fake a CFDataset object for testing methods that accept
    such an argument::

        fake_cf = Fake(a=1, b=2, metadata=Fake(x=10, y=20))
    """

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class Mock:
    """Class for mocking an existing object.

    Avoids setting the mocked attribute(s) on the original object, which can
    cause problems (e.g., with NetCDF).

    WARNING: Will fail if you mock an attribute named '_obj'.
    """

    def __init__(self, obj, **kwargs):
        """Mock the attributes of obj according to the keyword args.
        Keyword argument keys is attribute name, value is mocked attribute
        value.
        """
        self._obj = obj
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __getattr__(self, item):
        try:
            return self.__dict__[item]
        except KeyError:
            return getattr(self._obj, item)
