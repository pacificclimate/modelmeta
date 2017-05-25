class Mock:
    """Class for mocking other classes. Keyword params provided in instantiation are converted to class attributes.

    Typical usage: Mock a CFDataset object for testing methods that accept such an argument:

        mock_cf = Mock(a=1, b=2, metadata=Mock(x=10, y=20))
    """
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
