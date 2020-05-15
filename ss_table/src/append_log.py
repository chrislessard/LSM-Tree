class Singleton:
    def __init__(self, decorated):
        self._decorated = decorated

    def instance(self, filename):
        try:
            return self._instance
        except AttributeError:
            self._instance = self._decorated(filename)
            return self._instance

    def __call__(self):
        raise TypeError('Singletons must be accessed through `instance()`.')

    def __instancecheck__(self, inst):
        return isinstance(inst, self._decorated)

@Singleton
class AppendLog:
    def __init__(self, filename):
        self.filename = filename
        self.stream = open(filename, 'a')

    def write(self, val):
        try:
            self.stream.write(val)
            self.stream.flush()
        except IOError:
            print("The file stream isn't currently open")

    def clear(self):
        self.stream.close()
        # Clearing the stream should clear the current file contents
        self.stream = open(self.filename, 'w')

