class Watched(object):
    def __init__(self):
        self.watchers = []

    def add_watcher(self, w):
        self.watchers.append(w)

    def fire(self, event, *args):
        for w in self.watchers:
            try:
                func = getattr(w, event, None)
                if func is not None:
                    func(*args)
            except Exception as ex:
                print(ex)
