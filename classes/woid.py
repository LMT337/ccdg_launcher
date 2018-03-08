class Woid:
    def __init__(self, woid):

        self.woid = woid
        self.statusfile = woid + '.qcstatus.tsv'
        self.master = woid + '.master.samples.tsv'
        self.activepass = woid + '.instrument.pass.status.active.tsv'
        self.launchfail = woid + '.launch.fail.tsv'
