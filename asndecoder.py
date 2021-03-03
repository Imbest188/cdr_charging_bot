import subprocess

class asndecoder:
    def __init__(self, program_path='asn/casndecoder.exe', work_path='CDR/'):
        self.decoder_path = program_path
        self.work_folder = work_path

    def decode(self, file):
        si = subprocess.STARTUPINFO()
        si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        p = subprocess.Popen([self.decoder_path, self.work_folder + file], startupinfo=si)
        p.communicate()
        return not p.returncode
