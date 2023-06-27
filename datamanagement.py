# Required Libraries
import subprocess
import tempfile
import glob
import shutil

class FileManager:
    def __init__(self, config):
        self.config = config
        self.temp_dir = tempfile.mkdtemp()
        self.zst_path = config['zst_path']
        self.chunk_size = 1e6*self.config['chunk_size_MB']

    def split_zst_file(self):
        # Encapsulate bash command with subprocess module and chunk large zst file
        bash_cmd = f"unzstd --long=31 -c {self.zst_path} | split -C {self.chunk_size} -d -a4 - {self.temp_dir}/chunk --filter='gzip > {self.temp_dir}/$FILE.gz'"
        process = subprocess.Popen(bash_cmd, stdout=subprocess.PIPE, shell=True)
        output, error = process.communicate()

    def get_gz_chunk_files(self):
        # get gz chunk files
        return glob.glob(f"{self.temp_dir}/*.gz")

    def merge_csv_files(self):
        # Merge CSV chunks into one large CSV
        csv_files = glob.glob(f"{self.temp_dir}/*.csv")
        csv_out = self.config.get('csv_path', self.config['zst_path'].replace('.zst', '.csv'))
        with open(csv_out, 'w') as outfile:
            for i, fname in enumerate(csv_files):
                with open(fname) as infile:
                    if i != 0:
                        infile.readline()  # skip the header line on all files except the first one
                    shutil.copyfileobj(infile, outfile)

    def cleanup(self):
        # Remove all temporary gz and csv chunks
        shutil.rmtree(self.temp_dir)
