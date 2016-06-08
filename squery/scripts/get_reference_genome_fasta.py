import urllib.request
import pandas as pd
import gzip

__author__ = 'timputman'


def run(*taxid):
    def get_ref_ftp_path(taxid):
        taxid = taxid[0]
        assembly = urllib.request.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/genomes/refseq/bacteria/assembly_summary.txt")
        columns = ['assembly_accession', 'bioproject', 'biosample', 'wgs_master', 'refseq_category', 'taxid',
                   'species_taxid', 'organism_name', 'infraspecific_name', 'isolate', 'version_status',
                   'assembly_level',
                   'release_type', 'genome_rep', 'seq_rel_date', 'asm_name', 'submitter', 'gbrs_paired_asm',
                   'paired_asm_comp', 'ftp_path', 'excluded_from_refseq']

        data = pd.read_csv(assembly[0], sep="\t", dtype=object, skiprows=2, names=columns)

        selected = data[data['taxid'] == taxid]
        ftp_path = selected.iloc[0]['ftp_path']
        file_name = ftp_path.split('/')[-1]
        url = ftp_path + '/' + file_name + '_genomic.fna.gz'
        genome = urllib.request.urlretrieve(url)[0]
        return gzip.open(genome, 'r')

    def create_jbrowse_data()