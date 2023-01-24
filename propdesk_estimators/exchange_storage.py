from copy import deepcopy
import datetime
import tempfile
import pandas as pd
import os

from propdesk_services.azure_storage import AzureStorage
from propdesk_services.azure_storage import ServiceClient

from propdesk_services.blob_functions import get_tags_query_from_params_dict
from propdesk_services.blob_functions import validate_tags_dict

from propdesk_services.utils import get_dates_dataframe
from propdesk_services.utils import get_date_in_many_formats

class ExchangeStorage(AzureStorage):
    '''
        Creates an object that maps AzureStorage() functions to estimation data schema
        - Each exchange is a container
        - Each container contains a 'folder' for each asset pair
        - Each asset pair has a 'folder' for type of dataset
        - Each dataset has resolution of 1 day
    '''

    def __init__(self, exchange_str=None, overwrite=False, auto_create=False, conn_string=None):
        self.service_client = ServiceClient(conn_string)
        self.name = exchange_str
        self.container_client = None
        self.overwrite = overwrite
        self.auto_create = auto_create

        if self.name:
            self.container_client = self.connect_to_container(self.name)
            if not self.container_client.exists() and auto_create:
                self.service_client.connection.create_container(self.name, metadata={'exchange':str(True)})

    def list_available_pairs(self):
        '''
            List all available pairs in exchange
        '''
        return self.ls_dirs()

    def list_pair_datasets(self, pair_str):
        '''
            List all datasets for a given pair (e.g. 'btcbrl')
        '''
        return self.ls_files(path=pair_str, recursive=True)

    def download_dataset(self, dataset_str, dest=''):
        '''
            Download a dataset in full path relative to exchange root
            (e.g. '<@binance>/btcbrl/quotes/2021-01')
        '''
        return self.download(dataset_str, dest)

    def upload_dataset(self, dataset, dest_folder='', tags_dict={}):
        '''
            Upload a dataset in full path relative to exchange root
            (e.g. '@binance/btcbrl/quotes/2021-01')
            - dataset: path to dataset or data loaded in memory
            - dest_folder: path to upload dataset_to
            - tags_dict: tags to search files in storage by params_dict
              e.g.
              tags_dict = {
                'pair': 'btcbrl',
                'dataset_type': 'zma',
                'date_ref': '2021-01',
                'bandwidth: 5,
                'log': True
                }
        '''

        return self.upload(dataset, dest_folder, overwrite=True, tags_dict=tags_dict)

    def download_dataset_list(self, dataset_list, dest=''):
        '''
            Download a list of datasets in an exchange container
            (e.g. ['/btcbrl/quotes/2021-01', /btcbrl/quotes/2021-02'])
        '''
        for dataset in dataset_list:
            dest_path = os.path.join(dest, dataset)
            self.download_dataset(dataset, dest_path)

    def list_datasets_by_params(self, params_dict={}, amend=False):
        '''
            List all datasets that match params defined in params_dict
            e.g.
            params_dict = {
                'pair': 'btcbrl',
                'dataset_type': 'zma',
                'date_from': '2021-01',
                'date_to': '2021-05',
                'bandwidth: 5,
                'log': True
                }
        '''
        if not params_dict:
            return self.ls_all_container_files()

        exchange_query_str = f"@container='{self.name}' and "
        tags_dict_validated = validate_tags_dict(params_dict)
        if not tags_dict_validated:
            return []

        tags_query_str = exchange_query_str + get_tags_query_from_params_dict(tags_dict_validated)
        datasets = [i['name'] for i in self.service_client.connection.find_blobs_by_tags(tags_query_str)]

        if amend == True:
            print('\nAvailable datasets:',  datasets)
            self.amend_datasets_by_params(params_dict)
        return datasets

    def download_dataset_by_params(self, params_dict, dest='', amend=False):
        '''
            Download all datasets that match params defined in params_dict
            e.g.
            params_dict = {
                'pair': 'btcbrl',
                'dataset_type': 'zma',
                'date_from': '2021-01',
                'date_to': '2021-05',
                'bandwidth: 5,
                'log': True
                }
        '''

        datasets_list = self.list_datasets_by_params(params_dict, amend)
        self.download_dataset_list(datasets_list, dest)

    def diff_dataset_by_params(self, params_dict):
        missing_datasets = []
        if 'date_from' in params_dict and 'date_to' in params_dict:

            date_from_str = params_dict.pop('date_from')
            date_to_str = params_dict.pop('date_to')
            dates_df = get_dates_dataframe(date_from_str, date_to_str, '1D', inclusive=False)['datetime']

            for d in dates_df:
                date_ref_str = d.strftime("%Y-%m-%d")
                dataset_params = deepcopy(params_dict)
                dataset_params['date_ref'] = date_ref_str
                expected_dataset = self.list_datasets_by_params(dataset_params)
                if not expected_dataset:
                    missing_datasets.append(date_ref_str)
        return sorted(missing_datasets)


    def amend_datasets_by_params(self, params_dict):
        missing_datasets = self.diff_dataset_by_params(deepcopy(params_dict))
        print('\nMissing datasets:', missing_datasets)
        if not missing_datasets:
            return False

        # turning to a compute params, so start_date and end_date are used
        # instead of date_from and date_to
        params_dict.pop('date_from')
        params_dict.pop('date_to')

        start_date_str = missing_datasets[0]
        iter_datasets = iter(missing_datasets[1:])
        for date_dataset in missing_datasets:
            try:
                next_dataset = next(iter_datasets)
            except StopIteration:
                next_dataset = missing_datasets[-1]

            if get_date_in_many_formats(next_dataset) == get_date_in_many_formats(date_dataset) + datetime.timedelta(days=1):
                continue
            else:
                end_date_str = (get_date_in_many_formats(date_dataset) + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                job_params_dict = deepcopy(params_dict)
                job_params_dict['start_date'] = start_date_str
                job_params_dict['end_date'] = end_date_str
                job_params_dict['exchange'] = self.name
                print(f'\nRun appropriate job with these parameters:\n{job_params_dict}')
                start_date_str = next_dataset
        return True

#####################
# TOP LEVEL FUNCTIONS
#####################


def list_exchange_datasets_by_params(exchange_str, params_dict):
    '''
        Lists all datasets in exchange by params specified in params_dict
        e.g.
        params_dict = {
                'pair': 'btcbrl',
                'dataset_type': 'zma',
                'date_from': '2021-01',
                'date_to': '2021-05',
                'bandwidth: 5,
                'log': True
                }
    '''
    return ExchangeStorage(exchange_str).list_datasets_by_params(params_dict)


def list_datasets_by_params(params_dict):
    '''
        Lists all datasets in storage by params specified in params_dict
        e.g.
        params_dict = {
                'pair': 'btcbrl',
                'dataset_type': 'zma',
                'date_from': '2021-01',
                'date_to': '2021-05',
                'bandwidth: 5,
                'log': True
                }
    '''
    all_datasets = []
    tags_dict_validated = validate_tags_dict(params_dict)
    if not tags_dict_validated:
        raise Exception(f"Tags malformed:\n params_dict = {params_dict}")

    tags_query_str = get_tags_query_from_params_dict(tags_dict_validated)
    return [d['name'] for d in ServiceClient().connection.find_blobs_by_tags(tags_query_str)]


def download_datasets_by_params(exchange_str, params_dict, dest_folder=""):
    '''
        Download all datasets in storage by params specified in params_dict
        - exchange_str: specifies exchange to download from, else scan all exchanges
        - dest_folder: folder to download to
        e.g.
        params_dict = {
                'pair': 'btcbrl',
                'dataset_type': 'zma',
                'date_from': '2021-01',
                'date_to': '2021-05',
                'bandwidth: 5,
                'log': True
                }
    '''
    exchange_storage = ExchangeStorage(exchange_str)
    exchange_storage.download_dataset_by_params(params_dict, dest_folder)


def get_dataframe_by_params(exchange_str, params_dict, keep_local=True):
    tmp_folder = tempfile.mkdtemp()

    if keep_local:
        print(f"files saved to: {tmp_folder}")

    download_datasets_by_params(exchange_str, params_dict, dest_folder=tmp_folder)

    files = sorted([os.path.join(root, file) for root, _, files in os.walk(tmp_folder) for file in files])

    final_df = pd.DataFrame()
    for file_path in files:
        try:
            table = pd.read_parquet(file_path)
            final_df = pd.concat([final_df, table])
        except Exception as e:
            if e.__str__() == 'No columns to parse from file':
                pass  ## Empty file
            else:
                raise Exception("File error")
        if not keep_local:
            os.remove(file_path)

    final_df = final_df.reset_index(drop=True).drop_duplicates()
    return final_df
