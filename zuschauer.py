# -*- coding: utf-8 -*-
"""
Copyright (c) 2020, Bernhard Häußler.
License: BSD, see LICENSE for more details.
"""
# https://github.com/Azure/azure-sdk-for-python/blob/master/sdk/eventhub/azure-eventhub-checkpointstoreblob-aio/azure/eventhub/extensions/checkpointstoreblobaio/_vendor/storage/blob/_blob_client.py
# https://github.com/Azure/azure-sdk-for-python/tree/master/sdk/storage/azure-storage-blob
from gooey import Gooey
import argparse
from pathlib import Path
import os
import sys
import time
import logging
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler
from watchdog.events import FileSystemEventHandler
import re
import arrow
import subprocess
import sys

from azure.storage.blob._shared.base_client import create_configuration
from azure.storage.blob.aio import BlobServiceClient as BlobServiceClientAIO
from azure.storage.blob import BlobServiceClient

STORAGES = ["ADLS", "Blob", "onPrem"]


@Gooey
def parse_arguments():
    parser = argparse.ArgumentParser(description='Zuschauer - Dateisystem watchdog für den Upload spezifischer Dateien.')

    parser.add_argument(
        "--paths",
        "-p",
        type=lambda p: Path(p).absolute(),
        default=[Path(__file__).absolute().parent],
        nargs='+',
        help="Wurzelpfad(e)",
    )
    parser.add_argument(
        "--filetypes",
        "-f",
        default='cht',
        required=True,
        help="Erlaubte Dateiendung(en), Semikolon-seperariert. Asterisk for all types.",
    )
    parser.add_argument(
        "--storage",
        "-a",
        default=STORAGES[1],
        choices=STORAGES,
        required=True,
        help="Storage Option.",
    )
    parser.add_argument(
        "--connectionString",
        "-c",
        required=True,
        help="<AccountName=$$$;AccountKey=$$$;Path=$$$)> (für Azure Storage: ADLS Gen1/Blob Container - Pfad der Storage Ressource) oder Pfad für Netzwerklaufwerk.",
    )
    parser.add_argument(
        "--proxy",
        "-y",
        default='',
        help="Semikolon separated Proxy URLs or IP Adresses for http;https",
    )
    parser.add_argument(
        "--save",
        "-s",
        action='store_true',
        default=True,
        help="Save config for next startup.",
    )
    parser.add_argument(
        "--refresh",
        "-x",
        type=int,
        default=1,
        help="Refresh Frequency.",
    )
    parser.add_argument(
        "--recursive",
        "-r",
        action='store_true',
        default=True,
        help="Rekursive Ordnerpfade.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action='store_true',
        default=True,
        help="Run in verbose mode.",
    )
    try:
        return parser.parse_args()
    except SystemExit as e:
        # This exception will be raised if --help or invalid command line arguments
        # are used. Currently streamlit prevents the program from exiting normally
        # so we have to do a hard exit.
        os._exit(e.code)


def run_cli_command(cmd):
    # if os.name == 'nt':
    #     startupinfo = subprocess.STARTUPINFO()
    #     startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    result = subprocess.run([cmd], capture_output=True, shell=True, text=True) # startupinfo=startupinfo)
    if result.returncode:
        # failed
        print(">>>Tracelog: ", result.stderr, result.stdout, '\n')
    return result.returncode, result.stdout, result.stderr


class AzureStorageContainer():
    def __init__(self, connection_string: str, container_name: str, proxy: dict=None):
        self.connection_string = connection_string        
        # Create a storage Configuration object and update the proxy policy.
        self.config = create_configuration(storage_sdk='blob')

        if proxy is not None:
            http_proxy = proxy.get('http_proxy')
            https_proxy = proxy.get('https_proxy')
            if http_proxy is not None and https_proxy is not None:
                self.config.proxy_policy.proxies = {
                    'http': http_proxy,
                    'https': https_proxy
                }
        self.container_name = container_name

    def _available_containers(self):
        success = False
        containers = []
        try:
            bsc = self._bsc()
            with bsc:
                containers = list(bsc.list_containers(logging_enable=True))
            success = True
        except BaseException as e:
            print(e)
            pass
        return success, containers

    def _init_blob_service(self, blob_name):
        # Instantiate a new ContainerClient
        container_client = self.bsc.get_container_client(self.container_name)
        # Instantiate a new BlobClient
        blob_client = container_client.get_blob_client(blob_name)

        return blob_client

    def _bsc(self, asynced=False):
        if asynced:
            # Instantiate a new BlobServiceClientAIO using a connection string
            return BlobServiceClientAIO.from_connection_string(self.connection_string, _configuration=self.config)
        else:
            # Construct the BlobServiceClient, including the customized configuation.
            return BlobServiceClient.from_connection_string(self.connection_string, _configuration=self.config)

    async def _save_block_async(self, path, overwrite=False):
        async with self.bsc:
            try:
                # Instantiate a new BlobClient
                blob_client = self._init_blob_service(path.name)

                # [START upload_a_blob]
                # Upload content to block blob
                with open(path, "rb") as data:
                    await blob_client.upload_blob(data, blob_type="BlockBlob", overwrite=overwrite, logging_enable=True)
                # [END upload_a_blob]
            finally:
                pass
                
            return

    def _save_block(self, path, overwrite=False):
        with self.bsc:
            try:
                # Instantiate a new BlobClient
                blob_client = self._init_blob_service(path.name)

                # [START upload_a_blob]
                # Upload content to block blob
                with open(path, "rb") as data:
                    blob_client.upload_blob(data, blob_type="BlockBlob", overwrite=overwrite, logging_enable=True)
                # [END upload_a_blob]
            finally:
                pass

        return

    def save_block_blob(self, path: Path, asynced: bool=False, overwrite: bool=False):
        failed = True
        path = path.absolute()
        if path.exists() and path.is_file():
            self.bsc = self._bsc(asynced=asynced)
            if asynced:
                self._save_block_async(path, overwrite)
            else:
                self._save_block(path, overwrite)
            failed = False
            self.bsc = None
        else:
            print(f"{path} does not exist or not a file.")

        return failed

    @property
    def connected(self):
        return self._available_containers()[0]

    def download(self, blob_name, path):
            # Instantiate a new BlobClient
            blob_client = self._init_blob_service(blob_name)

            if path.is_directory():
                path = path.joinpath(blob_name)
            # [START download_a_blob]
            with open(path, "wb") as down:
                download_stream = blob_client.download_blob()
                down.write(download_stream.readall())
            # [END download_a_blob]

    def delete_blob(self, blob_name):
            # Instantiate a new BlobClient
            blob_client = self._init_blob_service(blob_name)

            # [START delete_blob]
            blob_client.delete_blob()
            # [END delete_blob]


class Zuschauer(FileSystemEventHandler):
    # files to exclude from being watched
    exclude = re.compile(r'|'.join(r'(.+/)?'+ a for a in [
        # Vim swap files
        r'\..*\.sw[px]*$',
        # backup files
        r'.~$',
        # git directories
        r'\.git/?',
        # __pycache__ directories
        r'__pycache__/?',
        ]))

    def __init__(self, paths, filetypes, storage, recursive=True, refreshFrequency=1,
            verboseMode=True, azureService=None,
            triggerActionAtStart=False # upload already available files
        ):
        self.inputPaths = paths
        self.paths = {Path(p).absolute():p for p in self.inputPaths}
        self.filetypes = ['.'+f for f in filetypes.split(';')]
        self.storage = storage
        self.recursive = recursive
        self.verboseMode = verboseMode
        self.refreshFrequency = refreshFrequency
        self.azureService = azureService
        self.observer = Observer(timeout=0.1)

        for p in self.paths:
            if p.exists():
                # Add directory
                self.observer.schedule(self, p, recursive=True)

    def execAction(self, changedFile, overwrite):
        failed = True
        stdout = open(os.devnull, 'wb') if self.verboseMode else None
        if self.storage == STORAGES[0]:
            # ADLS
            if isinstance(self.azureService, list):
                # f'az dls fs upload --account {AccountName} --source-path {changedFile} --destination-path "/{pathToDestination}/{changedFile.stem}"'
                cmd = self.azureService[0]+str(changedFile)+self.azureService[1]+str(changedFile.stem)+'"'
                failed, _, _ = run_cli_command(cmd)
            else:
                failed = True
        elif self.storage == STORAGES[2]:
            # TODO: onPrem
            raise NotImplementedError
        else:
            # Blob
            failed = self.azureService.save_block_blob(path=changedFile, overwrite=overwrite)

        now = arrow.now()
        if self.verboseMode:
            print_message = "'" + str(changedFile)
            print_message += ' at ' + arrow.now().format('YYYY-MM-DD HH:mm:ss ZZ')
            print_message += ", running '" + self.storage + "'"
            print('==> ' + print_message + ' <==')
        print("Success:  ", not(failed))

    def is_interested(self, path):
        print(path)
        if self.exclude.match(str(path)):
            return False

        if path in self.paths:
            return True

        if self.recursive:
            while path.parent.absolute() != path:
                path = path.parent.absolute()
                if path in self.paths:
                    return True
            
        return False

    def on_change(self, path, overwrite=False):
        path = Path(path)
        if self.is_interested(path):
            if path.is_file() and path.suffix in self.filetypes or '*' in self.filetypes:
                self.execAction(path, overwrite)

    def on_created(self, event):
        # if self.observer.__class__.__name__ == 'InotifyObserver':
        #     # inotify also generates modified events for created files
        #     return

        if event.is_directory:
            print('created dir ', event.src_path)
            self.on_change(event.src_path)
        else:
            if not Path(event.src_path).stem.startswith('.'):
                print('created file')
                self.on_change(event.src_path)

    def on_modified(self, event):
        if not event.is_directory:
            if not Path(event.src_path).stem.startswith('.'):
                pass
                print(event.src_path, ' modified')
                self.on_change(event.src_path, overwrite=True)

    def on_moved(self, event):
        if not event.is_directory:
            if not Path(event.src_path).stem.startswith('.'):
                pass
                # print(event.src_path, ' file_moved')
                # self.on_change(event.dest_path)

    def on_deleted(self, event):
        if not event.is_directory:
            if not Path(event.src_path).stem.startswith('.'):
                pass
                # print(event.src_path, ' file_deleted')
                # self.on_change(event.src_path)

    def run(self):    
        self.observer.start()
        try:
            while True:
                time.sleep(self.refreshFrequency)
        except KeyboardInterrupt:
            self.observer.stop()
        self.observer.join()


def main(args):
    # Create a logger for the 'azure.storage.blob' SDK
    logger = logging.getLogger(args.storage)
    logger.setLevel(logging.DEBUG)
    # Configure a console output
    handler = logging.StreamHandler(stream=sys.stdout)
    logger.addHandler(handler)

    # check if connection string arg, if correct init azureService to be passed to watchdog
    failed, out, err, azureService = True, '', '', None
    connString = args.connectionString
    if all([s in connString for s in ["AccountName=","AccountKey=","Path=",";"]]):
        split = connString.split(';', 2)
        if len(split) == 3:
            # parse connection string
            r = re.search("AccountName=(.*);AccountKey=(.*);Path=(.*)", connString)
            AccountName = r.group(1)
            AccountKey = r.group(2)
            pathToDestination = r.group(3)
            # get proxy settings
            if len(args.proxy) and ';' in args.proxy:
                http_proxy, https_proxy = args.proxy.split(';', 1)
                proxy = dict(http_proxy=http_proxy, https_proxy=https_proxy)
            else:
                proxy = None

            if args.storage == STORAGES[0]:
                # ADLS
                # TODO: proxy
                if not pathToDestination.startswith('/'):
                    pathToDestination = '/' + pathToDestination
                # check connection
                cmd = f'az dls fs list --account {AccountName} --path "{pathToDestination}"'
                failed, out, err = run_cli_command(cmd)
                if failed:
                    print("Did you set up azure-cli? Install and run az login in a shell: https://aka.ms/cli")
                    print("Otherwise either connection string invalid, or check proxy settings.")
                    exit(1)
                else:
                    azureService = [f'az dls fs upload --account {AccountName} --source-path ', f' --destination-path "/{pathToDestination}/']
            elif args.storage == STORAGES[1]:
                # BLOB
                # init 
                ac = AzureStorageContainer(connection_string=f"AccountName={AccountName};AccountKey={AccountKey}",
                    container_name=pathToDestination, proxy=proxy)
                # check connection
                if not ac.connected:
                    print("Cannot connect to Azure Blob Service.")
                else:
                    failed = False
                    azureService = ac
            # TODO: implement the rest of the storage options
            else:
                raise NotImplementedError
        else:
            print("Check connection string. Format of connection string of Azure Dashboard not yet supported.")

    if not failed and azureService is not None:
        if args.verbose:
            print("Schaue auf %s, bei Dateierstellung wird '%s' ausgeführt." % (args.paths, args.storage))
        # watch filesystem for file creation
        # subdirs are create per day
        # in those subdirs are files created
        zs = Zuschauer(paths=args.paths, filetypes=args.filetypes, storage=args.storage, recursive=args.recursive, refreshFrequency=args.refresh,
                verboseMode=args.verbose, azureService=azureService)
        try:
            zs.run()
        except KeyboardInterrupt:
            print('^C')
            exit(0)
    else:
        print("A connection to storage option could not be established.")
        exit(1)


if __name__ == "__main__":
    # prepare options
    args = parse_arguments()

    main(args) # upload already available files