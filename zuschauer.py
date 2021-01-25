#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "Bernhard Häußler"
__copyright__   = "Copyright (c) 2020"
__version__ = 0.3
__license__ = "BSD"
__maintainer__ = "Bernhard Häußler"
__email__ = "@drahnreb"
__status__ = "Production"

"""
    Zuschauer (*der Zuschauer dt. - spectator*) - 
    Watch a (or more) specified folder(s) for newly created or modified files and **copy** them to configured storage option. Supported options are `Azure Storage Blob`, `ADLS Gen 1` (untested) or on-premise Network Drives (in future).
    Zuschauer uses official APIs and opens files in read-only byte mode to copy files, it waits a second to prevent data loss.
    You need to install pip install pywin32.
    After that you need to run python Scripts/pywin32_postinstall.py -install from your Python directory to register the dlls.
    To hide the program, you can run it via pythonw.exe.

"""
if __package__ is None or __package__ == '':
    from gooey import Gooey, GooeyParser
import argparse
from pathlib import Path
import os
import platform
import sys
import time
import logging
import tempfile
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler
from watchdog.events import FileSystemEventHandler
import re
import arrow
import subprocess
import sys
import json

import keyring
STORECREDENTIALS = False
try:
    if platform.system().lower().startswith("win"):
        import pywin32
        STORECREDENTIALS = True
    elif platform.system().lower().startswith("lin"):
        import secretstorage
        STORECREDENTIALS = True
    else:
        raise NotImplementedError
except ImportError:
    print("Cannot use keyring features. Won't be able to store credentials")

# uses official Azure SDK for python
# https://github.com/Azure/azure-sdk-for-python/blob/master/sdk/eventhub/azure-eventhub-checkpointstoreblob-aio/azure/eventhub/extensions/checkpointstoreblobaio/_vendor/storage/blob/_blob_client.py
# https://github.com/Azure/azure-sdk-for-python/tree/master/sdk/storage/azure-storage-blob
from azure.storage.blob._shared.base_client import create_configuration
from azure.storage.blob.aio import BlobServiceClient as BlobServiceClientAIO
from azure.storage.blob import BlobServiceClient

STORAGES = ["ADLS", "Blob", "onPrem"]
CONFIGFILE = Path(Path(__file__).absolute().parent).joinpath('.config')
PAUSEAFTERMODIFIED = 3 # seconds of pause after file modification and until copying starts

@Gooey(program_name="zuschauer @drahnreb", default_size=(1200,910), taskbar=True)
def parse_arguments(defaults):
    # use arg parsing without gooey to enable help and enable/disable control of config loading
    # gooey parameter disables 'required arguments' to pass first headless check for load arg
    parser = GooeyParser(description=f'Zuschauer - Filesystem watchdog to copy data to remote storage and enable IoT.\tby {__author__}\tv.{__version__}')
    
    requiredNamed = parser.add_argument_group('Required arguments')
    requiredNamed.add_argument(
        "-paths",
        "-p",
        type=lambda p: Path(p),
        default=[Path(__file__).resolve().parent],
        nargs='+',
        help="Root path(s)",
        required=True,
        gooey_options={
            'initial_value': defaults.get('paths', [Path(__file__).resolve().parent])  
        }
    )
    requiredNamed.add_argument(
        "-filetypes",
        "-f",
        default='',
        required=True,
        help="Allowed file suffix(es), semicolon-separated. Asterisk or leave empty for all types.",
        gooey_options={
            'initial_value': defaults.get('filetypes', '')  
        }
    )
    requiredNamed.add_argument(
        "--storage",
        "-a",
        default=STORAGES[1],
        choices=STORAGES,
        required=True,
        help="Storage Option.",
        gooey_options={
            'initial_value': defaults.get('storage', STORAGES[1])  
        }
    )
    requiredNamed.add_argument(
        "-connectionString",
        "-c",
        required=True,
        help='"<AccountName=$$$;AccountKey=$$$;Path=$$$)>" (for Azure Storage: ADLS Gen1/Blob Container - Path of Storage Ressource) or path to network share.',
        gooey_options={
            'initial_value': defaults.get('connectionString', "")  
        }
    )
    # optional
    parser.add_argument(
        "--proxy",
        "-y",
        default='',
        help="Semicolon separated Proxy URLs or IP Adresses for http;http(s) if proxy doesn't support https use http:// prefix twice\nformat: 'http://proxyURLorIP:proxyPort;http(s)://proxyURLorIP:proxyPort'",
        gooey_options={
            'initial_value': defaults.get('proxy', "")  
        }
    )
    parser.add_argument(
        "--save",
        "-s",
        action='store_true',
        default=True,
        help="Save JSON config for next startup or headless mode. (Credentials are stored in keyring)",
        gooey_options={
            'initial_value': defaults.get('save', True)  
        }
    )
    parser.add_argument(
        "--load",
        "-l",
        default=CONFIGFILE,
        type=lambda p: Path(p),
        help="Specify path to JSON config file that should be used and loaded",
        gooey_options={
            'initial_value': defaults.get('load', CONFIGFILE)  
        }
    )
    parser.add_argument(
        "--refresh",
        "-x",
        type=int,
        default=1,
        help="Refresh Frequency.",
        gooey_options={
            'initial_value': defaults.get('refresh', 1)  
        }
    )
    parser.add_argument(
        "--recursive",
        "-r",
        action='store_true',
        default=True,
        help="Enable nested paths (deep changes) and check root paths recursively.",
        gooey_options={
            'initial_value': defaults.get('recursive', True)  
        }
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action='store_true',
        default=True,
        help="Run in verbose mode.",
        gooey_options={
            'initial_value': defaults.get('verbose', True)  
        }
    )
    parser.add_argument(
        "--dryrun",
        "-d",
        action='store_true',
        default=False,
        help="Use as a dry run to save config file and test connection without actually uploading anything. E.g. use to create JSON config file only.",
        gooey_options={
            'initial_value': defaults.get('dryrun', False)  
        }
    )
    parser.add_argument(
        "--existing",
        "-e",
        action='store_true',
        default=True,
        help="Upload existing files in specified paths.",
        gooey_options={
            'initial_value': defaults.get('existing', True)  
        }
    )
    try:
        return parser.parse_args()
    except SystemExit as e:
        os._exit(e.code)

def _parse_arguments(defaults={}, gooey=False):
    # use arg parsing without gooey to enable help and enable/disable control of config loading
    # gooey parameter disables 'required arguments' to pass first headless check for load arg
    parser = argparse.ArgumentParser(description=f'Zuschauer - Filesystem watchdog to copy data to remote storage and enable IoT.\tby {__author__}\tv.{__version__}')
    
    requiredNamed = parser.add_argument_group('Required arguments')
    requiredNamed.add_argument(
        "-paths",
        "-p",
        type=lambda p: Path(p),
        default=[Path(__file__).resolve().parent] if gooey else None,
        nargs='+',
        help="Root path(s)",
        required=gooey
    )
    requiredNamed.add_argument(
        "-filetypes",
        "-f",
        default='' if gooey else None,
        required=gooey,
        help="Allowed file suffix(es), semicolon-separated. Asterisk or leave empty for all types.",
    )
    requiredNamed.add_argument(
        "--storage",
        "-a",
        default=STORAGES[1] if gooey else None,
        choices=STORAGES,
        required=gooey,
        help="Storage Option.",
    )
    requiredNamed.add_argument(
        "-connectionString",
        "-c",
        required=gooey,
        help='"<AccountName=$$$;AccountKey=$$$;Path=$$$)>" (for Azure Storage: ADLS Gen1/Blob Container - Path of Storage Ressource) or path to network share.',
    )
    # optional
    parser.add_argument(
        "--proxy",
        "-y",
        default='' if gooey else None,
        help="Semicolon separated Proxy URLs or IP Adresses for http;http(s) if proxy doesn't support https use http:// prefix twice\nformat: 'http://proxyURLorIP:proxyPort;http(s)://proxyURLorIP:proxyPort'",
    )
    parser.add_argument(
        "--save",
        "-s",
        action='store_true',
        default=True if gooey else None,
        help="Save JSON config for next startup or headless mode. (CAUTION: credentials are stored in plain text!)",
    )
    parser.add_argument(
        "--load",
        "-l",
        default=CONFIGFILE,
        type=lambda p: Path(p),
        help="Specify path to JSON config file that should be used and loaded",
    )
    parser.add_argument(
        "--refresh",
        "-x",
        type=int,
        default=1 if gooey else None,
        help="Refresh Frequency.",
    )
    parser.add_argument(
        "--recursive",
        "-r",
        action='store_true',
        default=True if gooey else None,
        help="Enable nested paths (deep changes) and check root paths recursively.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action='store_true',
        default=True if gooey else None,
        help="Run in verbose mode.",
    )
    parser.add_argument(
        "--dryrun",
        "-d",
        action='store_true',
        default=False if gooey else None,
        help="Use as a dry run to save config file and test connection without actually uploading anything. E.g. use to create JSON config file only.",
    )
    parser.add_argument(
        "--existing",
        "-e",
        action='store_true',
        default=True if gooey else None,
        help="Upload existing files in specified paths.",
    )
    try:
        if gooey:
            return parser.parse_args()
        else:
            return parser
    except SystemExit as e:
        os._exit(e.code)

def checkArgs(args):
    # check Namespace
    try:
        _ = [args.paths, args.filetypes, args.connectionString, args.storage, args.proxy,\
            args.save, args.refresh, args.recursive, args.verbose, args.dryrun, args.existing]
    except AttributeError as e:
        print(f"Argument in config not set correctly: \n{e}")
        loggin.error(f"Argument in config not set correctly: \n{e}")
        exit(1)
        
    # check rest of required args
    if not len(args.paths) or not isinstance(args.paths, list):
        print(f"{args.paths} not set correctly.")
        logging.error(f"{args.paths} not set correctly.")
        exit(1)
    else:
        for p in args.paths:
            try:
                assert Path(p).is_absolute()
            except:
                print(f"{p} is not a valid path on this system. Provide an absolute path.")
                logging.ERROR(f"{p} is not a valid path on this system. Provide an absolute path.")
                exit(1)
    if not len(args.filetypes):
        print(f"{args.filetypes} not set correctly.")
        logging.error(f"{args.filetypes} not set correctly.")
        exit(1)

    # check if connection string arg, if correct init storageService to be passed to watchdog
    out, err, pathToDestination, storageService = '', '', '', None
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

            if pathToDestination:
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
                        storageService = [f'az dls fs upload --account {AccountName} --source-path ', f' --destination-path "/{pathToDestination}/']
                elif args.storage == STORAGES[1]:
                    # BLOB
                    # init
                    ac = AzureStorageContainer(connection_string=f"AccountName={AccountName};AccountKey={AccountKey}",
                        container_name=pathToDestination, proxy=proxy)
                    # check connection
                    if not ac.connected:
                        print("Cannot connect to Azure Blob Service.")
                    else:
                        storageService = ac
                # TODO: implement the rest of the storage options
                else:
                    raise NotImplementedError
            else:
                print("Path in Connection String not set (correctly).")
        else:
            print("Check connection string. Format of connection string of Azure Dashboard not yet supported.")
            raise NotImplementedError

    if storageService is None:
        print("A connection to storage option could not be established.")
        logging.error("A connection to storage option could not be established.")
        exit(1)

    return storageService

def run_cli_command(cmd):
    # if os.name == 'nt':
    #     startupinfo = subprocess.STARTUPINFO()
    #     startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    result = subprocess.run([cmd], capture_output=True, shell=True, text=True) # startupinfo=startupinfo)
    if result.returncode:
        # failed
        print(">>>Tracelog: ", result.stderr, result.stdout, '\n')
        logging.error(">>>Tracelog: ", result.stderr, result.stdout, '\n')
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
        path = path.resolve()
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
            logging.error(f"{path} does not exist or not a file.")

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
            verboseMode=True, dryRun=False, storageService=None
        ):
        self.paths = paths
        self.filetypes = filetypes
        self.storage = storage
        self.recursive = recursive
        self.dryRun = dryRun
        self.verboseMode = verboseMode if not self.dryRun else True
        self.refreshFrequency = refreshFrequency
        self.storageService = storageService
        self.observer = Observer(timeout=0.1)

        for p in self.paths:
            if p.exists():
                # Add directory
                self.observer.schedule(self, p, recursive=True)

    def execAction(self, changedFile, overwrite):
        if self.verboseMode:
            print_message = arrow.now().format('YYYY-MM-DD HH:mm:ss ZZ')
            print_message += "\t'" + str(changedFile.name) + "'"
            print_message += f"\t{'copy to' if not overwrite else 'overwrite in'} '" + self.storage + "'"
            print('==> ' + print_message + ' <==')

        # if dryRun active do not execute
        if self.dryRun:
            # sleep 3 secs to emulate long upload
            time.sleep(3)
            print(f"## would have {'copied' if not overwrite else 'overwritten'}.\nbut --dryrun enabled; no action executed.")
            return

        failed = True
        stdout = open(os.devnull, 'wb') if self.verboseMode else None

        if self.storage == STORAGES[0]:
            # ADLS
            if isinstance(self.storageService, list):
                # f'az dls fs upload --account {AccountName} --source-path {changedFile} --destination-path "/{pathToDestination}/{changedFile.stem}"'
                cmd = self.storageService[0]+str(changedFile)+self.storageService[1]+str(changedFile.stem)+'"'
                failed, _, _ = run_cli_command(cmd)
            else:
                failed = True
        elif self.storage == STORAGES[2]:
            # TODO: onPrem
            raise NotImplementedError
        else:
            # Blob
            failed = self.storageService.save_block_blob(path=changedFile, overwrite=overwrite)

        print(f"$$ Successfully {'copied' if not overwrite else 'overwritten'}: {str(changedFile.name)} {'to' if not overwrite else 'in'} {self.storage}" if not(failed) else f"## Failed copying: {str(changedFile.name)}")
        logging.info(f"$$ Successfully {'copied' if not overwrite else 'overwritten'}: {str(changedFile.name)} {'to' if not overwrite else 'in'} {self.storage}" if not(failed) else f"## Failed copying: {str(changedFile.name)}")

    def is_interested(self, path: Path, recursive: bool = False):
        if self.exclude.match(str(path)):
            return False

        # a path or file in watched paths
        if path in self.paths or (path.is_file() and path.parent in self.paths):
            return True

        if recursive:
            while path.parent != path:
                # walk up towards path's root until we reach root
                path = path.parent
                if self.is_interested(path, recursive=False):
                    return True

        return False

    def on_change(self, path, overwrite=False):
        path = Path(path)
        if self.is_interested(path, recursive=self.recursive):
            # print("interesting file")
            # print("\nis file: ", path.is_file(), '\nsuffix: ', path.suffix, '\nin filetypes: ', path.suffix in self.filetypes)
            if path.is_file() and (path.suffix in self.filetypes or '*' in self.filetypes or '.' in self.filetypes):
                self.execAction(path, overwrite)

    def on_created(self, event):
        # if self.observer.__class__.__name__ == 'InotifyObserver':
        #     # inotify also generates modified events for created files
        #     return

        if event.is_directory:
            logging.info(f'created dir {event.src_path}')
            self.on_change(event.src_path)
        else:
            self.on_change(event.src_path)

    def on_modified(self, event):
        if not event.is_directory:
            time.sleep(PAUSEAFTERMODIFIED)
            self.on_change(event.src_path, overwrite=True)

    def on_moved(self, event):
        if not event.is_directory:
            pass
            # print(event.src_path, ' file_moved')
            # self.on_change(event.dest_path)

    def on_deleted(self, event):
        if not event.is_directory:
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


def main(args, storageService):
    filetypes = ['.'+f if not f.startswith('.') else f for f in args.filetypes.split(';')]
    paths = {Path(p).resolve():p for p in args.paths}

    # create watchdog service
    zs = Zuschauer(paths=paths, filetypes=filetypes, storage=args.storage, recursive=args.recursive,
            refreshFrequency=args.refresh, verboseMode=args.verbose, dryRun=args.dryrun, storageService=storageService)

    # upload already available files
    if args.existing:
        if args.verbose:
            print(f"""-----------------\nUpload {'recursively' if args.recursive else ''} already existing files in:
                Paths: {list(paths.keys())}, with \nFiletypes: {filetypes}, to \nStorage: {args.storage}
            """)
        existing_files = {}
        nExist = 0
        for path in paths.keys():
            for ft in filetypes:
                foundExistingFiles = list(path.glob(f"{'*' if args.recursive else ''}*/*"+ft))
                nExistingFiles = len(foundExistingFiles)
                if nExistingFiles:
                    existing_files[path] = foundExistingFiles
        if len(existing_files):
            if args.verbose or args.dryrun:
                print(f"Uploading a total of {len(existing_files)} existing files.")
            if not args.dryrun:
                logging.info(f"Uploading a total of {len(existing_files)} existing files.")
                for existingFiles in existing_files.values():
                    for file_ in existingFiles:
                        if file_.is_file():
                            # upload with non-overwriting flag set to boost upload
                            zs.execAction(file_, overwrite=False)
        else:
            print(">>>> No existing files found. Nothing uploaded.\n-----------------\n\n")
    try:
        if args.verbose:
            print(f"""Starting watchdog with config:
                \nPaths: {list(paths.keys())}, \nFiletypes: {filetypes}, \nStorage: {args.storage}, \nRefreshRate: {args.refresh}
            """)
            print(f"Watch {'recursively' if args.recursive else ''} {list(paths.keys())}, action on file change {'would (--dryrun aktiv)' if args.dryrun else 'will'} copy on / overwrite in {args.storage}.")
        # start watchdog service 
        # watch filesystem for file creation
        zs.run()
    except KeyboardInterrupt:
        print('^C')
        exit(0)


if __name__ == "__main__":
    # create Tempfile
    logging.basicConfig(filename=Path(tempfile.gettempdir()).joinpath("zuschauer.log"), level=logging.INFO,
                        format='%(asctime)s-%(levelname)s: %(name)s "%(message)s"')

    logging.info(f"Starte Zuschauer\tby {__author__}\tv.{__version__}")
    # headless arg parsing
    parser = _parse_arguments()
    _args = parser.parse_args()
    configFile = _args.load

    configItems = {}
    # connectionString provided by arg?
    connString = _args.connectionString
    if connString is None and STORECREDENTIALS:
        # connectionString saved in keyring?
        connString = keyring.get_password("zuschauer@drahnreb", f"zs_connectionString_{platform.node()}")
        if connString:
            logging.info("retrieved connection string")
    configItems["connectionString"] = connString

    # config file available and connection string was retrieved (keyring or arg)
    if configFile.exists() and configFile.is_file() and configItems["connectionString"] is not None:
        logging.info(f'Loading config from file {configFile}')
        with open(configFile, 'rt') as f:
            t_args = argparse.Namespace()
            try:
                configItems.update(json.load(f))
                # add config options that are not necessary to be specified in config file but need to be initialized
                for k in ["save", "existing", "dryrun"]:
                    if k not in configItems.keys():
                        configItems[k] = False
                # consume current flags
                for k,v in _args.__dict__.items():
                    if not v is None and 'load' not in k:
                        configItems[k] = v
                t_args.__dict__.update(configItems)
                args = parser.parse_args(namespace=t_args)
            except BaseException as e:
                logging.error("Loading from config failed.", e)
                # if loading fails, prepare gooey interface
                args = parse_arguments(configItems)
    else:
        # ask for config, prepare gooey interface
        args = parse_arguments(configItems)

    # init logger before we start connection checks
    if args.verbose:
        level = logging.INFO
    else:
        level = logging.WARNING
    azureLogger = logging.getLogger('azure')
    azureLogger.setLevel(level)
    # # Configure a console output
    # handler = logging.StreamHandler(stream=sys.stdout)
    # handler.setLevel(level)
    # azureLogger.addHandler(handler)

    # check args including storage client and set up storageService
    storageService = checkArgs(args)

    if storageService is not None:
        # persist config for restart
        if args.save:
            if STORECREDENTIALS:
                keyring.set_password("zuschauer@drahnreb", f"zs_connectionString_{platform.node()}", str(args.connectionString))
            config = vars(args).copy()
            config['paths'] = [str(p) for p in args.paths]
            config['dryrun'] = False
            [config.pop(k, None) for k in ['save', 'load', 'existing', 'connectionString']]
            with open(CONFIGFILE, 'w') as outfile:
                json.dump(config, outfile, indent=2)
        # run main
        main(args, storageService)
    else:
        print("Arguments are wrong. Config not saved. Nothing uploaded. \n\nExit.")
        exit(1)