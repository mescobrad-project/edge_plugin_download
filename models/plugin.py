from dataclasses import dataclass
from fileinput import filename
from typing import Any, List


import os
import shutil
import sys
import subprocess
import virtualenv
import configparser
import time


PLUGIN_CONF_FILE_NAME = 'mescobrad_edge/plugins/edge_plugin_download/plugin.config'
PLUGIN_CONF_MAIN_SECTION = 'plugin-configuration'
PLUGIN_OUTPUT_FILE_DEST = '.'
# PLUGIN_OUTPUT_FILE_NAME_FORMAT = '{plugin_name}-{timestamp}'

@dataclass
class PluginActionResponse():
    file_content_type: str = None
    file_content: List[Any] = None
    file_name: List[str] = None
    data_info: str = None

@dataclass
class PluginExchangeMetadata():
    file_name: List[str] = None
    file_content_type: str = None
    file_size: List[int] = None
    created_on: List[str] = None
    data_info: str = None

class EmptyPlugin():

    def __init__(self):
        # Dynamically set plugin configuration
        config = configparser.ConfigParser()
        config.read(PLUGIN_CONF_FILE_NAME)
        for k in config[PLUGIN_CONF_MAIN_SECTION]:
            self.__dict__[(f"__{k}__").upper()] = config[PLUGIN_CONF_MAIN_SECTION][k]

        # create a venv with the requirements specification
        self.__venv_path__ = os.path.join(os.path.abspath(os.path.dirname(os.path.realpath(__file__))), ".venv")
        print(self.__venv_path__)
        self.__setup_venv__()


    def __destroy__(self):
        # Check if venv folder exists
        if os.path.isdir(self.__venv_path__):
            # Remove venv
            shutil.rmtree(self.__venv_path__, ignore_errors=True)

    def __setup_venv__(self):
        # Check if venv folder exists
        if not os.path.isdir(self.__venv_path__):
            # Create new venv
            virtualenv.cli_run([self.__venv_path__])
            # Activate venv
            self.__activate_venv__()
            # install pre_requisite on the venv
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])

    def __activate_venv__(self):
        # Activate the venv on the current process
        activate_this_file = f"{self.__venv_path__}/bin/activate_this.py"
        exec(open(activate_this_file).read())

    def __load__(self, input_file: PluginExchangeMetadata) -> Any:
        # Load input data
        with open(f"{PLUGIN_OUTPUT_FILE_DEST}/{input_file.file_name}", 'rb') as f:
            return f.read()

    def __store__(self, output: PluginActionResponse) -> PluginExchangeMetadata:
        file_sizes = []
        files_created_on = []
        file_names = []
        for file_content, file_name in zip(output.file_content, output.file_name):
            file_name = os.path.basename(file_name)
            file_names.append(file_name)
            files_created_on.append(str(round(time.time()*1000)))

            # Create output file
            with open(f"{PLUGIN_OUTPUT_FILE_DEST}/{file_name}", 'wb') as dest_file:
                dest_file.write(file_content.encode() if type(file_content)==str else file_content)

            # Get its size
            file_sizes.append(os.path.getsize(f"{PLUGIN_OUTPUT_FILE_DEST}/{file_name}"))

        # Build Metadata
        out_meta = PluginExchangeMetadata(file_name=file_names,
                                          file_content_type=output.file_content_type,
                                          file_size=file_sizes,
                                          created_on=files_created_on,
                                          data_info=output.data_info)

        return out_meta

    def action(self, data: any) -> List[PluginActionResponse]:
        pass

    def __execute__(self, inputFileMetadata: PluginExchangeMetadata = None) -> PluginExchangeMetadata:
        self.__activate_venv__()
        # Execute plugin-specific action
        output = self.action(inputFileMetadata)

        if output.file_content_type is not None and output.file_content is not None and output.file_name is not None:
            # Store the action output
            outputFileMetadata = (self.__store__(output))
        else:
            # Create an empty exchange metadata
            outputFileMetadata = (PluginExchangeMetadata())

        return outputFileMetadata
