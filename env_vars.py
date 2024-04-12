from enum import Enum
from os import environ

import botocore.session

green="\033[0;32m"
yellow="\033[0;33m"
off = "\033[0;37m" #dark grey as happy medium between light- and dark- mode.
red = "\033[1,31m"
happy = """( ˶ˆᗜˆ˵ )"""

class Environments(Enum):
    DEV = 'dev'
    STAGING = 'stag'
    PROD = 'prod'
    
    
VARS_UNIQUE_TO_ENV = [
    'AWS_ACCESS_KEY_ID',
    'AWS_APP_CLIENT_ID',
    'AWS_S3_REGION_NAME',
    'AWS_SECRET_ACCESS_KEY',
    'S3_BUCKET',
    'SQL_DATABASE',
    'SQL_PASSWORD',
    'SQL_USERNAME',
]

VARS_NOT_UNIQUE_TO_ENV = [
]

def get_env_vars_from_ssm(env_=Environments.DEV.value) -> str:
    session = botocore.session.get_session()
    ssm_client = session.create_client('ssm', region_name='ca-central-1')
    prefix = f'{env_}_'

    names_arr = [f'{prefix}{var}' for var in VARS_UNIQUE_TO_ENV]
    names_arr.extend(VARS_NOT_UNIQUE_TO_ENV)
    name_arr_10 = [] # get_parameter limited to groups of 10.
    name_list = []

    for name in names_arr:
        name_list.append(name)
        if len(name_list) == 10:
            name_arr_10.append(name_list)
            name_list = []
    
    if name_list:
        name_arr_10.append(name_list)

    for names in name_arr_10:
        parameters = ssm_client.get_parameters(Names=names)
        for parameter in parameters['Parameters']:
            print(f'set {parameter["Name"]}')
            key = parameter['Name'].strip(prefix)
            environ[key] = parameter['Value']
        if 'InvalidParameters' in parameters and len(parameters["InvalidParameters"]):
            print(f'{red}Could not set: {parameters["InvalidParameters"]} {off}')
        else:
            #add ascii art
            print(f'{green} ALL PARAMETERS SET!! WOO! \n {happy} {off}')

