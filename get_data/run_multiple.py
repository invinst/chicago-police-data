''' Runs the analysis pipeline on multiple Dropbox paths
in parallel container scripts.
Pass each directory as a command line argument.
'''

import logging
import sys

import civis
from civis.base import CivisJobFailure
from joblib import Parallel, delayed

LOG = logging.getLogger()
MAX_N_JOBS = 8


def run_container(dropbox_path):
    client = civis.APIClient()
    script_id = client.scripts.post_containers(
        name=f'Invisible Institute Data Run {dropbox_path}',
        docker_image_name='civisanalytics/datascience-python',
        docker_image_tag='5.0.0',
        required_resources={
            'cpu': 256,
            'memory': 4096,
            'disk_space': 5,
        },
        repo_http_uri='https://github.com/invinst/chicago-police-data.git',
        repo_ref='master',
        docker_command=f'''cd app
pip install -r requirements.txt
python -m get_data.run --path_to_execute {dropbox_path}''',
        params=[{'allowed_values': [],
                 'default': None,
                 'description': None,
                 'label': 'Dropbox Credential',
                 'name': 'DROPBOX_OAUTH',
                 'required': True,
                 'type': 'credential_custom',
                 'value': None}],
        arguments={'DROPBOX_OAUTH': 6644}
    )['id']

    run_id = client.scripts.post_containers_runs(script_id)['id']

    LOG.info(f'Analyzing {dropbox_path} in container script {script_id}'
             f' at run {run_id}')

    future = civis.futures.CivisFuture(
        client.scripts.get_containers_runs, (script_id, run_id)
    )

    try:
        result = future.result()
        state = result['state']
        LOG.info(f'Script {script_id} run {run_id} {state}')
    except CivisJobFailure:
        result = client.scripts.get_containers_runs(script_id, run_id)
        state = result['state']
        LOG.warning(f'Error: script {script_id} run {run_id} {state}')


def main(paths):
    Parallel(n_jobs=MAX_N_JOBS)(delayed(run_container)(path) for path in paths)


if __name__ == "__main__":
    LOGGING_PARAMS = {
        'stream': sys.stdout,
        'level': logging.INFO,
        'format': '%(message)s'
    }
    logging.basicConfig(**LOGGING_PARAMS)
    main(sys.argv[1:])
