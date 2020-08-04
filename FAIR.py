import requests, json, os

FAIR_URL = 'https://clarklab.uvarc.io/'

def upload_file(file_path,metadata):
    """
    Uploads data with associated metadata using transfer service.
    Returns minted PID

    Parameters
    ----------
    file_path : string (mandatory)
        Path to file to be uploaded.
    metadata : json-ld (mandatory)
        json-ld metadata describing file.
    """

    if not isinstance(metadata,dict):
        raise Exception('metadata must be of type dictionary.')
    if not os.path.exists(file_path):
        raise Exception('file_path must point to local file.')

    upload_response = requests.post(
        FAIR_URL + 'transfer/data/',
        files = {
            'files':open(file_path,'rb'),
            'metadata':json.dumps(metadata)
        }
    )

    try:
        minted_id = upload_response.json()['Minted Identifiers'][0]
    except:
        return upload_response.json()

    return minted_id

def search(query):
    """
    text search metadata

    Parameters
    ----------
    query : string (mandatory)
        Query string.
    """
    if not isinstance(query,str):
        raise Exception('query must be of type string')

    matches = requests.get(FAIR_URL + 'search/' + query).json()['matches']

    return matches

def mint_id(metadata,namespace = '99999'):
    """
    Mint an identifier for given metadata.

    Parameters
    ----------
    metadata: dict (mandatory)
        metadata to mint id for.
    namespace: string
        namespace to mint id in.
    """

    if not isinstance(metadata,dict):
        raise Exception('metadata must be a dict.')

    created = requests.post(FAIR_URL + 'mds/shoulder/ark:' + namespace,data = json.dumps(metadata))

    try:
        return created.json()['created']
    except:
        return created.json()

def delete_id(pid):
    """
    Deletes the given id.

    Parameters
    ----------
    pid : string (mandatory)
        PID to be deleted.
    """

    if not isinstance(pid,str):
        raise Exception('PID must be string.')

    deleted = requests.delete(FAIR_URL + 'mds/' + pid)

    return deleted.json()


def update_pid(pid,changes):
    """
    Updates metadata for a given pid.

    Parameters
    ----------
    pid : string (mandatory)
        PID to be updated.
    changes: dict (mandatory)
        dict containing updates to metadata
    """

    if not isinstance(pid,str):
        raise Exception('PID must be string.')
    if not isinstance(changes,dict):
        raise Exception('changes must be a dict.')

    update = requests.put(FAIR_URL + 'mds/' + pid,data = json.dumps(changes))

    return update.json()

def retrieve_metadata(pid):
    """
    Retrives metadata from mds for given pid.

    Parameters
    ----------
    pid : string (mandatory)
        PID of interest.
    """

    if not isinstance(pid,str):
        raise Exception('PID must be string.')

    metadata_request = requests.get('https://clarklab.uvarc.io/mds/' + pid)

    return metadata_request.json()

def create_namespace(namespace,namespace_meta):
    """
    Create namespace.

    Parameters
    ----------
    namespace: string (mandatory)
        string of namespace.
    """

    namespace = requests.post(FAIR_URL + 'mds/ark:' + namespace,
                                data = json.dumps(namespace_meta))

    return namespace.json()

def compute(data_id,script_id,job_type,container_id = '',namespace = '99999'):
    """
    Runs computation on given data and script.

    Parameters
    ----------
    data_id : string or list(mandatory)
        PIDs of data to run computations on.
    script_id: string (mandatory)
        PID of script to run on data.
    job_type: string (mandatory)
        type of computation to run. Must be one of nipype, spark, custom.
    container_id: string
        if custom container PID of container to run on must be provided.
    namespace: string
        namespace where to mint computation Identifiers
    """

    if job_type not in ['spark','nipype','custom']:
        raise Exception('job_type must be one of spark, nipype, custom.')

    job = {
    "datasetID":data_id,
    "scriptID":script_id,
    'namespace':namespace
    }

    if job_type == 'custom':
        if container_id == '':
            raise Exception('Custom jobs require container id.')
        job['containerID'] = container_id


    job_request = requests.post(
        "https://clarklab.uvarc.io/compute/" + job_type,
        json = job
    )

    job_id = job_request.content.decode()

    return job_id

def list_running_jobs():
    """
    Returns list of all running jobs.
    """

    job_request = requests.get(
        "https://clarklab.uvarc.io/compute/job",
    )

    running_pods = job_request.json()['runningJobIds']

    return running_pods

def check_job_status(pid):
    """
    Returns status of job for given pid.

    Parameters
    ----------
    pid : string (mandatory)
        PID of interest.
    """
    if not isinstance(pid,str):
        raise Exception('PID must be string.')

    status = requests.get(FAIR_URL + 'compute/job/' + pid)

    return status.json()


def evidence_graph(pid):
    """
    Retrives evidence graph for given pid.

    Parameters
    ----------
    pid : string (mandatory)
        PID of interest.
    """

    if not isinstance(pid,str):
        raise Exception('PID must be string.')

    eg_request = requests.get(FAIR_URL + 'evidencegraph/' + pid)

    return eg_request.json()

def download_file(pid,file_name = ''):
    """
    Downloads data of given ark.

    Parameters
    ----------
    pid : string (mandatory)
        PID of interest.
    file_name: string
        File path to download file to.
    """

    if file_name == '':
        meta = retrieve_metadata(pid)
        try:
            file_name = meta['distribution'][0]['name']
        except:
            raise Exception('PID missing distribution.')


    data = requests.get(
    FAIR_URL + 'transfer/data/' + pid
    )

    data = data.content
    with open(file_name,'wb') as f:
        f.write(data)

    return 'Success'
