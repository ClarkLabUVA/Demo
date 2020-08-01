import requests, json, os


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
        Exception('metadata must be of type dictionary.')

    upload_response = requests.post(
        'https://clarklab.uvarc.io/transfer/data/',
        files = {
            'files':open(file_path,'rb'),
            'metadata':json.dumps(metadata)
        }
    )

    try:
        minted_id = upload_response.json()['Minted Identifiers'][0]
    except:
        return 'Transfer Failure'

    return minted_id

def retrieve_metadata(pid):
    """
    Retrives metadata from mds for given pid.

    Parameters
    ----------
    pid : string (mandatory)
        PID of interest.
    """

    if not isinstance(pid,str):
        Exception('PID must be string.')

    metadata_request = requests.get('https://clarklab.uvarc.io/mds/' + pid)

    return metadata_request.json()

def compute(data_id,script_id,job_type,container_id = ''):
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
    """

    if job_type not in ['spark','nipype','custom']:
        Exception('job_type must be one of spark, nipype, custom.')

    job = {
    "datasetID":data_id,
    "scriptID":script_id
    }

    if job_type == 'custom':
        if container_id == '':
            Exception('Custom jobs require container id.')
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

def evidence_graph(pid):
    """
    Retrives evidence graph for given pid.

    Parameters
    ----------
    pid : string (mandatory)
        PID of interest.
    """

    if not isinstance(pid,str):
        Exception('PID must be string.')

    eg_request = requests.get('https://clarklab.uvarc.io/evidencegraph/' + pid)

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
        meta = retrive_metadata(pid)
        try:
            file_name = meta['distribution'][0]['name']
        except:
            Exception('PID missing distribution.')


    data = requests.get(
    'https://clarklab.uvarc.io/transfer/data/' + pid
    )
    data = data.content
    with open(file_name,'wb') as f:
        f.write(data)
