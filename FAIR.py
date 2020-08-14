#© 2020 By The Rector And Visitors Of The University Of Virginia

#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
import requests, json, os

FAIR_URL = 'https://clarklab.uvarc.io/'
TOKEN = ''
def upload_file(file_path,metadata,token = TOKEN):
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
        },
        headers = {"Authorization": token}
    )

    try:
        minted_id = upload_response.json()['Minted Identifiers'][0]
    except:
        return upload_response.content.decode()

    return minted_id

def search(query,token = TOKEN):
    """
    text search metadata

    Parameters
    ----------
    query : string (mandatory)
        Query string.
    """
    if not isinstance(query,str):
        raise Exception('query must be of type string')

    matches = requests.get(FAIR_URL + 'search/' + query,
                            headers = {"Authorization": token}
                                                        ).json()['matches']

    return matches

def mint_id(metadata,namespace = '99999',token = TOKEN):
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

    created = requests.post(FAIR_URL + 'mds/shoulder/ark:' + namespace,
                data = json.dumps(metadata),
                headers = {"Authorization": token})

    try:
        return created.json()['created']
    except:
        return created.content.decode()

def delete_id(pid,token = TOKEN):
    """
    Deletes the given id.

    Parameters
    ----------
    pid : string (mandatory)
        PID to be deleted.
    """

    if not isinstance(pid,str):
        raise Exception('PID must be string.')

    deleted = requests.delete(FAIR_URL + 'mds/' + pid,
                            headers = {"Authorization": token})

    return deleted.json()


def update_pid(pid,changes,token = TOKEN):
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

    update = requests.put(FAIR_URL + 'mds/' + pid,
                            data = json.dumps(changes),
                            headers = {"Authorization": token})

    return update.json()

def retrieve_metadata(pid,token = TOKEN):
    """
    Retrives metadata from mds for given pid.

    Parameters
    ----------
    pid : string (mandatory)
        PID of interest.
    """

    if not isinstance(pid,str):
        raise Exception('PID must be string.')

    metadata_request = requests.get('https://clarklab.uvarc.io/mds/' + pid,
                                headers = {"Authorization": token})

    try:
        return metadata_request.json()
    except:
        return metadata_request.content.decode()

def create_namespace(namespace,namespace_meta,token = TOKEN):
    """
    Create namespace.

    Parameters
    ----------
    namespace: string (mandatory)
        string of namespace.
    """

    namespace = requests.post(FAIR_URL + 'mds/ark:' + namespace,
                                data = json.dumps(namespace_meta),
                                headers = {"Authorization": token})

    return namespace.json()

def compute(data_id,script_id,job_type,container_id = '',namespace = '99999',token = TOKEN):
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
        json = job,
        headers = {"Authorization": token}
    )

    job_id = job_request.content.decode()

    return job_id

def list_running_jobs(token = TOKEN):
    """
    Returns list of all running jobs.
    """

    job_request = requests.get(
        "https://clarklab.uvarc.io/compute/job",
        headers = {"Authorization": token}
    )

    running_pods = job_request.json()['runningJobIds']

    return running_pods

def evidence_graph(pid,token = TOKEN):
    """
    Retrives evidence graph for given pid.

    Parameters
    ----------
    pid : string (mandatory)
        PID of interest.
    """

    if not isinstance(pid,str):
        raise Exception('PID must be string.')

    eg_request = requests.get('https://clarklab.uvarc.io/evidencegraph/' + pid,
                        headers = {"Authorization": token})

    return eg_request.json()

def download_file(pid,file_name = '',token = TOKEN):
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
    'https://clarklab.uvarc.io/transfer/data/' + pid,
    headers = {"Authorization": token}
    )

    data = data.content
    with open(file_name,'wb') as f:
        f.write(data)

    return 'Success'
