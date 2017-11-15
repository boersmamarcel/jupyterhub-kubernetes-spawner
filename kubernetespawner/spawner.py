"""Kubernetes Spawner for JupyterHub.

See LICENSE file for full license history.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
"""
import glob
import json
import os
import string
import time

from jupyterhub.spawner import Spawner
from requests_futures.sessions import FuturesSession
from string import Template
from tornado import gen
from traitlets import Bool
from traitlets import default
from traitlets import Integer
from traitlets import List
from traitlets import Unicode


class UnicodeOrFalse(Unicode):
  info_text = 'a unicode string or False'

  def validate(self, obj, value):
    if value is False:
      return value
    return super(UnicodeOrFalse, self).validate(obj, value)


class Kubernetespawner(Spawner):
  """Kubernetespawner Class.

  Creates K8s Pods for each new user.

  """
  kube_api_endpoint = Unicode(
      'https://%s:%s' % (os.environ['KUBERNETES_SERVICE_HOST'],
                         os.environ['KUBERNETES_PORT_443_TCP_PORT']),
      config=True,
      help='Endpoint to use for kubernetes API calls')

  kube_api_version = Unicode(
      'v1',
      config=True,
      help='Kubernetes API version to use')

  kube_namespace = Unicode(
      'jupyter',
      config=True,
      help='Kubernetes Namespace to create pods in'
      'first create this by running: kubectl create ns jupyter')

  pod_name_template = Unicode(
      'jupyter-{username}-{userid}',
      config=True,
      help='Template to generate pod names. Supports: {user} for username')

  hub_ip_connect = Unicode(
      '',
      config=True,
      help='Endpoint that containers should use to contact the hub'
      'create a K8s service on TCP port 8081 then use'
      ' <service-name>.<namespace>.svc.cluster.local:8081')

  kube_ca_path = UnicodeOrFalse(
      '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt',
      config=True,
      help='Path to the CA crt to use to connect to the kube API server')

  kube_token = Unicode(
      config=True,
      help='Kubernetes API authorization token')

  singleuser_image_spec = Unicode(
      'jupyter/singleuser',
      config=True,
      help='Name of Docker image to use when spawning user pods')

  kube_termination_grace = Integer(
      0,
      config=True,
      help='Number of seconds to wait before terminating a pod')

  cpu_limit = Unicode(
      '2000m',
      config=True,
      help='Max number of CPU cores that a single user can use')

  cpu_request = Unicode(
      '200m',
      config=True,
      help='Min nmber of CPU cores that a single user is guaranteed')

  mem_limit = Unicode(
      '1Gi',
      config=True,
      help='Max amount of memory a single user can use')

  mem_request = Unicode(
      '128Mi',
      config=True,
      help='Min amount of memory a single user is guaranteed')

  volumes = List(
      [],
      config=True,
      help='Config for volumes present in the spawned user pod.'
      '{username} and {userid} are expanded.')

  volume_mounts = List(
      [],
      config=True,
      help='Config for volume mounts in the spawned user pod.'
      '{username} and {userid} are expanded.')

  create_user_volume_locally = Bool(
      False,
      config=True,
      help='If shared storage is available locally then '
      'create a username directory and volume')

  local_user_volume_path = Unicode(
      '/mnt/jupyterhub/notebooks',
      config=True,
      help='local directory that mounts a shared filesystem')

  configmap_volume_path = Unicode(
      '/mnt/configmap',
      config=True,
      help='local directory that mounts a K8s configmap volume '
      'for custom manifest files')

  custom_manifest_file = Unicode(
      'default',
      config=True,
      help='in the configmap_volume_path a json file that must exist '
      'that defines a custom pod definition')

  custom_images_list = List(
      [],
      config=True,
      help='List of Docker Images users choose in the spawn form.')

  use_options_form = Bool(
      False,
      config=True,
      help='Setting this to true will enable the default options '
      'form which uses Environment variables to capture which '
      'image specs to use and give resource limits options')

  options_form = Unicode()

  @default('kube_token')
  def _kube_token_default(self):
    try:
      with open('/var/run/secrets/kubernetes.io/serviceaccount/token') as f:
        return f.read().strip()
    except:
      return ''

  @default('options_form')
  def _options_form_default(self):
    """Get the HTML to present the user with a Spawn Form.

    If this function returns nothing, no form will be presented to the user.
    Otherwise, the HTML Form will be presented before spawning their instance.
    Will only activate if the setting 'use_options_form' is set to true in the
    jupyterhub_config.py

    Returns:
      String containing the HTML form to present to the user prior to spawning
    """
    self.log.debug('Evaluating default options_form')
    if self.use_options_form == True:
      self.log.debug('use_options_form == true : Getting Options Form HTML')
      default_env = 'YOURNAME=%s\n' % self.user.name.lower()
      file_select = self._detect_custom_manifest_files()
      image_select = self._load_custom_images_list()
      return """
            <label for="env">Environment variables (one per line)</label>
            <textarea name="env">{env}</textarea>
            {fs}
            {image}
            """.format(env=default_env, fs=file_select, image=image_select)
    else:
      return ''

  def _load_custom_images_list(self):
    if self.custom_images_list:
      image_dropdown = [
        '<label for="custom_image">Choose Custom Jupyter Image</label>',
        '<select id="custom_image" name="custom_image">',
        '<option value="default">default</option>'
        ]
      for i in self.custom_images_list:
        val = '<option value="{}">{}</option>'.format(i, i)
        image_dropdown.append(val)
      image_dropdown.append('</select>')
      return ''.join(image_dropdown)
    else:
      return ''

  @property
  def pod_name(self):
    return self._expand_user_properties(self.pod_name_template)

  def _detect_custom_manifest_files(self):
    """Detect custom manifest json files for select list.

    Adds custom manifest files into a select list for the options form

    Returns:
      Blank if no files were detected

      String of html to create a select input for the spawner web form
    """
    self.log.debug('Detecting custom manifest files for options form')
    custom_files = glob.glob(os.path.join(self.configmap_volume_path, '*.json'))
    if not custom_files:
      self.log.debug('No custom manifest files detected for options form')
      return ''
    cfile_dropdown = [
        '<label for="custom_manifest">Choose Custom Pod Manifest File</label>',
        '<select id="custom_manifest" name="custom_manifest">',
        '<option value="default">default</option>'
    ]

    for cf in custom_files:
      val = '<option value="%s">%s</option>' % (cf, cf.replace(self.configmap_volume_path+'/', ''))
      cfile_dropdown.append(val)
    cfile_dropdown.append('</select>')
    return ''.join(cfile_dropdown)

  def options_from_form(self, formdata):
    """Extract user submitted Spawn form data.

    Implementing this function enables custom forms for users to specify
    parameters before spawning their instance. In this case Environment
    variables and custom manifest files are the form options.

    Args:
      formdata: dictionary of user submitted form data

    Returns:
      Dictionary of parsed values from user form data
    """
    options = {}

    custom_file = formdata.get('custom_manifest', '')
    if custom_file:
      options['custom_manifest'] = custom_file[0]

    custom_image = formdata.get('custom_image', '')
    if custom_image:
      options['custom_image'] = custom_image[0]

    options['env'] = env = {}

    env_lines = formdata.get('env', [''])
    for line in env_lines[0].splitlines():
      if line:
        key, value = line.split('=', 1)
        env[key.strip()] = value.strip()

    return options

  def _expand_user_properties(self, template):
    """Expands username and userid into the template string.

    Args:
      template: A string containing {username} or {userid}

    Returns:
      a string formatted to substitue
    """
    safe_username = self._get_safe_username(self.user.name)
    return template.format(userid=self.user.id, username=safe_username)

  def _get_safe_username(self, name):
    """Modify name to meet restrictions for DNS labels.

    Args:
      name: String to be modified

    Returns:
      A new string that meets DNS label restrictions.
    """
    safe_chars = set(string.ascii_lowercase + string.digits)
    return ''.join([s if s in safe_chars else '-' for s in name.lower()])

  def _expand_all(self, src):
    if isinstance(src, list):
      return [self._expand_all(i) for i in src]
    elif isinstance(src, dict):
      return {k: self._expand_all(v) for k, v in src.items()}
    elif isinstance(src, str):
      return self._expand_user_properties(src)
    else:
      return src

  def _expand_custom_pod_manifest(self, cust_file, image_spec):
    """Read custom manifest from json file.

    Looks for a local json file, reads it to a string, and does subsitution
    on special variables from this class. Variable subsitution needs the
    variable to be in this format in the file: % (variable name)s
    Ideally the file would be from a Kubernetes ConfigMap Volume Mount.
    Use this feature to add additional Pod settings such as livenessProbe

    Args:
      cust_file: the custom definition file to load and sub variables

    Returns:
      A Kubernetes Pod manifest string
    """
    
    cust_manifest = Template(self._read_custom_manifest_file(cust_file))
    volmounts = json.dumps(self._get_secure_volume_mounts())
    vols = json.dumps(self._get_secure_volumes())
    username = self._expand_user_properties('{username}')
    env = json.dumps([{'name': k, 'value': v} for k, v in self.get_env().items()])
    env_minus_nbdir = json.dumps([{'name': k, 'value': v} for k, v in self.get_env(False).items()])
    subdict = dict(pod_name='"%s"'%self.pod_name ,
                  image_name='"%s"'%image_spec,
                  mem_request='"%s"'%self.mem_request,
                  mem_limit='"%s"'%self.mem_limit,
                  cpu_request='"%s"'%self.cpu_request,
                  cpu_limit='"%s"'%self.cpu_limit,
                  username='"%s"'%username,
                  username_nq=username,
                  volmounts=volmounts,
                  vols=vols,
                  env=env,
                  env_minus_nbdir=env_minus_nbdir)

    manifest = cust_manifest.safe_substitute(subdict)
    # technically using json.loads and then later using json.dumps could
    # result in unexpected output, but should be fine for this small use case
    return json.loads(manifest)

  def _read_custom_manifest_file(self, cust_file):
    """Read the file into a string.

    File should be Kubernetes Pod manifest json.

    Args:
      cust_file: value is either set by the user in the Options Form
      or specified at startup in the jupyterhub configuration file

    Returns:
      Raw string data from the cust_file file
    """

    data = ''
    with open(cust_file, 'r') as mfile:
      data = mfile.read()
    return data

  def get_pod_manifest(self):
    """Get the Pod definition to spawn.

    Loads either the custom file specified at startup, a custom file selected
    by the user in the Options Form, or the default definition.

    Returns:
      A dictionary representing the Json Pod manifest definition after
      variable subsitution
    """
    if self.user_options.get('custom_manifest'):
      self.custom_manifest_file = self.user_options.get('custom_manifest')
      self.log.debug('found custom manifest file %s', self.custom_manifest_file)
    image_spec = self.user_options.get('custom_image')
    if not image_spec:
      image_spec = self.singleuser_image_spec
    if self.custom_manifest_file and self.custom_manifest_file != 'default':
      custom_file = self.custom_manifest_file
      if not custom_file.startswith(self.configmap_volume_path):
        #If single file is configured in jupyterhub_config.py set up full path
        custom_file = os.path.join(self.configmap_volume_path, custom_file)
      if os.path.isfile(custom_file):
        return self._expand_custom_pod_manifest(custom_file, image_spec)
      else:
        self.log.error(
            'Could not load file: %s returning default manifest',
            custom_file)
    return self._get_default_pod_manifest(image_spec)

  def _get_secure_volume_mounts(self):
    """Append a VolumeMount to override the serviceaccount in Jupyter Pod.

       This is the only way to secure the K8's service account access
       since Jupyter allows access to the shell, and K8's hasn't fix this issue
       https://github.com/kubernetes/kubernetes/issues/16779#issuecomment-157460294

       Returns: 
          Dictionary of volume_mounts with added override to the serviceaccount path
    """

    secure_volume_mount = [{
      'name': 'no-api-access',
      'mountPath': '/var/run/secrets/kubernetes.io/serviceaccount',
      'readOnly': True
    }]
    return self._expand_all(self.volume_mounts) + secure_volume_mount

  def _get_secure_volumes(self):
    """Append a Volume to override the serviceaccount in Jupyter Pod.

    This is the only way to secure the K8's service account access
    since Jupyter allows access to the shell, and K8's hasn't fix this issue
    https://github.com/kubernetes/kubernetes/issues/16779#issuecomment-157460294

    Returns: 
        Dictionary of volumes plus an empty volume to mount instead of serviceaccount
    """
    secure_volumes = [{
        'name': 'no-api-access',
        'emptyDir': {}
    }]

    return self._expand_all(self.volumes) + secure_volumes

  def _get_default_pod_manifest(self, image_spec): 
    """Get the basic default manifest definition.

    Substitue the variables for user specific pod creation.

    Returns:
      The dictionary representing the json needed to create the new Pod.

    """
    return {
        'apiVersion': 'v1',
        'kind': 'Pod',
        'metadata': {
            'name': self.pod_name,
            'labels': {
                'name': self.pod_name
            }
        },
        'spec': {
            'containers': [
                {
                    'name': 'jupyter',
                    'image': image_spec,
                    'resources': {
                        'requests': {
                            'memory': self.mem_request,
                            'cpu': self.cpu_request,
                        },
                        'limits': {
                            'memory': self.mem_limit,
                            'cpu': self.cpu_limit
                        }
                    },
                    'env': [
                        {'name': k,
                         'value': v} for k, v in self.get_env().items()
                    ],
                    'volumeMounts': self._get_secure_volume_mounts()
                }
            ],
            'volumes': self._get_secure_volumes()
        }
    }

  def _create_user_notebook_dir(self):
    """Create the new user's notebook directory.

    Assuming that the Hub container is mounting a remote filesystem that will
    be shared by the spawned user containers, this will create a folder
    within the defined local_user_volume_path directory with the username.
    The volumes and volumeMounts in the Pod manifest must be defined properly
    in the jupyterhub_config.py file using this as an example:

    c.Kubernetespawner.volumes=[ {"name": "{username}-nfs", "nfs": {"path":
    "/mnt/jupyterhub/notebooks/{username}","server":"10.0.0.2"}}]
    c.Kubernetespawner.volume_mounts=[ {"name": "{username}-nfs", "mountPath":
    "/mnt/notebooks"} ]
    """
    if self.create_user_volume_locally:
      user_dir = self._expand_user_properties(self.local_user_volume_path +
                                              '/{username}')
      self.log.debug('UserDir for NFS  %s ', user_dir)
      if not os.path.exists(user_dir):
        os.makedirs(user_dir)

  def _get_pod_url(self, pod_name=None):
    url = '{host}/api/{version}/namespaces/{namespace}/pods'.format(
        host=self.kube_api_endpoint,
        version=self.kube_api_version,
        namespace=self.kube_namespace)
    if pod_name:
      url = url + '/' + pod_name
    self.log.info('got Pod URL  %s ', url)
    return url

  def _public_hub_api_url(self):
    if self.hub_ip_connect:
      proto, path = self.hub.api_url.split('://', 1)
      rest = path.split('/', 1)[1]
      retval = '{proto}://{ip}/{rest}'.format(proto=proto,
                                              ip=self.hub_ip_connect,
                                              rest=rest)
      self.log.debug('Created public hub api URL  %s ', retval)
      return retval
    else:
      self.log.debug('using default public hub api URL  %s ', self.hub.api_url)
      return self.hub.api_url

  def _env_keep_default(self):
    return []

  def get_env(self, include_nbdir=True):
    env = super(Kubernetespawner, self).get_env()
    env.update(dict(JPY_USER=self.user.name,
                JPY_COOKIE_NAME=self.user.server.cookie_name,
                JPY_BASE_URL=self.user.server.base_url,
                JPY_HUB_PREFIX=self.hub.server.base_url,
                JPY_HUB_API_URL=self._public_hub_api_url()))
    if include_nbdir:
      env.update(dict(NOTEBOOK_DIR=self.notebook_dir))
    
    if self.user_options.get('env'):
      env.update(self.user_options['env'])
    self.log.debug('JPY_COOKIE_NAME:  %s \n JPY_BASE_URL:  %s \n'
                   'JPY_HUB_PREFIX:  %s \n JPY_HUB_API_URL: %s',
                   self.user.server.cookie_name, self.user.server.base_url,
                   self.hub.server.base_url, self._public_hub_api_url())
    return env

  @property
  def session(self):
    if hasattr(self, '_session'):
      return self._session
    else:
      self._session = FuturesSession()
      auth_header = 'Bearer %s' % self.kube_token
      self._session.headers['Authorization'] = auth_header
      self._session.verify = self.kube_ca_path
      return self._session

  def load_state(self, state):
    super(Kubernetespawner, self).load_state(state)

  def get_state(self):
    state = super(Kubernetespawner, self).get_state()
    self.log.debug('Kubernetespawner get_state:  %s ', state)
    state['hi'] = 'hello'
    return state

  @gen.coroutine
  def get_pod_info(self, pod_name):
    resp = self.session.get(self._get_pod_url(),
                            params={'labelSelector': 'name = %s' % pod_name})
    data = yield resp
    return data.json()

  def is_pod_running(self, pod_info):
    return ('items' in pod_info and len(pod_info['items']) > 0 and
            pod_info['items'][0]['status']['phase'] == 'Running')

  @gen.coroutine
  def poll(self):
    data = yield self.get_pod_info(self.pod_name)
    if self.is_pod_running(data):
      return None
    return 1

  @gen.coroutine
  def start(self):
    self._create_user_notebook_dir()
    pod_manifest = self.get_pod_manifest()
    self.log.debug('Starting Kubernetespawner  %s ', pod_manifest)
    response = yield self.session.post(self._get_pod_url(),
                                   data=json.dumps(pod_manifest),
                                   timeout=360)
    if response.status_code >= 400:
      self.log.error("Error starting container response: %s", response)
    else:
      self.log.debug('Kubernetes apply API response %s ', response)
      while True:
        data = yield self.get_pod_info(self.pod_name)
        if self.is_pod_running(data):
          break
        time.sleep(5)
      self.user.server.ip = data['items'][0]['status']['podIP']
      self.user.server.port = 8888
      self.db.commit()

  @gen.coroutine
  def stop(self):
    body = {
        'kind': 'DeleteOptions',
        'apiVersion': 'v1',
        'gracePeriodSeconds': self.kube_termination_grace
    }
    response = yield self.session.delete(self._get_pod_url(self.pod_name),
                                      data=json.dumps(body))
    if response.status_code >= 400:
      self.log.error("Error deleting container response: %s", response)
    else:
      self.log.debug('Kubernetes stop API response %s ', response)
      while True:
        data = yield self.get_pod_info(self.pod_name)
        if 'items' not in data or not data['items']:
          break
        time.sleep(5)
