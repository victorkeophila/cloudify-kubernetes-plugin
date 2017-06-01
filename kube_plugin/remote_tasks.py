########
# Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    * See the License for the specific language governing permissions and
#    * limitations under the License.

#
# Kubernetes plugin implementation
#
from cloudify import ctx, manager, utils
from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError, RecoverableError
from StringIO import StringIO
import os
import re
import subprocess
import sys
import threading
import time
import traceback
import yaml


KUBECTL_PATH = "/usr/local/bin/kubectl"

K8S_GET_SERVICE = "get_service"
K8S_PROPS_CLUSTERIP = "clusterIP"
K8S_PROPS_PORT = "port"

NB_RETRIES_MAX = 20


#
# Consumes subprocess logs
#
class OutputConsumer(object):
    def __init__(self, out):
        self.out = out
        self.buffer = StringIO()
        self.consumer = threading.Thread(target=self.consume_output)
        self.consumer.daemon = True
        self.consumer.start()

    def consume_output(self):
        for line in iter(self.out.readline, b''):
            self.buffer.write(line)
        self.out.close()

    def join(self):
        self.consumer.join()


#
# Raise an error if kubectl does not exist
#
def check_kubectl():
    if not os.path.exists(KUBECTL_PATH):
        error_message = "Couldn't find '{}'. Please make sure you have installed it on Cloudify manager".format(KUBECTL_PATH)
        ctx.logger.error(error_message)
        raise NonRecoverableError(error_message)
    else:
        version_command = "{} version --client".format(KUBECTL_PATH)
        try:
            subprocess.check_call(version_command, stderr=None, stdout=None, shell=True)
        except:
            raise NonRecoverableError("Couldn't execute the command {}".format(version_command))


#
# Execute a bash command and return the output.
# Raise an Exception if the command returns an error code.
#
def execute_kubectl_command(args):
    check_kubectl()

    k8s_master_ip = ctx.instance.runtime_properties['master_ip']
    k8s_master_port = ctx.instance.runtime_properties['master_port']
    k8s_master_url = "http://{}:{}".format(k8s_master_ip, k8s_master_port)

    command = "{} -s {} {}".format(KUBECTL_PATH, k8s_master_url, args)
    ctx.logger.debug('Executing: {0}'.format(command))

    process = subprocess.Popen(command,
                               shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               preexec_fn=os.setsid)

    return_code = None
    stdout_consumer = OutputConsumer(process.stdout)
    stderr_consumer = OutputConsumer(process.stderr)

    while True:
        return_code = process.poll()
        if return_code is not None:
            break
        time.sleep(0.1)

    stdout_consumer.join()
    stderr_consumer.join()

    if return_code != 0:
        error_message = "Command {0} encountered error with return code {1} and standard output {2}, error output {3}".format(command, return_code,
                                                                                                                             stdout_consumer.buffer.getvalue(),
                                                                                                                             stderr_consumer.buffer.getvalue())
        error_message = str(unicode(error_message, errors='ignore'))
        ctx.logger.warn(error_message)
        raise RecoverableError(error_message)
    else:
        ok_message = "Command {0} executed normally with standard output {1} and error output {2}".format(command, stdout_consumer.buffer.getvalue(),
                                                                                                         stderr_consumer.buffer.getvalue())
        ok_message = str(unicode(ok_message, errors='ignore'))
        ctx.logger.info(ok_message)

    return stdout_consumer.buffer.getvalue()


#
# Called when connecting to master.  Gets ip and port
#
@operation
def connect_master(**kwargs):
  ctx.logger.info("in connect_master")
  ctx.target.node._get_node_if_needed()
  if(ctx._local):
    ctx.logger.info("running local mode")
    ctx.source.instance.runtime_properties['master_ip'] = ctx.target.node.properties['ip']
  elif(ctx.target.node._node.type=='cloudify.kubernetes.Master'):
    ctx.logger.info("connecting to master node")
    ctx.source.instance.runtime_properties['master_ip'] = ctx.target.instance.runtime_properties['ip']
    #following properties ignored for non-fabric operation
    ctx.source.instance.runtime_properties['master_port'] = ctx.target.node.properties['master_port']
    ctx.source.instance.runtime_properties['ssh_username'] = ctx.target.node.properties['ssh_username']
    ctx.source.instance.runtime_properties['ssh_password'] = ctx.target.node.properties['ssh_password']
    ctx.source.instance.runtime_properties['ssh_port'] = ctx.target.node.properties['ssh_port']
    ctx.source.instance.runtime_properties['ssh_keyfilename'] = ctx.target.node.properties['ssh_keyfilename']
  elif(ctx.target.node._node.type == 'cloudify.nodes.DeploymentProxy'):
    ctx.logger.info("connecting to dproxy")
    ctx.logger.info("got *dproxy url:" + ctx.target.instance.runtime_properties['kubernetes_info']['url'])
    try:
      r = re.match('http://(.*):(.*)',ctx.target.instance.runtime_properties['kubernetes_info']['url'])
      ctx.logger.info("GROUPS:{}".format(r.groups()))
      ctx.source.instance.runtime_properties['master_ip']=r.group(1)
      ctx.source.instance.runtime_properties['master_port']=r.group(2)
    except:
      print "Unexpected error:", sys.exc_info()[0]
      raise
  else:
    raise(NonRecoverableError('unsupported relationship'))
 

#
# called to connect to a deployment proxy.  generalization of connect_master
#
@operation
def connect_proxy(**kwargs):
  if (ctx.target.node._node.type!='cloudify.nodes.DeploymentProxy'):
    raise (NonRecoverableError('must connect to DeploymentProxy type'))
  for output in ctx.target.node.properties['inherit_outputs']:
    ctx.source.instance.runtime_properties[output]=ctx.target.instance.runtime_properties[output]

    
@operation
def contained_in(**kwargs):
  ctx.source.instance.runtime_properties['ip']=ctx.target.instance.runtime_properties['ip']


@operation
def copy_rtprops(**kwargs):
  if (not "prop_list" in kwargs or kwargs["prop_list"]==""):
    return
  for prop in kwargs['prop_list'].split(','):
    if(prop in ctx.target.instance.runtime_properties):
      ctx.source.instance.runtime_properties[prop]=ctx.target.instance.runtime_properties[prop]


#
# Perform substitutions in overrides
#
def process_subs(s):

  with open("/tmp/subs","a+") as f:
    f.write("processing "+s)

  pat = '@{([^}]+)}|%{([^}]+)}'
  client = None
  m = re.search(pat,s)

  with open("/tmp/subs","a+") as f:
    f.write(" m "+str(m)+"\n")

  if(not m):
    #no patterns found
    ctx.logger.info('no pattern found:{}'.format(s))
    return s;

  ctx.logger.info("matching the configs")

  while(m):

    # Match @ syntax.  Gets runtime properties
    if(m.group(1)):
      with open("/tmp/subs","a+") as f:
        f.write(" m.group(1)="+str(m.group(1))+"\n")
      fields = m.group(1).split(',')
      if m and len(fields)>1:
        # do substitution
        if(not client):
          client = manager.get_rest_client()

        if( fields[0] == K8S_GET_SERVICE ):
          node_id = fields[1].strip()
          property_key = fields[2].strip()
          relationship_instance = get_one_relationship_instance(ctx, node_id)
          if relationship_instance is not None:
            if('service' in relationship_instance.runtime_properties):
              service_properties = relationship_instance.runtime_properties['service']
              if(property_key == K8S_PROPS_CLUSTERIP):
                val = service_properties['clusterIP']
              elif(property_key == K8S_PROPS_PORT):
                val = service_properties['ports'][0]['port']
              s = s[:m.start()] + str(val) + s[m.end(1) + 1:]
              ctx.logger.info("Service port property found ({}) on node {}".format(val, node_id))
            else:
              raise Exception("No service found on the node '{}'".format(node_id))
          else:
              raise Exception("No relationship found with node '{}'".format(node_id))
          m = re.search(pat, s)
        else:
          instances = client.node_instances.list(deployment_id=ctx.deployment.id,node_name=fields[0])
          if(instances and len(instances)):
            # Just use first instance if more than one
            node_instance = instances[0]
            # If variable not found in runtime properties, search in the node properties
            if( fields[1] not in node_instance.runtime_properties):
              node = client.nodes.get(deployment_id=ctx.deployment.id,node_id=node_instance.node_id)
              val = node.properties
            else:
              val = node_instance.runtime_properties
            # Get the value
            for field in fields[1:]:
              field = field.strip()
              if( field == "ip" ):
                # Special treatment for ip value
                host_instance_id = node_instance.host_id
                host_instance = client.node_instances.get(host_instance_id)
                if(host_instance):
                  val = host_instance.runtime_properties['ip']
                else:
                  raise Exception("ip not found for node: {}".format(fields[0]))
              else:
                val = val[field] #handle nested maps
            # Override the expression in the node
            s = s[:m.start()] + str(val) + s[m.end(1)+1:]
            m = re.search(pat,s)
          else:
            raise Exception("no instances found for node: {}".format(fields[0]))
      else:
        raise Exception("invalid pattern: " + s)

    # Match % syntax.  Gets context property.
    # also handles special token "management_ip"
    elif(m.group(2)):
      fields=m.group(2).split(',')
      if len(fields)>0:
        with open("/tmp/subs","a+") as f:
          f.write("m.group(2)=" + str(m.group(2)) + "\n")
        if(m.group(2)=="management_ip"):
          s = s[:m.start()] + str(utils.get_manager_ip()) + s[m.end(2) + 1:]
        else:
          s = s[:m.start()] + str(eval("ctx." + m.group(2))) + s[m.end(2) + 1:]
        m = re.search(pat,s)
      
  return s


#
# Retrieve the target relationship of the current instance based on the target node_id.
# It returns the first found instance.
#
def get_one_relationship_instance(ctx, node_id):
  for relationship in ctx.instance.relationships:
    if relationship.target is not None and relationship.target.node.id == node_id:
      return relationship.target.instance
  return None


#
# Wait to ensure that all pods are deleted
#
def wait_until_pods_deleted(resource_name):
    cmd = "get po -l app={} --no-headers 2>/dev/null | grep {} | wc -l".format(resource_name, resource_name)
    current_pod_numbers = int(execute_kubectl_command(cmd))
    while current_pod_numbers != 0:
      ctx.logger.info("Waiting pods to ReplicationContoller {} to be deleted (remaining={})".format(resource_name, current_pod_numbers))
      time.sleep(1)
      current_pod_numbers = int(execute_kubectl_command(cmd))


#
# delete existing item
# Note ONLY FUNCTIONS FOR FILE BASED CONFIG
#
@operation
def kube_delete(**kwargs):
  retry = 0
  metadata_name = ctx.instance.runtime_properties['metadata_name']
  while retry < NB_RETRIES_MAX: 
    try:

      ctx.logger.debug("Getting the number of replicas for resource {}".format(metadata_name))
      nb_replicas_cmd = "get rc --no-headers {}".format(metadata_name)
      total_instances = 0
      try:
        output = execute_kubectl_command(nb_replicas_cmd)
        total_instances = int(output.split()[1])
      except RecoverableError as e:
        # If the resource does not exist anymore. Ignore the error, otherwise throw the exception.
        if "NotFound" not in e.message:
          raise e

      if total_instances > 1:
        ctx.logger.info("Resource {} has {} replicas. Decrease number.".format(metadata_name, total_instances))
        _scale(False)
      else:
        ctx.logger.info("Resource {} has {} replica left. Delete all resources.".format(metadata_name, total_instances))
        if "kubernetes_resources" in ctx.instance.runtime_properties:
          for resource_name, kind_types in ctx.instance.runtime_properties['kubernetes_resources'].iteritems():
            ctx.logger.info("Deleting resource={} types={}".format(resource_name, kind_types))
            for kind_type in kind_types:
              cmd = "delete {} {}".format(kind_type, resource_name)
              try:
                execute_kubectl_command(cmd)
                if kind_type == 'ReplicationController':
                  wait_until_pods_deleted(resource_name)
              except RecoverableError as e:
                # If the resource does not exist anymore. Ignore the error, otherwise throw the exception.
                if "NotFound" not in e.message:
                  raise e
      return
    except RecoverableError:
      retry += 1
      ctx.logger.warn("Fail to delete {} (retry {}/{})".format(metadata_name, retry, NB_RETRIES_MAX))
      time.sleep(1)

  # If reach here, it means that the number of retries has been reached so some resource(s) has not been deleted.
  raise NonRecoverableError("Couldn't complete the deletion of resource {}. Some resources may remains in Kubernetes cluster.".format(metadata_name))


#
# Wait to ensure that pods of a ReplicationController are running
#
def wait_until_pods_running(yaml_content):
    if yaml_content['kind'] == 'ReplicationController':
        expected_replicas = int(yaml_content['spec']['replicas'])
        selector_app_value = yaml_content['spec']['selector']['app']
        cmd = "get po -l app={} --no-headers 2>/dev/null | grep Running | wc -l".format(selector_app_value)
        current_replicas = int(execute_kubectl_command(cmd))
        while current_replicas < expected_replicas:
            ctx.logger.info("Waiting for pod {} ({}/{})".format(selector_app_value, current_replicas, expected_replicas))
            time.sleep(1)
            current_replicas = int(execute_kubectl_command(cmd))


#
# Create a kubernetes resources using the create command
#
def _create_resource(yaml_content):
    fname="/tmp/kub_{}_{}.yaml".format(ctx.instance.id,time.time())
    with open(fname,'w') as f:
      yaml.safe_dump(yaml_content,f)
    cmd="create -f {}".format(fname)
    execute_kubectl_command(cmd)
    wait_until_pods_running(yaml_content)


#
# Create a service if it does not already exists.
#
def _create_service_if_not_exist(yaml_content):
  try:
    metadata_name = yaml_content['metadata']['name']
    ctx.logger.debug("Checking existence of Service {}".format(metadata_name))
    execute_kubectl_command("get svc {}".format(metadata_name))
  except:
    ctx.logger.info("Service {} does not exist. Create it.".format(metadata_name))
    try:
      _create_resource(yaml_content)
    except RecoverableError as e:
      if "already exists" in e.message:
        ctx.logger.info("As a matter of fact, the service {} already exists.".format(metadata_name))
        return


#
# Scale or unscale (add or delete 1 replica)
#
def _scale(add=True):
  metadata_name = ctx.instance.runtime_properties['metadata_name']

  action = "Scale" if add else "Unscale"
  ctx.logger.debug("{} resource {}".format(action, metadata_name))

  # Get current number of replicas
  ctx.logger.debug("Getting the number of replicas the resource {}".format(metadata_name))
  nb_replicas_cmd="get rc --no-headers {}".format(metadata_name)
  output = execute_kubectl_command(nb_replicas_cmd)
  total_instances = int(output.split()[1])
  amount = total_instances + (1 if add else -1)

  # Add 1 instance
  ctx.logger.info("The resource {} should currently have {} replicas. Try to increase to {}".format(metadata_name, total_instances, amount))
  scale_cmd = "scale --current-replicas={} --replicas={} rc {}".format(total_instances, amount, metadata_name)
  execute_kubectl_command(scale_cmd)


#
# Create a ReplicationController if it does not exist. Scale the instance by 1 otherwise.
#
def _create_or_scale(config, scale=False):
  metadata_name = ctx.instance.runtime_properties['metadata_name']
  retry = 0
  while retry < NB_RETRIES_MAX: 
    try:
      do_create = False
      try:
        ctx.logger.debug("Checking existence of ReplicationController {}".format(metadata_name))
        execute_kubectl_command("get rc {}".format(metadata_name))
        do_create = False
      except:
        do_create = True

      if do_create:
        ctx.logger.info("Resource {} does not exist yet. Create it.".format(metadata_name))
        _create_resource(config)
      else:
        ctx.logger.info("Resource {} already exist. Try to scale it.".format(metadata_name))
        _scale(True)

      # All good, the resource has been created
      return
    except RecoverableError:
      retry += 1
      ctx.logger.warn("Fail to scale {} (retry {}/{})".format(metadata_name, retry, NB_RETRIES_MAX))
      time.sleep(1)

  # If reach here, it means that the number of retries has been reached so some resource(s) has not been created.
  raise NonRecoverableError("Couldn't complete the creation of resource {}".format(metadata_name))


#
# Use kubectl to run and expose a service
#
@operation
def kube_run_expose(scale=False, **kwargs):
  config_files = ctx.node.properties['config_files']

  #file config
  if(len(config_files)):

    # /!\ Using dict won't work if using multiple Service or ReplicationController ...
    resources = {}
    for file in config_files:
      if (not ctx._local):
        local_path = ctx.download_resource(file['file'])
      else:
        local_path = file['file']

      with open(local_path) as f:
        base = yaml.load(f)

        #store kubernetes resources for uninstall
        metadata_name = base['metadata']['name']
        if metadata_name not in resources:
          resources[metadata_name] = []
          resources[metadata_name].append(base['kind'])
        if base['kind'] == 'ReplicationController':
          ctx.instance.runtime_properties['metadata_name'] = metadata_name

      if('overrides' in file):
        try: 
          for o in file['overrides']:
            ctx.logger.info("exeing o={}".format(o))
            #check for substitutions
            o=process_subs(o)
            exec "base" + o in globals(), locals()
        except:
          raise(RecoverableError("Erreur in process_subs: {}".format(traceback.format_exc())))

      if base['kind'] == 'ReplicationController':
        _create_or_scale(base, scale)
      elif base['kind'] == 'Service':
        _create_service_if_not_exist(base)
        # Add service runtime properties
        _add_service_in_runtime_properties(metadata_name, ctx)
      else:
        raise NonRecoverableError("Kind {} not supported in the plugin".format(base['kind']))

    ctx.instance.runtime_properties['kubernetes_resources']=resources


#
# Add service information into runtime properties to retrieve ports for endpoints
#
def _add_service_in_runtime_properties(service_name, ctx):
  cmd = 'get service {} -o yaml'.format(service_name)
  ctx.logger.info("Retrieve service {} details".format(service_name))
  output = execute_kubectl_command(cmd)
  service_k8s = yaml.load(output)
  service = {}
  service['name'] = service_name
  service['clusterIP'] = service_k8s['spec']['clusterIP']
  service['ports'] = service_k8s['spec']['ports']
  ctx.instance.runtime_properties['service'] = service
