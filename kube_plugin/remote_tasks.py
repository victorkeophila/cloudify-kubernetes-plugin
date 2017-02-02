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


# Called when connecting to master.  Gets ip and port
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
    

# called to connect to a deployment proxy.  generalization of connect_master
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


def wait_until_pods_deleted(k8s_master_url, resource_name):
    cmd = "get po -l app={} --no-headers 2>/dev/null | grep {} | wc -l".format(resource_name, resource_name)
    current_pod_numbers = int(execute_kubectl_command(k8s_master_url, cmd))
    while current_pod_numbers != 0:
      ctx.logger.info("Waiting pods to ReplicationContoller {} to be deleted (remaining={})".format(resource_name, current_pod_numbers))
      time.sleep(1)
      current_pod_numbers = int(execute_kubectl_command(k8s_master_url, cmd))


#
# delete existing item
# Note ONLY FUNCTIONS FOR FILE BASED CONFIG
#
@operation
def kube_delete(**kwargs):
  k8s_master_ip = ctx.instance.runtime_properties['master_ip']
  k8s_master_port = ctx.instance.runtime_properties['master_port']
  k8s_master_url = "http://{}:{}".format(k8s_master_ip, k8s_master_port)

  if "kubernetes_resources" in ctx.instance.runtime_properties:
    for resource_name, kind_types in ctx.instance.runtime_properties['kubernetes_resources'].iteritems():
      ctx.logger.info("deleting resource={} types={}".format(resource_name, kind_types))
      for kind_type in kind_types:
        cmd = "delete {} {}".format(kind_type, resource_name)
        ctx.logger.info("Running command '{}'".format(cmd))
        execute_kubectl_command(k8s_master_url, cmd)
        if kind_type == 'ReplicationController':
          wait_until_pods_deleted(k8s_master_url, resource_name)


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


#
# Execute a bash command and return the output.
# Raise an Exception if the command returns an error code.
#
def execute_kubectl_command(master_url, args):
    check_kubectl()

    command = "sudo {} -s {} {}".format(KUBECTL_PATH, master_url, args)
    ctx.logger.info('Executing: {0}'.format(command))

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
        error_message = "Script {0} encountered error with return code {1} and standard output {2}, error output {3}".format(command, return_code,
                                                                                                                             stdout_consumer.buffer.getvalue(),
                                                                                                                             stderr_consumer.buffer.getvalue())
        error_message = str(unicode(error_message, errors='ignore'))
        ctx.logger.error(error_message)
        raise NonRecoverableError(error_message)
    else:
        ok_message = "Script {0} executed normally with standard output {1} and error output {2}".format(command, stdout_consumer.buffer.getvalue(),
                                                                                                         stderr_consumer.buffer.getvalue())
        ok_message = str(unicode(ok_message, errors='ignore'))
        ctx.logger.info(ok_message)

    return stdout_consumer.buffer.getvalue()


def wait_until_pods_running(k8s_master_url, yaml_content):
    if yaml_content['kind'] == 'ReplicationController':
        expected_replicas = int(yaml_content['spec']['replicas'])
        selector_app_value = yaml_content['spec']['selector']['app']
        cmd = "get po -l app={} --no-headers 2>/dev/null | grep Running | wc -l".format(selector_app_value)
        current_replicas = int(execute_kubectl_command(k8s_master_url, cmd))
        while current_replicas < expected_replicas:
            ctx.logger.info("Waiting for pod {} ({}/{})".format(selector_app_value, current_replicas, expected_replicas))
            time.sleep(1)
            current_replicas = int(execute_kubectl_command(k8s_master_url, cmd))


#
# Use kubectl to run and expose a service
#
@operation
def kube_run_expose(**kwargs):
  config = ctx.node.properties['config']
  config_files = ctx.node.properties['config_files']

  k8s_master_ip = ctx.instance.runtime_properties['master_ip']
  k8s_master_port = ctx.instance.runtime_properties['master_port']
  k8s_master_url = "http://{}:{}".format(k8s_master_ip, k8s_master_port)

  ctx.logger.info("Kubernetes master url={}".format(k8s_master_url))

  def write_and_run(yaml_content):
    fname="/tmp/kub_{}_{}.yaml".format(ctx.instance.id,time.time())
    with open(fname,'w') as f:
      yaml.safe_dump(yaml_content,f)
    cmd="create -f {}".format(fname)
    execute_kubectl_command(k8s_master_url, cmd)
    wait_until_pods_running(k8s_master_url, yaml_content)

  #embedded config
  if(config):
    write_and_run(config)

  #file config
  elif(len(config_files)):

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

      if('overrides' in file):
        try: 
          for o in file['overrides']:
            ctx.logger.info("exeing o={}".format(o))
            #check for substitutions
            o=process_subs(o)
            exec "base"+o in globals(),locals()
        except:
          raise(RecoverableError("Erreur in process_subs: {}".format(traceback.format_exc())))
      write_and_run(base)

    # Add service runtime properties
    for resource in resources:
       for kind_type in resources[resource]:
         if "Service" == kind_type:
           add_service_in_runtime_properties(k8s_master_url, resource, ctx)

    ctx.instance.runtime_properties['kubernetes_resources']=resources

  #built-in config
  else:
    # !! Not used in Alien4Cloud plugin !!
    # do kubectl run
    cmd = 'run {} --image={} --port={} --replicas={}'.format(ctx.node.properties['name'],ctx.node.properties['image'],ctx.node.properties['target_port'],ctx.node.properties['replicas'])
    if(ctx.node.properties['run_overrides']):
      cmd = cmd + " --overrides={}".format(ctx.node.properties['run_overrides'])
    execute_kubectl_command(k8s_master_url, cmd)

    # do kubectl expose
    cmd = 'expose rc {} --port={} --protocol={}'.format(ctx.node.properties['name'],ctx.node.properties['port'],ctx.node.properties['protocol'])
    if(ctx.node.properties['expose_overrides']):
      cmd = cmd + " --overrides={}".format(ctx.node.properties['expose_overrides'])
    execute_kubectl_command(k8s_master_url, cmd)

    # Add service runtime properties
    add_service_in_runtime_properties(k8s_master_url, ctx.node.properties['name'], ctx)


#
# Add service information into runtime properties to retrieve ports for endpoints
#
def add_service_in_runtime_properties(k8s_master_url, service_name, ctx):
  cmd = 'get service {} -o yaml'.format(service_name)
  ctx.logger.info("Running command to retrieve services: {}".format(cmd))
  output = execute_kubectl_command(k8s_master_url, cmd)
  service_k8s = yaml.load(output)
  ctx.logger.info("Service {}={}".format(service_name, service_k8s))
  service = {}
  service['name'] = service_name
  service['clusterIP'] = service_k8s['spec']['clusterIP']
  service['ports'] = service_k8s['spec']['ports']
  ctx.instance.runtime_properties['service'] = service
