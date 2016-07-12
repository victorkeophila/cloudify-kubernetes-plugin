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
from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError, RecoverableError
from cloudify import ctx,manager,utils
from fabric.api import env, put, run, sudo
import os
import sys
import re
import time
import subprocess
import yaml
import traceback

K8S_GET_SERVICE_CLUSTERIP = "get_service_clusterIP"
K8S_GET_SERVICE_PORT = "get_service_port"

# Called when connecting to master.  Gets ip and port
@operation
def connect_master(**kwargs):
  ctx.logger.info("in connect_master")
  ctx.target.node._get_node_if_needed()
  if(ctx._local):
    ctx.logger.info("running local mode")
    ctx.source.instance.runtime_properties['master_ip']=ctx.target.node.properties['ip']
  elif(ctx.target.node._node.type=='cloudify.kubernetes.Master'):
    ctx.logger.info("connecting to master node")
    ctx.source.instance.runtime_properties['master_ip']=ctx.target.instance.runtime_properties['ip']
    #following properties ignored for non-fabric operation
    ctx.source.instance.runtime_properties['master_port']=ctx.target.node.properties['master_port']
    ctx.source.instance.runtime_properties['ssh_username']=ctx.target.node.properties['ssh_username']
    ctx.source.instance.runtime_properties['ssh_password']=ctx.target.node.properties['ssh_password']
    ctx.source.instance.runtime_properties['ssh_port']=ctx.target.node.properties['ssh_port']
    ctx.source.instance.runtime_properties['ssh_keyfilename']=ctx.target.node.properties['ssh_keyfilename']
  elif(ctx.target.node._node.type=='cloudify.nodes.DeploymentProxy'):
    ctx.logger.info("connecting to dproxy")
    ctx.logger.info("got *dproxy url:"+ctx.target.instance.runtime_properties['kubernetes_info']['url'])

    try:

      r=re.match('http://(.*):(.*)',ctx.target.instance.runtime_properties['kubernetes_info']['url'])

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

  pat='@{([^}]+)}|%{([^}]+)}'
  client=None
  m=re.search(pat,s)

  with open("/tmp/subs","a+") as f:
    f.write(" m "+str(m)+"\n")

  if(not m):
    #no patterns found
    ctx.logger.info('no pattern found:{}'.format(s))
    return s;

  ctx.logger.info("matching the configs")

  k8s_custom_func = [K8S_GET_SERVICE_CLUSTERIP, K8S_GET_SERVICE_PORT]

  while(m):

    # Match @ syntax.  Gets runtime properties
    if(m.group(1)):
      with open("/tmp/subs","a+") as f:
        f.write(" m.group(1)="+str(m.group(1))+"\n")
      fields=m.group(1).split(',')
      if m and len(fields)>1:
        # do substitution
        if(not client):
          client=manager.get_rest_client()

        if( fields[0] in k8s_custom_func ):
          node_id = fields[1].strip()
          if(fields[0] == K8S_GET_SERVICE_CLUSTERIP):
            relationship_instance = get_one_relationship_instance(ctx, node_id)
            if relationship_instance is not None:
              if('service' in relationship_instance.runtime_properties):
                val = relationship_instance.runtime_properties['service']['clusterIP']
                s = s[:m.start()] + str(val) + s[m.end(1) + 1:]
                ctx.logger.info("Service clusterIP property found ({}) on node {}".format(val, node_id))
              else:
                ctx.logger.info("No service found on the node '{}'".format(node_id))
            else:
              ctx.logger.info("No relationship found with node '{}'".format(node_id))
          elif(fields[0] == K8S_GET_SERVICE_PORT):
            relationship_instance = get_one_relationship_instance(ctx, node_id)
            if relationship_instance is not None:
              if('service' in relationship_instance.runtime_properties):
                val = relationship_instance.runtime_properties['service']['ports'][0]['port']
                s = s[:m.start()] + str(val) + s[m.end(1) + 1:]
                ctx.logger.info("Service port property found ({}) on node {}".format(val, node_id))
              else:
                ctx.logger.info("No service found on the node '{}'".format(node_id))
            else:
                ctx.logger.info("No relationship found with node '{}'".format(node_id))
          m = re.search(pat, s)
        else:
          instances=client.node_instances.list(deployment_id=ctx.deployment.id,node_name=fields[0])
          if(instances and len(instances)):
            #just use first instance if more than one
            val=instances[0].runtime_properties
            for field in fields[1:]:
              field=field.strip()
              val=val[field]    #handle nested maps

            s=s[:m.start()]+str(val)+s[m.end(1)+1:]
            m=re.search(pat,s)
          else:
            raise Exception("no instances found for node: {}".format(fields[0]))
      else:
        raise Exception("invalid pattern: "+s)

    # Match % syntax.  Gets context property.
    # also handles special token "management_ip"
    elif(m.group(2)):
      fields=m.group(2).split(',')
      if len(fields)>0:
        with open("/tmp/subs","a+") as f:
          f.write("m.group(2)="+str(m.group(2))+"\n")
        if(m.group(2)=="management_ip"):
          s=s[:m.start()]+str(utils.get_manager_ip())+s[m.end(2)+1:]
        else:
          s=s[:m.start()]+str(eval("ctx."+m.group(2)))+s[m.end(2)+1:]
        m=re.search(pat,s)
      
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
# delete existing item
# Note ONLY FUNCTIONS FOR FILE BASED CONFIG
#
@operation
def kube_delete(**kwargs):
  env['host_string']=ctx.instance.runtime_properties['master_ip']
  env['user']=ctx.node.properties['ssh_username']
  env['key_filename']=ctx.node.properties['ssh_keyfilename']

  if "kubernetes_resources" in ctx.instance.runtime_properties:
    for resource_name, kind_types in ctx.instance.runtime_properties['kubernetes_resources'].iteritems():
      ctx.logger.info("deleting resource={} types={}".format(resource_name, kind_types))
      for kind_type in kind_types:
        cmd="sudo /usr/local/bin/kubectl delete {} {}".format(kind_type, resource_name)
        output=run(cmd)
        if(output.return_code):
          raise(NonRecoverableError('kubectl delete failed:{}'.format(output.stderr)))
  
      
  #cmd="sudo /usr/local/bin/kubectl delete {}s {}".format(ctx.instance.runtime_properties['kind'],ctx.node.properties['name'])
  
  #output=run(cmd)
  #if(output.return_code):
  #  raise(NonRecoverableError('kubectl delete failed:{}'.format(output.stderr)))

#
# Use kubectl to run and expose a service
#
@operation
def kube_run_expose(**kwargs):
  config=ctx.node.properties['config']
  config_files=ctx.node.properties['config_files']
  env['host_string']=ctx.instance.runtime_properties['master_ip']
  env['user']=ctx.node.properties['ssh_username']
  env['key_filename']=ctx.node.properties['ssh_keyfilename']

  ctx.logger.info("fabric settings.  host_string={}  user={} key_filename={}".format(ctx.instance.runtime_properties['master_ip'],ctx.node.properties['ssh_username'],ctx.node.properties['ssh_keyfilename']))

  def write_and_run(d):
    fname="/tmp/kub_{}_{}.yaml".format(ctx.instance.id,time.time())
    with open(fname,'w') as f:
      yaml.safe_dump(d,f)
    put(fname,fname)
    cmd="sudo /usr/local/bin/kubectl -s http://localhost:8080 create -f "+fname + " >> /tmp/kubectl.out 2>&1"
    ctx.logger.info("running create: {}".format(cmd))

    output=run(cmd)
    if(output.return_code):
      raise(NonRecoverableError('kubectl create failed:{}'.format(output.stderr)))

  #embedded config
  if(config):
    write_and_run(config)

  #file config
  elif(len(config_files)):

    # /!\ Using dict won't work if using multiple Service or ReplicationController ...
    resources={}
    for file in config_files:
      if (not ctx._local):
        local_path=ctx.download_resource(file['file'])
      else:
        local_path=file['file']
      with open(local_path) as f:
        base=yaml.load(f)

        #store kubernetes resources for uninstall
        metadata_name=base['metadata']['name']
        if metadata_name not in resources:
          resources[metadata_name]=[]
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
           add_service_in_runtime_properties(resource, ctx)

    ctx.instance.runtime_properties['kubernetes_resources']=resources


  #built-in config
  else:
    # do kubectl run
    cmd='sudo /usr/local/bin/kubectl -s http://localhost:8080 run {} --image={} --port={} --replicas={}'.format(ctx.node.properties['name'],ctx.node.properties['image'],ctx.node.properties['target_port'],ctx.node.properties['replicas'])
    if(ctx.node.properties['run_overrides']):
      cmd=cmd+" --overrides={}".format(ctx.node.properties['run_overrides'])

    output=run(cmd)
    if(output.return_code):
      raise(NonRecoverableError('kubectl run failed:{}'.format(output.stderr)))

    # do kubectl expose
    cmd='sudo /usr/local/bin/kubectl -s http://localhost:8080 expose rc {} --port={} --protocol={}'.format(ctx.node.properties['name'],ctx.node.properties['port'],ctx.node.properties['protocol'])
    if(ctx.node.properties['expose_overrides']):
      cmd=cmd+" --overrides={}".format(ctx.node.properties['expose_overrides'])

    output=call(cmd)
    if(output.return_code):
      raise(NonRecoverableError('kubectl expose failed:{}'.format(output.stderr)))

    # Add service runtime properties
    add_service_in_runtime_properties(ctx.node.properties['name'], ctx)

def add_service_in_runtime_properties(service_name, ctx):
  cmd='sudo /usr/local/bin/kubectl -s http://localhost:8080 get service {} -o yaml'.format(service_name)
  ctx.logger.info("Running command to retrieve services: {}".format(cmd))
  output=run(cmd)
  service_k8s=yaml.load(output)
  ctx.logger.info("Service {}={}".format(service_name, service_k8s))
  service={}
  service['name']=service_name
  service['clusterIP']=service_k8s['spec']['clusterIP']
  service['ports']=service_k8s['spec']['ports']
  ctx.instance.runtime_properties['service']=service
