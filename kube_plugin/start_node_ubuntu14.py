from cloudify.decorators import operation
from kube_plugin import get_docker, edit_docker_config
from cloudify import ctx
import os
import subprocess
import time
import socket
import fcntl
import struct

@operation
def start_node(**kwargs):
  if(not ctx.node.properties['install']):
    return

  os.chdir(os.path.expanduser("~"))

  K8S_VERSION="v1.2.4"
  ETCD_VERSION="v2.3.3"
  FLANNEL_VERSION="0.5.5"

  subprocess.call("sudo apt-get update",shell=True)

  if(ctx.node.properties['install_docker']):
    get_docker(ctx)
  
  master_ip=ctx.instance.runtime_properties['master_ip']
  master_port=ctx.instance.runtime_properties['master_port']

  # UPDATE HOSTS
  res=subprocess.Popen(["hostname"],stdout=subprocess.PIPE,stderr=open("/dev/null"))
  res.wait()

  hn=res.stdout.read().strip()

  os.system("sudo sh -c 'echo {} {} >> /etc/hosts'".format(ctx.instance.host_ip,hn))

  ctx.logger.info("updated hosts file: {} {}".format(ctx.instance.host_ip,hn))

  ctx.logger.info("got inputs master_ip={} master_port={}".format(master_ip,master_port))

  # START DOCKER
  pipe=subprocess.Popen(['sudo','nohup','docker','daemon','-H','unix:///var/run/docker-bootstrap.sock','-p','/var/run/docker-bootstrap.pid','--iptables=false','--ip-masq=false','--bridge=none','--graph=/var/lib/docker-bootstrap'],stdout=open('/dev/null'),stderr=open('/tmp/docker-bootstrap.log','w'),stdin=open('/dev/null'))
  time.sleep(2)
  if(pipe.returncode!=None):  #failed
    raise(NonRecoverableError('docker start failed'))

  time.sleep(2)
  
  os.system("sudo service docker stop")
  
  # RUN FLANNEL

  pipe=subprocess.Popen(['sudo','docker','-H','unix:///var/run/docker-bootstrap.sock','run','-d','--net=host','--privileged','-v','/dev/net:/dev/net','quay.io/coreos/flannel:'+FLANNEL_VERSION,"/opt/bin/flanneld", "--etcd-endpoints=http://{}:4001".format(master_ip)],stderr=open('/dev/null'),stdout=subprocess.PIPE)

  # GET CONTAINER ID
  cid=pipe.stdout.read().strip()
  print cid
  pipe.wait()
  if(pipe.returncode):
    raise(NonRecoverableError('run flannel failed: ret={}'.format(pipe.returncode)))

  # GET FLANNEL SUBNET SETTINGS
  output=os.popen("sudo docker -H unix:///var/run/docker-bootstrap.sock exec {} cat /run/flannel/subnet.env".format(cid))
  flannel=";".join(output.read().split())
  print "flannel config:"+flannel
  if(not output or not flannel):
    raise(NonRecoverableError('get flannel config failed: ret={}'.format(output.returncode)))

  # EDIT DOCKER CONFIG
  edit_docker_config(flannel)

  # remove existing docker bridge
  ret=os.system("sudo /sbin/ifconfig docker0 down")
  if(ret):
    raise(NonRecoverableError('docker0 down failed: ret={}'.format(ret)))
  os.system("sudo apt-get install -y bridge-utils")
  ret=os.system("sudo brctl delbr docker0")
  if(ret):
    raise(NonRecoverableError('brctl docker0 delete failed: ret={}'.format(ret)))

  # RESTART DOCKER
  os.system("sudo service docker start")

  # START THE KUBE
  ret=subprocess.call("sudo docker run \
    --volume=/:/rootfs:ro \
    --volume=/sys:/sys:ro \
    --volume=/var/lib/docker/:/var/lib/docker:rw \
    --volume=/var/lib/kubelet/:/var/lib/kubelet:rw \
    --volume=/var/run:/var/run:rw \
    --net=host \
    --privileged=true \
    --pid=host \
    -d \
    gcr.io/google_containers/hyperkube-amd64:{} \
    /hyperkube kubelet \
        --allow-privileged=true \
        --api-servers=http://{}:{} \
        --v=2 \
        --address=0.0.0.0 \
        --enable-server \
        --config=/etc/kubernetes/manifests-multi \
        --containerized".format(K8S_VERSION,master_ip,master_port),shell=True)

  ctx.logger.info("start master:"+str(ret))
  if(ret):
    raise(NonRecoverableError('master start failed, ret={}'.format(ret)))

  # RUN THE PROXY
#  ret=subprocess.call("sudo docker run -d --net=host --privileged gcr.io/google_containers/hyperkube:{} /hyperkube proxy --master=http://{}:{} --v=2".format(K8S_VERSION,master_ip,master_port),shell=True)
#  if(ret):
#    ctx.logger.info("proxy start failed")
#    return 1
    #raise(NonRecoverableError('master start failed, ret={}'.format(ret)))

