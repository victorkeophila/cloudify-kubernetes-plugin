from kube_plugin import get_docker,edit_docker_config
from cloudify.decorators import operation
from cloudify import ctx
import os
import subprocess
import time

@operation
def start_master(**kwargs):
  if(not ctx.node.properties['install']):
    return

  os.chdir(os.path.expanduser("~"))

  K8S_VERSION="v1.2.4"
  ETCD_VERSION="v2.3.3"
  FLANNEL_VERSION="0.5.5"

  subprocess.call("sudo apt-get update",shell=True)

  if(ctx.node.properties['install_docker']):
    ctx.logger.info("getting docker")
    get_docker(ctx)

  master_port=ctx.node.properties['master_port']
  ip=ctx.instance.runtime_properties['ip']

  ctx.logger.info("in start_master")

  # START DOCKER
  pipe=subprocess.Popen(['sudo','nohup','docker','daemon','-H','unix:///var/run/docker-bootstrap.sock','-p','/var/run/docker-bootstrap.pid','--iptables=false','--ip-masq=false','--bridge=none','--graph=/var/lib/docker-bootstrap'],stdout=open('/dev/null'),stderr=open('/tmp/docker-bootstrap.log','w'),stdin=open('/dev/null'))
  time.sleep(2)
  if(pipe.returncode!=None):  #failed
    raise(NonRecoverableError('docker start failed'))

  print "starting etcd"

  # START ETCD
  res=subprocess.Popen(["sudo","docker","-H","unix:///var/run/docker-bootstrap.sock","run","-d","--net=host","quay.io/coreos/etcd:"+ETCD_VERSION,"--listen-client-urls=http://127.0.0.1:4001,http://"+ip+":4001","--advertise-client-urls=http://"+ip+":4001","--data-dir=/var/etcd/data"],stdout=subprocess.PIPE,stderr=open("/dev/null"))
  res.wait()
  ctx.logger.info("start etcd:"+str(res.returncode))
#  ctx.logger.info(" stderr:"+res.stdout.read())
  if(res.returncode):
    raise(NonRecoverableError('etcd start failed, ret={}'.format(res.returncode)))

  cid=res.stdout.read().strip()

  time.sleep(2)

  # SET CIDR RANGE FOR FLANNEL
  res=os.system("sudo docker -H unix:///var/run/docker-bootstrap.sock exec "+cid+" /etcdctl set /coreos.com/network/config '{ \"Network\": \"10.1.0.0/16\" }'")

  ctx.logger.info("set flannel cidr:"+str(res))

  if(res):
    raise(NonRecoverableError('set flannel cidr failed, ret={}'.format(res)))

  # stop docker

  os.system("sudo service docker stop")

  #run flannel

  pipe=subprocess.Popen(['sudo','docker','-H','unix:///var/run/docker-bootstrap.sock','run','-d','--net=host','--privileged','-v','/dev/net:/dev/net','quay.io/coreos/flannel:'+FLANNEL_VERSION],stderr=open('/dev/null'),stdout=subprocess.PIPE)

  # get container id
  cid=pipe.stdout.read().strip()
  pipe.wait()
  if(pipe.returncode):
    raise(NonRecoverableError('run flannel failed: ret={}'.format(pipe.returncode)))

  # get flannel subnet settings
  output=os.popen("sudo docker -H unix:///var/run/docker-bootstrap.sock exec {} cat /run/flannel/subnet.env".format(cid))
  flannel=";".join(output.read().split())
  print "flannel config:"+flannel
  if(not output or not flannel):
    raise(NonRecoverableError('get flannel config failed: ret={}'.format(output.returncode)))


  # edit docker config
  edit_docker_config(flannel)

  # remove existing docker bridge
  ret=os.system("sudo /sbin/ifconfig docker0 down")
  if(ret):
    raise(NonRecoverableError('docker0 down failed: ret={}'.format(ret)))
  os.system("sudo apt-get install -y bridge-utils")
  ret=os.system("sudo brctl delbr docker0")
  if(ret):
    raise(NonRecoverableError('brctl docker0 delete failed: ret={}'.format(ret)))

  # restart docker
  os.system("sudo service docker start")

  # start the master
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
        --api-servers=http://localhost:{} \
        --v=2 \
        --address=0.0.0.0 \
        --enable-server \
        --hostname-override=127.0.0.1 \
        --config=/etc/kubernetes/manifests-multi \
        --containerized".format(K8S_VERSION,master_port),shell=True)

  ctx.logger.info("start master:"+str(ret))
  if(ret):
    raise(NonRecoverableError('master start failed, ret={}'.format(ret)))

  # get kubectl
  ret=subprocess.call("wget http://storage.googleapis.com/kubernetes-release/release/{}/bin/linux/amd64/kubectl -O kubectl".format(K8S_VERSION),shell=True)
  if(ret):
    raise(NonRecoverableError('kubectl wget failed, ret={}'.format(ret)))
  subprocess.call("chmod +x kubectl",shell=True)

