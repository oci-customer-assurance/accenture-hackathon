#####################################
## Oracle Linux 8 nats.io k8 setup ##
#####################################
## Install nats server
sudo su -
dnf update -y
wget https://github.com/nats-io/nats-server/releases/download/v2.0.4/nats-server-v2.0.4-386.rpm
rpm -ivh nats-server-v2.0.4-386.rpm