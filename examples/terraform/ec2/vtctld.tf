resource "aws_network_interface" "vtctld" {
  subnet_id="${data.aws_subnet_ids.all.ids[0]}"
  security_groups = ["${aws_security_group.default.id}"]  
}

resource "aws_eip" "vtctld" {
  vpc=true
  network_interface="${aws_network_interface.vtctld.id}"
}

resource "aws_instance" "vtctld" {
  ami           = "${data.aws_ami.amazon_linux.id}"
  instance_type = "${var.instance-type}"
  key_name = "${aws_key_pair.key.key_name}"
  
  tags = {
    Name = "${var.cluster-name}-vtctld"
    Cluster = "${var.cluster-name}"
    Component = "vtctld"
  }

  network_interface {
    device_index            = 0
    network_interface_id    = "${aws_network_interface.vtctld.id}"
  }
  
  provisioner "file" {
    source="vitess_distro.tgz"
    destination="/tmp/vitess_distro.tgz"

    connection {
      type = "ssh"
      user = "ec2-user"
    }    
  }

  provisioner "file" {
    content= <<EOT
TOPO_IMPLEMENTATION=etcd2
TOPO_GLOBAL_ROOT=/vitess/${var.cluster-name}
TOPO_GLOBAL_SERVER_ADDRESS=${aws_eip.vtctld.private_dns}:2379

CELL_ROOT=/vitess/${var.cluster-name}/${local.cell-name}
CELL_TOPO_SERVER=${aws_eip.vtctld.private_dns}:2379
EOT
    destination="/tmp/vtctld-${local.cell-name}.conf"

    connection {
      type = "ssh"
      user = "ec2-user"
    }    
  }

  provisioner "file" {
    content= <<EOT
[mariadb]
name = MariaDB
baseurl = http://yum.mariadb.org/10.3/centos7-amd64
gpgkey=https://yum.mariadb.org/RPM-GPG-KEY-MariaDB
gpgcheck=1
EOT
    destination="/tmp/MariaDB.repo"

    connection {
      type = "ssh"
      user = "ec2-user"
    }
  }

  provisioner "remote-exec" {
    inline = [
      "sudo useradd vitess",
      "sudo useradd etcd",
      "sudo mkdir /var/lib/etcd",
      "sudo chown -R etcd:etcd /var/lib/etcd",
      "sudo mv /tmp/MariaDB.repo /etc/yum.repos.d/",
      "sudo rpm --import https://yum.mariadb.org/RPM-GPG-KEY-MariaDB",
      "sudo yum install -y MariaDB-server galera MariaDB-client MariaDB-shared MariaDB-backup MariaDB-common",
      "curl -L https://github.com/etcd-io/etcd/releases/download/v3.3.12/etcd-v3.3.12-linux-amd64.tar.gz -o - | sudo tar xfvz - -C /usr/local/bin/ --strip-components=1 etcd-v3.3.12-linux-amd64/etcdctl etcd-v3.3.12-linux-amd64/etcd",
      "sudo mkdir /vt",
      "sudo tar xfvz /tmp/vitess_distro.tgz -C /vt",
      "sudo cp /vt/src/vitess.io/vitess/config/systemd/* /etc/systemd/system/",
      "sudo mkdir -p /etc/vitess/conf",
      "sudo mv /tmp/vtctld-${local.cell-name}.conf /etc/vitess/conf/",
      "sudo chown -R vitess:vitess /vt",
      "sudo systemctl daemon-reload",
      "sudo systemctl start etcd.service",
      "sudo systemctl enable vtctld@${local.cell-name}.service",
      "sudo systemctl enable cell@${local.cell-name}.service",
      "sudo systemctl enable vitess-cluster.target",
      "sudo systemctl start vitess-cluster.target"
    ]

    connection {
      type = "ssh"
      user = "ec2-user"
    }
  }
}

resource "aws_lb_target_group_attachment" "vtctld-1" {
  target_group_arn="${aws_lb_target_group.vtctld.arn}"
  target_id = "${aws_instance.vtctld.id}"
  port = 15000
}
