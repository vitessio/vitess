resource "aws_instance" "vttablet-1000" {
  ami           = "${data.aws_ami.amazon_linux.id}"
  instance_type = "${var.instance-type}"
  key_name = "${aws_key_pair.key.key_name}"
  vpc_security_group_ids = ["${aws_security_group.default.id}"]
  
  tags = {
    Name = "${var.cluster-name}-vttablet"
    Cluster = "${var.cluster-name}"
    Component = "vttablet,vtgate"
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
{
	"${var.mysql-user}": [
		{
			"Password": "${var.mysql-password}",
			"UserData": "${var.mysql-user}"
		}
	]
}
EOT
    destination = "/tmp/userdata.json"
    connection {
      type = "ssh"
      user = "ec2-user"
    }
  }
  
  provisioner "file" {
    content= <<EOT
CELL=${local.cell-name}
TOPO_IMPLEMENTATION=etcd2
TOPO_GLOBAL_ROOT=/vitess/${var.cluster-name}
TOPO_GLOBAL_SERVER_ADDRESS=${aws_eip.vtctld.private_dns}:2379

EXTRA_VTGATE_FLAGS="-mysql_auth_server_impl static -mysql_auth_server_static_file=/etc/vitess/userdata.json"
EOT
    destination="/tmp/vtgate.conf"

    connection {
      type = "ssh"
      user = "ec2-user"
    }    
  }

  provisioner "file" {
    content= <<EOT
CELL=${local.cell-name}
KEYSPACE=${var.keyspace-name}
SHARD="-"
TOPO_IMPLEMENTATION=etcd2
TOPO_GLOBAL_ROOT=/vitess/${var.cluster-name}
TOPO_GLOBAL_SERVER_ADDRESS=${aws_eip.vtctld.private_dns}:2379

GRPC_PORT=16002
TABLET_PORT=17002
MYSQL_PORT=18002
MYSQL_FLAVOR=MariaDB103
VT_MYSQL_ROOT=/usr
EOT
    destination="/tmp/vttablet-1001.conf"

    connection {
      type = "ssh"
      user = "ec2-user"
    }    
  }

  provisioner "file" {
    content= <<EOT
CELL=${local.cell-name}
KEYSPACE=${var.keyspace-name}
SHARD="-"
TOPO_IMPLEMENTATION=etcd2
TOPO_GLOBAL_ROOT=/vitess/${var.cluster-name}
TOPO_GLOBAL_SERVER_ADDRESS=${aws_eip.vtctld.private_dns}:2379

GRPC_PORT=16003
TABLET_PORT=17003
MYSQL_PORT=18003
MYSQL_FLAVOR=MariaDB103
VT_MYSQL_ROOT=/usr
EOT
    destination="/tmp/vttablet-1002.conf"

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
      "sudo mv /tmp/MariaDB.repo /etc/yum.repos.d/",
      "sudo rpm --import https://yum.mariadb.org/RPM-GPG-KEY-MariaDB",
      "sudo yum install -y MariaDB-server galera MariaDB-client MariaDB-shared MariaDB-backup MariaDB-common",
      "curl -L https://github.com/etcd-io/etcd/releases/download/v3.3.12/etcd-v3.3.12-linux-amd64.tar.gz -o - | sudo tar xfvz - -C /usr/local/bin/ --strip-components=1 etcd-v3.3.12-linux-amd64/etcdctl etcd-v3.3.12-linux-amd64/etcd",
      "sudo mkdir /vt",
      "sudo tar xfvz /tmp/vitess_distro.tgz -C /vt",
      "sudo cp /vt/src/vitess.io/vitess/config/systemd/* /etc/systemd/system/",
      "sudo mkdir -p /etc/vitess/conf",
      "sudo mv /tmp/vtgate.conf /tmp/vttablet-1001.conf /tmp/vttablet-1002.conf /etc/vitess/conf/",
      "sudo mv /tmp/userdata.json /etc/vitess/",      
      "sudo chown -R vitess:vitess /vt",
      "sudo systemctl daemon-reload",
      "sudo systemctl enable vtgate@1.service",
      "sudo systemctl enable mysqlctld@1001.service",
      "sudo systemctl enable mysqlctld@1002.service",
      "sudo systemctl enable vttablet@1001.service",
      "sudo systemctl enable vttablet@1002.service",                  
      "sudo systemctl enable vitess-cluster.target",
      "sudo systemctl start vitess-cluster.target"
    ]

    connection {
      type = "ssh"
      user = "ec2-user"
    }
  }
}

resource "aws_lb_target_group_attachment" "vtgate-1000" {
  target_group_arn="${aws_lb_target_group.vtgate.arn}"
  target_id = "${aws_instance.vttablet-1000.id}"
  port = 3306
}

resource "aws_lb_target_group_attachment" "vtgate-ui-1000" {
  target_group_arn="${aws_lb_target_group.vtgate-ui.arn}"
  target_id = "${aws_instance.vttablet-1000.id}"
  port = 15001
}
