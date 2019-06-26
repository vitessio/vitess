variable "region" {
  default="us-west-1"
}

variable "instance-type" {
  default="t3.medium"
}

variable "cluster-name" {
  default="vitess"
}

variable "keyspace-name" {
  default="messagedb"
}

variable "mysql-user" {
  default="mysql_user"
}

variable "mysql-password" {
  default="mysql_password"
}

variable "public-key" {}

locals{
  cell-name="${replace(var.region,"-","")}"
}

provider "aws" {
  region="${var.region}"
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnet_ids" "all" {
  vpc_id = "${data.aws_vpc.default.id}"
}

data "aws_ami" "amazon_linux" {
  most_recent = true
  owners = ["amazon"]

  filter {
    name = "name"
    values = [
      "amzn2-ami-hvm-*-x86_64-gp2",
    ]
  }

  filter {
    name = "owner-alias"
    values = [
      "amazon",
    ]
  }
}

resource "aws_key_pair" "key" {
  key_name = "yubikey"
  public_key = "${var.public-key}"
}

resource "aws_security_group" "default" {
  name = "${var.cluster-name}-default"
  description = "A security group for the vitess hosts"
  vpc_id = "${data.aws_vpc.default.id}"
}

resource "aws_security_group_rule" "egress-all" {
  type="egress"
  from_port=0
  to_port=0
  protocol = "-1"
  cidr_blocks =["0.0.0.0/0"]

  security_group_id = "${aws_security_group.default.id}"
}

resource "aws_security_group_rule" "internal" {
  type="ingress"
  from_port=0
  to_port=0
  protocol = "-1"

  security_group_id = "${aws_security_group.default.id}"
  source_security_group_id = "${aws_security_group.default.id}"  
}

resource "aws_security_group_rule" "inbound-ssh" {
  type = "ingress"
  from_port = 22
  to_port = 22
  protocol = "tcp"
  cidr_blocks = ["0.0.0.0/0"]

  security_group_id = "${aws_security_group.default.id}"
}

resource "aws_security_group_rule" "inbound-mysql" {
  type = "ingress"
  from_port = 3306
  to_port = 3306
  protocol = "tcp"
  cidr_blocks = ["0.0.0.0/0"]

  security_group_id = "${aws_security_group.default.id}"
}

resource "aws_security_group_rule" "inbound-vitess" {
  type = "ingress"
  from_port = 15000
  to_port = 19000
  protocol = "tcp"
  cidr_blocks = ["0.0.0.0/0"]

  security_group_id = "${aws_security_group.default.id}"
}

