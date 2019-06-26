resource "aws_lb" "vitess" {
  name="vitess-${var.cluster-name}"
  load_balancer_type = "network"
  subnets = ["${data.aws_subnet_ids.all.ids}"]
  enable_cross_zone_load_balancing = true
  
  tags = {
    Cluster = "${var.cluster-name}"
  }  
}

resource "aws_lb_target_group" "vtgate" {
  name = "vitess-${var.cluster-name}-vtgate-tg"
  vpc_id = "${data.aws_vpc.default.id}"  
  target_type = "instance"
  protocol = "TCP"
  port = "3306"
}

resource "aws_lb_listener" "vtgate" {
  load_balancer_arn = "${aws_lb.vitess.arn}"
  port = "3306"
  protocol =  "TCP"

  default_action {
    type = "forward"
    target_group_arn = "${aws_lb_target_group.vtgate.arn}"
  }
}

resource "aws_lb_target_group" "vtgate-ui" {
  name = "vitess-${var.cluster-name}-vtgate-ui-tg"
  vpc_id = "${data.aws_vpc.default.id}"  
  target_type = "instance"
  protocol = "TCP"
  port = "15001"
}

resource "aws_lb_listener" "vtgate-ui" {
  load_balancer_arn = "${aws_lb.vitess.arn}"
  port = "15001"
  protocol =  "TCP"

  default_action {
    type = "forward"
    target_group_arn = "${aws_lb_target_group.vtgate-ui.arn}"
  }
}

resource "aws_lb_target_group" "vtctld" {
  name = "vitess-${var.cluster-name}-vtctld-tg"
  vpc_id = "${data.aws_vpc.default.id}"
  target_type = "instance"
  protocol = "TCP"
  port = "15000"
}

resource "aws_lb_listener" "vtctld" {
  load_balancer_arn = "${aws_lb.vitess.arn}"
  port = "15000"
  protocol =  "TCP"

  default_action {
    type = "forward"
    target_group_arn = "${aws_lb_target_group.vtctld.arn}"
  }
}
