ENV["LC_ALL"] = "en_US.UTF-8"

Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/xenial64"
  config.vm.hostname = 'vitess'

  config.vm.network "private_network", type: "dhcp"

  # vtctld
  config.vm.network "forwarded_port", guest: 8000, host: 8000 # http
  config.vm.network "forwarded_port", guest: 15000, host: 15000 # http
  config.vm.network "forwarded_port", guest: 15999, host: 15999 # grpc

  # vtgate
  config.vm.network "forwarded_port", guest: 15001, host: 15001 # http
  config.vm.network "forwarded_port", guest: 15991, host: 15991 # grpc
  config.vm.network "forwarded_port", guest: 15306, host: 15306 # mysql

  # vttablet 1
  config.vm.network "forwarded_port", guest: 15100, host: 15100 # http
  config.vm.network "forwarded_port", guest: 16100, host: 16100 # grpc

  # vttablet 2
  config.vm.network "forwarded_port", guest: 15101, host: 15101 # http
  config.vm.network "forwarded_port", guest: 16101, host: 16101 # grpc

  # vttablet 3
  config.vm.network "forwarded_port", guest: 15102, host: 15102 # http
  config.vm.network "forwarded_port", guest: 16102, host: 16102 # grpc

  # vttablet 4
  config.vm.network "forwarded_port", guest: 15103, host: 15103 # http
  config.vm.network "forwarded_port", guest: 16103, host: 16103 # grpc

  # vttablet 5
  config.vm.network "forwarded_port", guest: 15104, host: 15104 # http
  config.vm.network "forwarded_port", guest: 16104, host: 16104 # grpc

  # Demo Appp
  config.vm.network "forwarded_port", guest: 8000, host: 8000 # http

  # N.B. It's possible to use NFS to help speed up IO operations in the VM but
  # some OSX users have reported issues running govendor with it enabled.
  # Additional details in https://github.com/vitessio/vitess/issues/4396
  #
  # To enable, use:
  #   config.vm.synced_folder ".", "/vagrant/src/vitess.io/vitess", type: "nfs"
  config.vm.synced_folder ".", "/vagrant/src/vitess.io/vitess"

  config.vm.provider :virtualbox do |vb|
    vb.name = "vitess"
    vb.customize ["modifyvm", :id, "--ioapic", "on"]
    vb.customize ["modifyvm", :id, "--cpuexecutioncap", "85"]
    vb.customize [ "modifyvm", :id, "--uartmode1", "disconnected" ]
    vb.memory = 12888
    vb.cpus = 4
  end
  config.vm.provision "shell", path: "./vagrant-scripts/bootstrap_vm.sh"
end
