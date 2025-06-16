# Readme

This directory provides a series of scripts that start Vitess services (topo, vtctld, vtgate, tablets etc).

They are intended to work like init.d scripts, so for example you can call then in sequence as:
```
/etc/init.d/topo start
/etc/init.d/vtctld start
```

.. and in future sequence shouldn't strictly be required, since they should be a bit smarter at waiting for each-other.

My intention is that they replace `common/scripts/*` but I will give users a chance to migrate over.

They make extensive use of `/etc/vitess.yaml` as a declarative source of configuration. For example, `mysqlctl-vttablet start` will start as many tablets as are defined here. If you add a new tablet in configuration, and then re-execute `start` it will launch any new tablets that hadn't previously started.
