---
name: Bug Report
about: You're experiencing an issue with Vitess that is different than the documented behavior.

---

When filing a bug, please include the following headings if
possible. Any example text in this template can be deleted.

#### Overview of the Issue

A paragraph or two about the issue you're experiencing.

#### Reproduction Steps

Steps to reproduce this issue, example:

1. Deploy the following `vschema`:

    ```javascript
    {
      "sharded": true,
      "vindexes": {
        "hash": {
          "type": "hash"
        },
      "tables": {
        "user": {
          "column_vindexes": [
            {
              "column": "user_id",
              "name": "hash"
            }
          ]
        }
      }
    }
    ```

1. Deploy the following `schema`:

    ```sql
    create table user(user_id bigint, name varchar(128), primary key(user_id));
    ```

1. Run `SELECT...`
1. View error

#### Binary version
Example:

```sh
giaquinti@workspace:~$ vtgate --version
Version: a95cf5d (Git branch 'HEAD') built on Fri May 18 16:54:26 PDT 2018 by giaquinti@workspace using go1.10 linux/amd64
```

#### Operating system and Environment details

OS, Architecture, and any other information you can provide
about the environment.

- Operating system (output of `cat /etc/os-release`):
- Kernel version (output of `uname -sr`):
- Architecture (output of `uname -m`):

#### Log Fragments

Include appropriate log fragments. If the log is longer than a few dozen lines, please
include the URL to the [gist](https://gist.github.com/) of the log instead of posting it in the issue.
