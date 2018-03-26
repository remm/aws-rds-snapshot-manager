
# TODO

## Code
* [+] creating 
* [+] copying to backup region
* [+] deleting
* [+] show instance snapshots
* [] rotate snapshots (delete older than 14 days)

## Insfrastructure

* pack to docker container

# Libraries used in  script
* http://boto3.readthedocs.io/en/latest/
* * https://stackoverflow.com/questions/33068055/boto3-python-and-how-to-handle-errors
* https://docs.python.org/3/howto/argparse.html


## Bugs

Show Backup Instance Snapshots step doesn't return anything
#############################################
[scripts] Running shell script
+ python3 snapshot-manager.py -ss -i prod-db-2
{}
#############################################


Fix this workaround
#############################################
if args.copy_snapshot:
    if not args.instance:
        raise ValueError('Specify instance ID with -i argument')
#############################################