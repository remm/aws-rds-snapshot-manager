import boto3
import datetime
import argparse
import time
from botocore.exceptions import ClientError
from os import getenv

AWS_ACCESS_KEY_ID = getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = getenv('AWS_DEFAULT_REGION', "eu-west-1")  # Ireland
AWS_BACKUP_REGION = getenv('AWS_BACKUP_REGION', "eu-central-1")  # Frankfurt


client = boto3.client(
    'rds',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_DEFAULT_REGION
)

backup_region_client = boto3.client(
    'rds',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_BACKUP_REGION
)


def get_manual_snapshots_for_db_instance(client_obj, instance_id):
    """ Return list of manual snapshots for instance """
    db_snapshots = client_obj.describe_db_snapshots(
        DBInstanceIdentifier=instance_id,
        SnapshotType='manual'
    )
    return db_snapshots['DBSnapshots']


def get_snapshot_pairs(instance_id):
    backups = {}
    manual_snapshots = get_manual_snapshots_for_db_instance(client, instance_id)
    # print('Manual snapshots in default region: %s' % manual_snapshots)
    backup_manual_snapshots = get_manual_snapshots_for_db_instance(backup_region_client, instance_id)
    # print('Manual snapshots in backup region: %s' % backup_manual_snapshots)
    for snap in manual_snapshots:
        for bsnap in backup_manual_snapshots:
            if snap['DBSnapshotArn'] == bsnap['SourceDBSnapshotIdentifier']:
                backups[snap['DBSnapshotIdentifier']] = bsnap['DBSnapshotIdentifier']
    return backups


def create_snapshot_for_db_instance(instance_id):
    snapshot = 1
    try:
        date_now = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
        snapshot = client.create_db_snapshot(
            DBSnapshotIdentifier='%s-%s-%s' % ('scheduled', instance_id, date_now),
            DBInstanceIdentifier=instance_id,
            Tags=[
                {
                    'Key': 'type',
                    'Value': 'scheduled'
                },
            ]
        )
    except ClientError as cl_err:
        print("create_snapshot failed: %s" % cl_err.response['Error']['Message'])

    return snapshot


def wait_for_snapshot_creating(snapshot):
    status_str = 'Snapshot creating %s' % snapshot['DBSnapshot']['DBSnapshotIdentifier']
    while True:
        snapshot_status = client.describe_db_snapshots(
            DBSnapshotIdentifier=snapshot['DBSnapshot']['DBSnapshotIdentifier']
        )
        status_str += '.'
        print(status_str)
        time.sleep(90)
        if snapshot_status['DBSnapshots'][0]['Status'] == 'available':
            print('Snapshot created successfully.')
            break


def wait_for_snapshot_copying(snapshot):
    status_str = 'Snapshot copying %s' % snapshot['DBSnapshot']['DBSnapshotIdentifier']
    while True:
        snapshot_copying_status = backup_region_client.describe_db_snapshots(
            DBSnapshotIdentifier=snapshot['DBSnapshot']['DBSnapshotIdentifier']
        )
        status_str += '.'
        print(status_str)
        time.sleep(90)
        if snapshot_copying_status['DBSnapshots'][0]['Status'] == 'available':
            print('Snapshot copied successfully.')
            break


def delete_snapshot_for_db_instance(instance_id):
    response = client.delete_db_snapshot(
        DBSnapshotIdentifier=instance_id
    )
    return response


def delete_snapshot(client, snapshot_id):
    response = client.delete_db_snapshot(
        DBSnapshotIdentifier=snapshot_id
    )
    print('Deleting snapshot', response['DBSnapshot']['DBSnapshotIdentifier'])


def copy_snapshot_to_backup_region(snapshot, instance_id):
    snapshot_id = snapshot['DBSnapshot']['DBSnapshotIdentifier']
    _trigger_copying_snapshot_to_backup_region(backup_region_client, snapshot_id, instance_id)


def _trigger_copying_snapshot_to_backup_region(client_obj, snapshot_id, instance_id):
    src_db_snaps_arn = _get_arn_by_snapshot_id(snapshot_id, instance_id)
    response = client_obj.copy_db_snapshot(
        SourceDBSnapshotIdentifier=src_db_snaps_arn,
        TargetDBSnapshotIdentifier=snapshot_id,
        SourceRegion=AWS_DEFAULT_REGION
    )
    return response


def _get_arn_by_snapshot_id(snapshot_id, instance_id):
    manual_snaps = get_manual_snapshots_for_db_instance(client, instance_id)
    for snap in manual_snaps:
        if snapshot_id == snap['DBSnapshotIdentifier']:
            return snap['DBSnapshotArn']


def rotate_snapshots(client_obj, instance):
    expire_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
    manual_snapshots = get_manual_snapshots_for_db_instance(client_obj, instance)
    for snap in manual_snapshots:
        if snap['SnapshotCreateTime'] < expire_date:
            delete_snapshot(client_obj, snap['DBSnapshotIdentifier'])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--instance", help="RDS Instance ID")
    parser.add_argument("-s", "--snapshot", help="Snapshot ID")
    parser.add_argument("-ss", "--show_snapshots", action="store_true", help="Show snapshots for instance")
    parser.add_argument("-sbs", "--show_backup_snapshots", action="store_true",
                       help="Show snapshots for instance from backup region")
    parser.add_argument("-c", "--create_snapshot", action="store_true", help="Create snapshot for instance")
    parser.add_argument("-cp", "--copy_snapshot", action="store_true", help="Copy snapshot for backup region")
    parser.add_argument("-d", "--delete_snapshot", action="store_true", help="Delete snapshot for instance")
    parser.add_argument("-r", "--rotate_snapshot", action="store_true", help="Rotate snapshot for instance")
    args = parser.parse_args()

    if args.show_snapshots:
        if not args.instance:
            raise ValueError('Specify instance ID with -i argument')
        backups = get_snapshot_pairs(args.instance)
        print(backups)

    if args.create_snapshot:
        if not args.instance:
            raise ValueError('Specify instance ID with -i argument')
        snapshot = create_snapshot_for_db_instance(args.instance)
        wait_for_snapshot_creating(snapshot)

    if args.copy_snapshot:
        if not args.instance:
            raise ValueError('Specify instance ID with -i argument')
        if not args.create_snapshot:
            raise ValueError('Run copying to backup region only with -c option')
        copy_snapshot_to_backup_region(snapshot, args.instance)
        wait_for_snapshot_copying(snapshot)

    if args.delete_snapshot:
        if not args.snapshot:
            raise ValueError('Specify instance ID with -i argument')
        res = delete_snapshot_for_db_instance(args.snapshot)
        print(res)

    if args.rotate_snapshot:
        print('Rotate snapshots in default region')
        rotate_snapshots(client, args.instance)
        print('Rotate snapshots in backup region')
        rotate_snapshots(backup_region_client, args.instance)

    # TODO enable it the end of work
    # else:
    #     parser.print_help()
