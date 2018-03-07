import boto3
import datetime
import argparse
from botocore.exceptions import ClientError

from config import *

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


def get_manual_snapshots_for_db_instance(instance_id):
    """ Return list of manual snapshots for instance """
    db_snapshots = client.describe_db_snapshots(
        DBInstanceIdentifier=instance_id,
        SnapshotType='manual'
    )
    return db_snapshots['DBSnapshots']


def create_snapshot_for_db_instance(instance_id):
    response = 1
    try:
        date_now = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
        response = client.create_db_snapshot(
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
    return response


def delete_snapshot_for_db_instance(instance_id):
    response = client.delete_db_snapshot(
        DBSnapshotIdentifier=instance_id
    )
    return response


def _get_arn_by_snapshot_id(snapshot_id):
    manual_snaps = get_manual_snapshots_for_db_instance("dev-db-1")
    for snap in manual_snaps:
        if snapshot_id == snap['DBSnapshotIdentifier']:
            return snap['DBSnapshotArn']


def copy_snapshot_to_backup_region(client_obj, snapshot_id):
    src_db_snaps_arn = _get_arn_by_snapshot_id(snapshot_id)
    response = client_obj.copy_db_snapshot(
        SourceDBSnapshotIdentifier=src_db_snaps_arn,
        TargetDBSnapshotIdentifier='%s-copy' % snapshot_id,
        Tags=[
            {
                'Key': 'src-snap',
                'Value': snapshot_id
            },
        ],
        SourceRegion=AWS_DEFAULT_REGION
    )
    return response


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--instance", help="RDS Instance ID")
    parser.add_argument("-ss", "--show_snapshots", action="store_true", help="Show snapshots for instance")
    parser.add_argument("-c", "--create_snapshot", action="store_true", help="Create snapshot for instance")
    parser.add_argument("-d", "--delete_snapshot", action="store_true", help="Delete snapshot for instance")
    args = parser.parse_args()

    if args.show_snapshots:
        manual_snapshots = get_manual_snapshots_for_db_instance("dev-db-1")
        print(manual_snapshots)

    # for snap in DB_Snapshots["DBSnapshots"]:
    #     print(snap)

    # create
    # res = create_snapshot_for_db_instance('dev-db-1')
    # print(res)
    ####

    # delete
    # res = delete_snapshot_for_db_instance('scheduled-dev-db-1-2018-03-06-18-35')
    # print(res)

    # copy
    res = copy_snapshot_to_backup_region(backup_region_client, 'scheduled-dev-db-1-2018-03-07-11-34')
    print(res)
