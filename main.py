import boto3
import datetime
from botocore.exceptions import ClientError

from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION

client = boto3.client(
    'rds',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_DEFAULT_REGION
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


def copy_snapshot_to_backup_region(snapshot_id):
    response = client.copy_db_snapshot(
        SourceDBSnapshotIdentifier=snapshot_id,
        TargetDBSnapshotIdentifier='string',
        Tags=[
            {
                'Key': 'string',
                'Value': 'string'
            },
        ],
        OptionGroupName='string',
        SourceRegion=AWS_DEFAULT_REGION
    )
    return response


if __name__ == "__main__":
    # get manual snapshots
    #manual_snapshots = get_manual_snapshots_for_db_instance("dev-db-1")
    #print(manual_snapshots)


    # for snap in DB_Snapshots["DBSnapshots"]:
    #     print(snap)


    # create
    res = create_snapshot_for_db_instance('dev-db-1')
    print(res)
    ####


    # delete
    # res = delete_snapshot_for_db_instance('scheduled-dev-db-1-2018-03-06-18-35')
    # print(res)