"""Table definitions and communications for DynamoDB."""

from botocore.exceptions import ClientError
import logging


logger = logging.getLogger(__name__)


class BuildingApprovals:
    """Amazon DynamoDB table for building approval data."""

    def __init__(self, dyn_resource):
        """Initialise a table object for building approvals.

        :param dyn_resource: A Boto3 DynamoDB resource.
        :type: boto3 object

        """
        self.dyn_resource = dyn_resource
        self.table_name = 'building_approvals'
        self.table = None

    def create_table(self):
        """Create an Amazon DynamoDB table for building approvals data.

        :return: A new table

        """
        try:
            self.table = self.dyn_resource.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {'AttributeName': 'lga_id', 'KeyType': 'HASH'},
                    {'AttributeName': 'date', 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'lga_id', 'AttributeType': 'S'},
                    {'AttributeName': 'date', 'AttributeType': 'S'}
                ],
                BillingMode='PAY_PER_REQUEST'
            )
            self.table.wait_until_exists()

        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", self.table_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

        else:
            return self.table

    def delete_table(self):
        """Delete the table."""
        try:
            self.dyn_resource.Table(self.table_name).delete()
            self.table = None

        except ClientError as err:
            logger.error(
                "Couldn't delete table. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def __repr__(self) -> str:
        """Return the table name and status."""
        print(f"Table '{self.table_name}' status:", self.table.table_status)
