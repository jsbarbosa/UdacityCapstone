import os
from io import BytesIO
from typing import Callable

import boto3
import pandas as pd
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from botocore.exceptions import ClientError


class S3Operator(BaseOperator):
    def __init__(self, bucket: str, s3_conn_id: str, *args, **kwargs):
        super(S3Operator, self).__init__(*args, **kwargs)

        self._bucket = bucket
        self._s3_conn_id = s3_conn_id

    @property
    def bucket_name(self) -> str:
        return self._bucket.split('/')[0]

    @property
    def s3_conn_id(self) -> str:
        return self._s3_conn_id

    def get_connection(self):
        aws = AwsHook(
            aws_conn_id=self._s3_conn_id,
        ).get_credentials()

        return boto3.resource(
            's3',
            aws_access_key_id=aws.access_key,
            aws_secret_access_key=aws.secret_key,
        ).Bucket(
            self.bucket_name
        )

    def load_bytes(self, data: bytes):
        self.get_connection().put_object(
            Body=data,
            Key="/".join(
                self._bucket.split('/')[1:]
            )
        )


class StageToS3(S3Operator):
    """
    Operator that copies local data into S3
    """

    @apply_defaults
    def __init__(
            self,
            bucket: str,
            partition_by: list,
            s3_conn_id: str,
            path: str,
            read_func: Callable,
            *args,
            **kwargs
    ):
        super(StageToS3, self).__init__(
            bucket=bucket,
            s3_conn_id=s3_conn_id,
            *args,
            **kwargs
        )

        self._partition_by = partition_by
        self._path = path
        self._read_func = read_func

    def execute(self, context: dict):
        """
        Method that executes the AWS CLI COPY statement

        Parameters
        ----------
        context:
            Airflow context

        Returns
        -------

        """
        data = self._read_func(
            os.path.join(
                Variable.get('raw_data'),
                self._path
            )
        )

        self.log.info(
            f"Data from {self._path} has been loaded"
        )

        buffer = data.to_parquet(
            partition_cols=self._partition_by,
            index=False
        )

        self.load_bytes(
            buffer
        )

        self.log.info(
            f"Data from '{self._path}' staged to '{self._bucket}'"
        )


class DataQualityOperator(S3Operator):
    """
    Class that reads the Redshift data in order to verify that the pipeline
    was correct

    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
            self,
            bucket: str,
            s3_conn_id: str,
            tests: dict,
            *args,
            **kwargs
    ):
        """
        Initializer

        Parameters
        ----------
        tests:
            aws bucket keys to be verified
        aws_conn_id:
            reference to the Airflow aws connection
        """
        super(DataQualityOperator, self).__init__(
            bucket=bucket,
            s3_conn_id=s3_conn_id,
            *args,
            **kwargs
        )

        self._tests = tests

    @property
    def tests(self) -> dict:
        return self._tests

    def check_records(self, table: str, tests: list, hook):
        """
        Method that verifies if the query was successful and if records exist
        in the table parameter
        """
        try:
            records = hook.Object(
                table
            )
        except ClientError:
            class Temp:
                pass

            records = Temp()
            records.content_length = 0

        for test in tests:
            if not eval(test):
                self.log.error(
                    f"Table '{table}' failed check. When running: '{test}'"
                )
                raise ValueError(
                    f"Table '{table}' failed check."
                )

    def execute(self, context: dict):
        """
        Method that executes the data quality procedure

        Parameters
        ----------
        context:
            Airflow context
        """

        self.log.info(
            f"Checks for keys: {list(self._tests.keys())}"
        )

        hook = self.get_connection()

        for table, tests in self._tests.items():
            self.check_records(
                "/".join(table.split('/')[1:]),
                tests,
                hook
            )

            self.log.info(
                f"Check for key '{table}' was successful"
            )


class JobCheck(S3Operator):
    """
    S3 Operator that collects the previously generated parquet files and makes 
    a join between a dimension table and the fact table.
    """
    @apply_defaults
    def __init__(
            self,
            bucket: str,
            s3_conn_id: str,
            dimension_bucket: str,
            fact_bucket: str,
            dimension_id: str,
            fact_id: str,
            *args,
            **kwargs
    ):
        """
        
        Parameters
        ----------
        bucket:
            S3 bucket to read
        s3_conn_id:
            Airflow connection id
        dimension_bucket:
            path of the dimension table
        fact_bucket:
            path of the fact table
        dimension_id:
            column name of the dimension table id
        fact_id:
            column name of the fact table to join with dimension_id
        args
        kwargs
        """
        super(JobCheck, self).__init__(
            bucket=bucket,
            s3_conn_id=s3_conn_id,
            *args,
            **kwargs
        )

        self._dimension_bucket = dimension_bucket.replace(
            self.bucket_name,
            ''
        )[1:]
        self._fact_bucket = fact_bucket.replace(
            self.bucket_name,
            ''
        )[1:]
        self._dimension_id = dimension_id
        self._fact_id = fact_id

    def execute(self, context: dict):
        hook = self.get_connection()

        fact_keys = [
            item.key for item in hook.objects.filter(
                Prefix=self._fact_bucket
            ) if item.key.endswith('.parquet')
        ]

        if fact_keys:
            fact = pd.read_parquet(
                BytesIO(
                    hook.Object(
                        fact_keys[0]
                    ).get()['Body'].read()
                )
            )

            dimension = pd.read_parquet(
                BytesIO(
                    hook.Object(
                        self._dimension_bucket
                    ).get()['Body'].read()
                )
            )

            if dimension.merge(
                fact,
                left_on=self._dimension_id,
                right_on=self._fact_id,
                how='inner'
            ).empty:
                raise ValueError(
                    "No values can be joined"
                )

        else:
            raise ValueError("No fact data was found")
