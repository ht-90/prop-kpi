"""Process data file to fir into a corresponding database table."""

import os
import pandas as pd
import json
from decimal import Decimal


class BuildingApprovalsProcessor:
    """Read and process building approvals data file into dict format."""

    def __init__(self, data_dir, data_file):
        """Initialise building approvals processor object.

        :param data_dir: A directory path for a data file
        :type: str
        :param data_file: A file name
        :type: str
        """
        self.map_state = {
            "04": "nsw",
            "08": "vic",
            "12": "qld",
            "16": "sa",
            "20": "wa",
            "24": "tas",
            "28": "nt",
            "32": "act",
        }
        self.cols = {
            "lga_id": int,
            "lga_name": str,
            "new_houses": int,
            "new_other_res": int,
            "total_dwell": int
        }
        self.sheet = "Table_1"
        self.dfile = data_file
        self.ddir = data_dir

    def process_data(self, data_id, attrs):
        """Process building approvals data file to dict format.

        :param data_id: A starting data record id
        :type: int
        :param attrs: DynamoDB table attributes
        :type: dict
        :returns: A list of data in dict format and an updated data id
        :rtype: list and int
        """
        # Extract metadata and data types
        meta_info = self.parse_metadata()
        col_dtypes = get_dtypes_from_dynamodb(attrs=attrs)

        # Read data file and process as a dataframe
        pengine = self.get_processor_engine()
        df = self.read_excel_file(engine=pengine)

        # Set columns and data types
        df.columns = self.cols.keys()
        df = df.astype(self.cols)

        # Add columns for metadata
        df = self.add_metadata(df=df, meta=meta_info)
        df = df.astype(col_dtypes)

        return convert_df_to_json(df)

    def get_processor_engine(self):
        """Return an appropriate engine name based on a file format.

        :returns: A pandas read_excel engine name
        :rtype: str
        """
        fformat = self.dfile.split(".")[-1]
        if fformat == "xls":
            return "xlrd"
        elif fformat == "xlsx":
            return "openpyxl"
        else:
            print(f"'{fformat}' is not processed - needs to be either '.xls' or '.xlsx'.")

    def read_excel_file(self, engine):
        """Read data and remove empty rows.

        :returns: A dataframe sliced for a data cube
        :rtype: pandas DataFrame
        """
        df = pd.read_excel(
            os.path.join(self.ddir, self.dfile),
            engine=engine,
            sheet_name=self.sheet,
            skiprows=6
        ).iloc[:, :5]
        rm_row = df[df[df.columns[0]].isna()].index[0]

        return df.iloc[:rm_row, :]

    def parse_metadata(self):
        """Get metadata info from file name.

        :returns: Meta information extracted from a data file name
        :rtype: dict
        """
        file_parts = self.dfile.split("_")
        meta = {
            "year": int(file_parts[1][:4]),
            "month": int(file_parts[1][4:6]),
            "state_id": file_parts[0][-2:],
            "state_name": self.map_state[file_parts[0][-2:]],
            "date": f"{file_parts[1][:4]}-{file_parts[1][4:6]}-01"
        }

        return meta

    def add_metadata(self, df, meta):
        """Add metadata as new columns.

        :param df: A dataframe to update
        :type: pandas DataFrame
        :param meta: Meta information
        :type: dict
        :returns: A dataframe with metadata added as new columns
        :rtype: pandas DataFrame
        """
        for key in meta.keys():
            df[key] = meta[key]

        return df


def get_dtypes_from_dynamodb(attrs):
    """Set column data types for dataframe.

    :param attrs: DynamoDB table attributes
    :type: dict
    :returns: Pairs of column names and column values {"column name": data type}
    :rtype: dict
    """
    col_dtypes = dict()
    for attr in attrs:
        if attr["AttributeType"] == "S":
            col_dtypes.update({attr["AttributeName"]: str})
        elif attr["AttributeType"] == "N":
            col_dtypes.update({attr["AttributeName"]: int})

    return col_dtypes


def convert_df_to_json(df):
    """Convert dataframe to json.

    :param: A pandas dataframe
    :type: pandas DataFrame
    :returns: A json object with float values converted to Decimal type
    :rtype: json
    """
    return json.loads(
        json.dumps(df.to_dict(orient="records")),
        parse_float=Decimal
    )
