"""Process data file to fir into a corresponding database table."""

import os
import pandas as pd


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
        df = self.set_column_names(df=df)
        df = self.add_metadata(df=df, meta=meta_info)
        df["id"] = range(data_id, data_id + df.shape[0])
        df = df.astype(col_dtypes)

        # Update record id
        data_id += df.shape[0]

        return df.to_dict(orient="records"), data_id

    def get_processor_engine(self):
        """Returns an appropriate engine name based on a file format.

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

    def set_column_names(self, df):
        """Set column names for a database table.

        :param df: A dataframe
        :type: pandas DataFrame
        :returns: A dataframe with updated column names
        :rtype: pandas DataFrame
        """
        cols = ["lga_id", "lga_name", "new_houses", "new_other_res", "total_dwell"]
        df.columns = cols

        return df

    def parse_metadata(self):
        """Get metadata info from file name.

        :returns: Meta information extracted from a data file name
        :rtype: dict
        """
        file_parts = self.dfile.split("_")
        meta = {
            "year": file_parts[1][:4],
            "month": file_parts[1][4:6],
            "state_id": file_parts[0][-2:],
            "state_name": self.map_state[file_parts[0][-2:]],
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
