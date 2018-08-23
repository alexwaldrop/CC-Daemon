import json
import logging

def parse_qc_report(out):
    # Return QCReport parsed from a string

    # Try loading json
    try:
        qc_string = json.loads(out)
    except:
        logging.error("Unable to load QCReport! Output is not valid JSON:\n%s" % out)
        raise

    # Try loading QCReport from json
    try:
        qc_report = QCReport(report=qc_string)
    except:
        logging.error("JSON is valid but unable to parse into QCReport!")
        raise
    return qc_report


class QCReportError(Exception):
    pass


class QCReport:
    # Class ported from QCParser to hold QC data associated with samples, modules
    def __init__(self, report=None):
        # Initialize report from existing dictionary or from empty dict
        self.report     = {} if report is None else report
        self.validate()

    def get_sample_names(self):
        return self.report.keys()

    def get_colnames(self, sample=None):
        if sample is None:
            sample_names = self.get_sample_names()
            if len(sample_names) == 0:
                return []
            sample = sample_names[0]
        # Get data colnames associated with a sample
        return [x["Name"] for x in self.get_sample_data(sample)]

    def get_modules(self, sample=None):
        # Get list of modules used to produce QCReport data
        if sample is None:
            sample_names = self.get_sample_names()
            if len(sample_names) == 0:
                return []
            sample = sample_names[0]
        return [x["Module"] for x in self.get_sample_data(sample)]

    def get_sample_data(self, sample):
        if sample not in self.get_sample_names():
            logging.error("Sample '%s' not found in QCReport!")
            raise QCReportError("Cannot get data of non-existant sample!")
        return self.report[sample]

    def validate(self):
        # Determine whether QCReport is valid
        for sample_name, sample_data in self.report.iteritems():
            # Make sure every data point in every sample row contains only the required fields
            for sample_column in sample_data:
                if not "".join(sorted(sample_column.keys())) == "ModuleNameNoteSourceValue":
                    logging.error("Entry in QCReport for sample %s does not contain required columns!" % sample_name)
                    raise QCReportError("Invalid QCReport schema.")

        # Check to make sure QCReport is square
        if not self.is_square():
            # Check if
            logging.error("QCReport is not square! Not all rows have same number of columns!")
            raise QCReportError("Invalid QCReport! Not all rows have same number of columns!")

        # Check to make sure all rows in QCReport have columns in same order
        if not self.is_ordered():
            logging.error("QCReport is not ordered! Data columns are not same for every sample or are not in same order!")
            raise QCReportError("Invalid QCReport! Data columns are not same for every sample or are not in same order!")

    def is_square(self):
        # Return True if all rows have same number of columns
        row_len = -1
        for sample in self.get_sample_names():
            if row_len == -1:
                row_len = len(self.get_colnames(sample))
            else:
                if len(self.get_colnames(sample)) != row_len:
                    return False
        return True

    def is_ordered(self):
        # Return True if columns in every row are in same order
        row_order = ""
        for sample in self.get_sample_names():
            if row_order == "":
                row_order = "_".join(self.get_colnames(sample))
            elif "_".join(self.get_colnames(sample)) != row_order:
                return False
        return True