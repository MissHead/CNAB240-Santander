import boto3
import os
from io import BytesIO
from dotenv import load_dotenv

APP_ROOT = os.path.join(os.path.dirname(__file__), '..')
dotenv_path = os.path.join(APP_ROOT, '.env')
load_dotenv(dotenv_path)

def _(name):
    return os.getenv(name)

class Santander:
    FILE_HEADER = {}
    BATCH_HEADER = {}
    DETAILS = []
    FILE_TRAILLER = {}
    BATCH_TRAILLER = {}
    TOTAL_LINES = 0
    SECOND_LINE = 0
    SEGMENT_T1_LINE = 0
    SEGMENT_T2_LINE = 0
    SEGMENT_U1_LINE = 0
    SEGMENT_U2_LINE = 0
    FIRST_LINE = 0
    PENULTIMATE_LINE = 0

    def __init__(self):
        self.FILE_HEADER = {}
        self.BATCH_HEADER = {}
        self.DETAILS = []
        self.FILE_TRAILLER = {}
        self.BATCH_TRAILLER = {}
        self.TOTAL_LINES = 0
        self.FIRST_LINE = 0
        self.SECOND_LINE = 1
        self.PENULTIMATE_LINE = 0

    def process(self, file):
        self.__processa(file)

    def __processa(self, file):
        self.TOTAL_LINES = len(file) - 1
        self.PENULTIMATE_LINE = len(file) - 2
        for line_number, line in list(enumerate(file)):
            if line_number == self.FIRST_LINE:
                self.FILE_HEADER = self.mount_file_headers(line)
            if line_number == self.SECOND_LINE:
                self.BATCH_HEADER = self.mount_batch_headers(line)
            if line_number == self.PENULTIMATE_LINE:
                self.BATCH_TRAILLER = self.mount_batch_trailler(line)
            if line_number == self.TOTAL_LINES:
                self.FILE_TRAILLER = self.mount_file_trailler(line)
            if line[13:14] == 'T' and line[7:8] == '3':
                self.DETAILS.append(
                    self.mount_details_segment_T(line)
                )
            if line[13:14] == 'U' and line[7:8] == '3':
                self.DETAILS.append(
                    self.mount_details_segment_U(line)
                )

    def mount_file_headers(self, line):
        return {
            'bank_code_compensation': line[0:3],
            'service_batch': line[3:7],
            'registry_type': line[7:8],
            'company_registration_type': line[16:17],
            'company_registration_number': line[17:32],
            'beneficiary_agency': line[32:36],
            'beneficiary_digit': line[36:37],
            'account_number': line[37:46],
            'account_digit': line[46:47],
            'beneficiary_code': line[52:61],
            'company_name': line[72:102],
            'bank_name': line[102:132],
            'return_code': line[142:143],
            'file_date': line[143:151],
            'file_number': line[157:163],
            'layout_version': line[163:166],
        }

    def mount_batch_headers(self, line):
        return {
            'bank_code_compensation': line[0:3],
            'batch_return_number': line[3:7],
            'registry_type': line[7:8],
            'operation_type': line[8:9],
            'service_type': line[9:11],
            'layout_version': line[13:16],
            'company_registration_type': line[17:18],
            'company_registration_number': line[18:33],
            'beneficiary_code': line[33:42],
            'beneficiary_agency': line[53:57],
            'beneficiary_agency_digit': line[57:58],
            'beneficiary_account_number': line[58:67],
            'beneficiary_account_digit': line[67:68],
            'company_name': line[73:103],
            'return_number': line[183:191],
            'return_date': line[191:199],
        }

    def mount_details_segment_T(self, line: str):
        return {
            'bank_code_compensation': line[0:3],
            'batch_return_number': line[3:7],
            'registry_type': line[7:8],
            'batch_registry_number': line[8:13],
            'registry_code': line[13:14],
            'moviment_code': line[15:17],
            'beneficiary_agency': line[17:21],
            'beneficiary_agency_digit': line[21:22],
            'beneficiary_account_number': line[22:31],
            'beneficiary_account_digit': line[31:32],
            'our_number': line[40:53],
            'wallet_code': line[53:54],
            'your_number': line[54:69],
            'due_date': line[69:77],
            'amount': line[77:92],
            'bank_collector_receiver_number': line[92:95],
            'agency_collector_receiver_number': line[95:99],
            'agency_collector_receiver_digit': line[99:100],
            'company_title_identifier': line[100:125],
            'coin_code': line[125:127],
            'payer_registration_type': line[127:128],
            'payer_registration_number': line[128:143],
            'payer_name': line[143:183],
            'collection_account': line[183:193],
            'tax_and_fee': line[193:208],
            'ocurrence': line[208:218],
        }


    def mount_details_segment_U(self, line: str):
        return {
            'bank_code_compensation': line[0:3],
            'batch_service': line[3:7],
            'registry_type': line[7:8],
            'batch_registry_number': line[8:13],
            'registry_code': line[13:14],
            'moviment_code': line[15:17],
            'tax_fee_fine': line[17:32],
            'discount': line[32:47],
            'abatement': line[47:62],
            'iof': line[62:77],
            'payed_amount': line[77:92],
            'net_amount': line[92:107],
            'other_amounts': line[107:122],
            'other_credits_amounts': line[122:137],
            'creation_date': line[137:145],
            'effective_date': line[145:153],
            'payer_code': line[153:157],
            'pay_date': line[157:165],
            'pay_amount': line[165:180],
            'ocurrence': line[180:210],
            'compensation_bank_code': line[210:213],
        }

    def mount_file_trailler(self, line):
        return {
            'bank_code_compensation': line[0:3],
            'batch_return_number': line[3:7],
            'registry_type': line[7:8],
            'batch_registry_quantity': line[17:23],
            'title_quantity': line[23:29],
        }

    def mount_batch_trailler(self, line):
        return {
            'bank_code_compensation': line[0:3],
            'service_batch': line[3:7],
            'registry_type': line[7:8],
            'batch_registry_quantity': line[17:23],
            'title_quantity': line[23:29],
            'all_amount': line[29:46],
            'quantity_linked': line[46:52],
            'all_amount_linked': line[52:69],
            'quantity_engaged': line[69:75],
            'all_amount_engaged': line[75:92],
            'quantity_discount': line[92:98],
            'all_amount_discount': line[98:115],
            'warning_number': line[115:123],
        }


bucketName = _('S3_BUCKET')
s3 = boto3.client('s3')
startAfter = _('S3_PATH')
nameDirectory = []
objects = s3.list_objects_v2(Bucket=bucketName, StartAfter=startAfter)
    

for item in objects['Contents']:
    nameDirectory.append(item['Key'])
files = [ x for x in nameDirectory if '.TXT' in x ]
f = BytesIO()
for file in files:
    s3.download_fileobj(bucketName, file, f)
    f.seek(0)
    cnab240 = s3.get_object(Bucket=bucketName, Key=file)['Body'].read()
    trans = Santander()
    cnab240 = [x for x in cnab240.decode('utf-8').split('\r\n')]
    cnab240.pop()
    trans.process(cnab240)
