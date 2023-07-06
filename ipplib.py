import struct
import base64
import http.client
import getpass
import copy
import os
import ssl
from enum import IntEnum
from tempfile import NamedTemporaryFile

IPP_PROTO_VERSION = (2, 0)
IPP_CHARSET_LANGUAGE = 'en-US'
IPP_CHARSET = 'utf-8'

IPP_DEFAULT_PRINTER_ATTRIBUTES = ['printer-name', 'printer-type', 'printer-location', 'printer-info',
                                  'printer-make-and-model', 'printer-state', 'printer-state-message',
                                  'printer-state-reason', 'printer-uri-supported', 'device-uri', 'printer-is-shared']
IPP_DEFAULT_CLASS_ATTRIBUTES = ['printer-name', 'member-names']
IPP_DEFAULT_JOB_ATTRIBUTES = ["job-id", "job-name", 'printer-uri', "job-state", "job-state-reasons", "job-hold-until",
                              "job-media-progress", "job-k-octets", "number-of-documents", "copies",
                              'job-originating-user-name']


class IppStatus(IntEnum):
    CUPS_INVALID = -1
    OK = 0x0000
    OK_IGNORED_OR_SUBSTITUTED = 0x0001
    OK_CONFLICTING = 0x0002
    OK_IGNORED_SUBSCRIPTIONS = 0x0003
    OK_IGNORED_NOTIFICATIONS = 0x0004
    OK_TOO_MANY_EVENTS = 0x0005
    OK_BUT_CANCEL_SUBSCRIPTION = 0x0006
    OK_EVENTS_COMPLETE = 0x0007
    REDIRECTION_OTHER_SITE = 0x0200
    CUPS_SEE_OTHER = 0x0280
    ERROR_BAD_REQUEST = 0x0400
    ERROR_FORBIDDEN = 0x0401
    ERROR_NOT_AUTHENTICATED = 0x0402
    ERROR_NOT_AUTHORIZED = 0x0403
    ERROR_NOT_POSSIBLE = 0x0404
    ERROR_TIMEOUT = 0x0405
    ERROR_NOT_FOUND = 0x0406
    ERROR_GONE = 0x0407
    ERROR_REQUEST_ENTITY = 0x0408
    ERROR_REQUEST_VALUE = 0x0409
    ERROR_DOCUMENT_FORMAT_NOT_SUPPORTED = 0x040a
    ERROR_ATTRIBUTES_OR_VALUES = 0x040b
    ERROR_URI_SCHEME = 0x040c
    ERROR_CHARSET = 0x040d
    ERROR_CONFLICTING = 0x040e
    ERROR_COMPRESSION_ERROR = 0x040f
    ERROR_DOCUMENT_FORMAT_ERROR = 0x0410
    ERROR_DOCUMENT_ACCESS = 0x0411
    ERROR_ATTRIBUTES_NOT_SETTABLE = 0x0412
    ERROR_IGNORED_ALL_SUBSCRIPTIONS = 0x0413
    ERROR_TOO_MANY_SUBSCRIPTIONS = 0x0414
    ERROR_IGNORED_ALL_NOTIFICATIONS = 0x0415
    ERROR_PRINT_SUPPORT_FILE_NOT_FOUND = 0x0416
    ERROR_DOCUMENT_PASSWORD = 0x0417
    ERROR_DOCUMENT_PERMISSION = 0x0418
    ERROR_DOCUMENT_SECURITY = 0x0419
    ERROR_DOCUMENT_UNPRINTABLE = 0x041a
    ERROR_ACCOUNT_INFO_NEEDED = 0x041b
    ERROR_ACCOUNT_CLOSED = 0x041c
    ERROR_ACCOUNT_LIMIT_REACHED = 0x041d
    ERROR_ACCOUNT_AUTHORIZATION_FAILED = 0x041e
    ERROR_NOT_FETCHABLE = 0x041f
    ERROR_CUPS_ACCOUNT_INFO_NEEDED = 0x049C
    ERROR_CUPS_ACCOUNT_CLOSED = 0x049d
    ERROR_CUPS_ACCOUNT_LIMIT_REACHED = 0x049e
    ERROR_CUPS_ACCOUNT_AUTHORIZATION_FAILED = 0x049f
    ERROR_INTERNAL = 0x0500
    ERROR_OPERATION_NOT_SUPPORTED = 0x0501
    ERROR_SERVICE_UNAVAILABLE = 0x0502
    ERROR_VERSION_NOT_SUPPORTED = 0x0503
    ERROR_DEVICE = 0x0504
    ERROR_TEMPORARY = 0x0505
    ERROR_NOT_ACCEPTING_JOBS = 0x0506
    ERROR_BUSY = 0x0507
    ERROR_JOB_CANCELED = 0x0508
    ERROR_MULTIPLE_JOBS_NOT_SUPPORTED = 0x0509
    ERROR_PRINTER_IS_DEACTIVATED = 0x050a
    ERROR_TOO_MANY_JOBS = 0x050b
    ERROR_TOO_MANY_DOCUMENTS = 0x050c
    ERROR_CUPS_AUTHENTICATION_CANCELED = 0x1000
    ERROR_CUPS_PKI = 0x1001
    ERROR_CUPS_UPGRADE_REQUIRED = 0x1002


class IppOperation(IntEnum):
    CUPS_INVALID = -0x0001
    CUPS_NONE = 0x0000
    PRINT_JOB = 0x0002
    PRINT_URI = 0x0003
    VALIDATE_JOB = 0x0004
    CREATE_JOB = 0x0005
    SEND_DOCUMENT = 0x0006
    SEND_URI = 0x0007
    CANCEL_JOB = 0x0008
    GET_JOB_ATTRIBUTES = 0x0009
    GET_JOBS = 0x000a
    GET_PRINTER_ATTRIBUTES = 0x000b
    HOLD_JOB = 0x000c
    RELEASE_JOB = 0x000d
    RESTART_JOB = 0x000e
    PAUSE_PRINTER = 0x0010
    RESUME_PRINTER = 0x0011
    PURGE_JOBS = 0x0012
    SET_PRINTER_ATTRIBUTES = 0x0013
    SET_JOB_ATTRIBUTES = 0x0014
    GET_PRINTER_SUPPORTED_VALUES = 0x0015
    CREATE_PRINTER_SUBSCRIPTIONS = 0x0016
    CREATE_JOB_SUBSCRIPTIONS = 0x0017
    GET_SUBSCRIPTION_ATTRIBUTES = 0x0018
    GET_SUBSCRIPTIONS = 0x0019
    RENEW_SUBSCRIPTION = 0x001a
    CANCEL_SUBSCRIPTION = 0x001b
    GET_NOTIFICATIONS = 0x001c
    SEND_NOTIFICATIONS = 0x001d
    GET_RESOURCE_ATTRIBUTES = 0x001e
    GET_RESOURCE_DATA = 0x001f
    GET_RESOURCES = 0x0020
    GET_PRINT_SUPPORT_FILES = 0x0021
    ENABLE_PRINTER = 0x0022
    DISABLE_PRINTER = 0x0023
    PAUSE_PRINTER_AFTER_CURRENT_JOB = 0x0024
    HOLD_NEW_JOBS = 0x0025
    RELEASE_HELD_NEW_JOBS = 0x0026
    DEACTIVATE_PRINTER = 0x0027
    ACTIVATE_PRINTER = 0x0028
    RESTART_PRINTER = 0x0029
    SHUTDOWN_PRINTER = 0x002a
    STARTUP_PRINTER = 0x002b
    REPROCESS_JOB = 0x002c
    CANCEL_CURRENT_JOB = 0x002d
    SUSPEND_CURRENT_JOB = 0x002e
    RESUME_JOB = 0x002f
    PROMOTE_JOB = 0x0030
    SCHEDULE_JOB_AFTER = 0x0031
    CANCEL_DOCUMENT = 0x0033
    GET_DOCUMENT_ATTRIBUTES = 0x0034
    GET_DOCUMENTS = 0x0035
    DELETE_DOCUMENT = 0x0036
    SET_DOCUMENT_ATTRIBUTES = 0x0037
    CANCEL_JOBS = 0x0038
    CANCEL_MY_JOBS = 0x0039
    RESUBMIT_JOB = 0x003a
    CLOSE_JOB = 0x003b
    IDENTIFY_PRINTER = 0x003c
    VALIDATE_DOCUMENT = 0x003d
    ADD_DOCUMENT_IMAGES = 0x003e
    ACKNOWLEDGE_DOCUMENT = 0x003f
    ACKNOWLEDGE_IDENTIFY_PRINTER = 0x0040
    ACKNOWLEDGE_JOB = 0x0041
    FETCH_DOCUMENT = 0x0042
    FETCH_JOB = 0x0043
    GET_OUTPUT_DEVICE_ATTRIBUTES = 0x0044
    UPDATE_ACTIVE_JOBS = 0x0045
    DEREGISTER_OUTPUT_DEVICE = 0x0046
    UPDATE_DOCUMENT_STATUS = 0x0047
    UPDATE_JOB_STATUS = 0x0048
    UPDATE_OUTPUT_DEVICE_ATTRIBUTES = 0x0049
    GET_NEXT_DOCUMENT_DATA = 0x004a
    ALLOCATE_PRINTER_RESOURCES = 0x004b
    CREATE_PRINTER = 0x004c
    DEALLOCATE_PRINTER_RESOURCES = 0x004d
    DELETE_PRINTER = 0x004e
    GET_PRINTERS = 0x004f
    SHUTDOWN_ONE_PRINTER = 0x0050
    STARTUP_ONE_PRINTER = 0x0051
    CANCEL_RESOURCE = 0x0052
    CREATE_RESOURCE = 0x0053
    INSTALL_RESOURCE = 0x0054
    SEND_RESOURCE_DATA = 0x0055
    SET_RESOURCE_ATTRIBUTES = 0x0056
    CREATE_RESOURCE_SUBSCRIPTIONS = 0x0057
    CREATE_SYSTEM_SUBSCRIPTIONS = 0x0058
    DISABLE_ALL_PRINTERS = 0x0059
    ENABLE_ALL_PRINTERS = 0x005a
    GET_SYSTEM_ATTRIBUTES = 0x005b
    GET_SYSTEM_SUPPORTED_VALUES = 0x005c
    PAUSE_ALL_PRINTERS = 0x005d
    PAUSE_ALL_PRINTERS_AFTER_CURRENT_JOB = 0x005e
    REGISTER_OUTPUT_DEVICE = 0x005f
    RESTART_SYSTEM = 0x0060
    RESUME_ALL_PRINTERS = 0x0061
    SET_SYSTEM_ATTRIBUTES = 0x0062
    SHUTDOWN_ALL_PRINTER = 0x0063
    STARTUP_ALL_PRINTERS = 0x0064
    PRIVATE = 0x4000
    CUPS_GET_DEFAULT = 0x4001
    CUPS_GET_PRINTERS = 0x4002
    CUPS_ADD_MODIFY_PRINTER = 0x4003
    CUPS_DELETE_PRINTER = 0x4004
    CUPS_GET_CLASSES = 0x4005
    CUPS_ADD_MODIFY_CLASS = 0x4006
    CUPS_DELETE_CLASS = 0x4007
    CUPS_ACCEPT_JOBS = 0x4008
    CUPS_REJECT_JOBS = 0x4009
    CUPS_SET_DEFAULT = 0x400a
    CUPS_GET_DEVICES = 0x400b
    CUPS_GET_PPDS = 0x400c
    CUPS_MOVE_JOB = 0x400d
    CUPS_AUTHENTICATE_JOB = 0x400e
    CUPS_GET_PPD = 0x400f
    CUPS_GET_DOCUMENT = 0x4027
    CUPS_CREATE_LOCAL_PRINTER = 0x4028


class IppTag(IntEnum):
    CUPS_INVALID = -1
    ZERO = 0x00
    OPERATION = 0x01
    JOB = 0x02
    END = 0x03
    PRINTER = 0x04
    UNSUPPORTED_GROUP = 0x05
    SUBSCRIPTION = 0x06
    EVENT_NOTIFICATION = 0x07
    RESOURCE = 0x08
    DOCUMENT = 0x09
    SYSTEM = 0x0a
    UNSUPPORTED_VALUE = 0x10
    DEFAULT = 0x11
    UNKNOWN = 0x12
    NO_VALUE = 0x013
    NOT_SETTABLE = 0x15
    DELETE_ATTR = 0x16
    ADMIN_DEFINE = 0x17
    INTEGER = 0x21
    BOOLEAN = 0x22
    ENUM = 0x23
    STRING = 0x30
    DATE = 0x31
    RESOLUTION = 0x32
    RANGE = 0x33
    BEGIN_COLLECTION = 0x34
    TEXT_LANG = 0x35
    NAME_LANG = 0x36
    END_COLLECTION = 0x37
    TEXT = 0x41
    NAME = 0x42
    RESERVED_STRING = 0x43
    KEYWORD = 0x44
    URI = 0x45
    URI_SCHEME = 0x46
    CHARSET = 0x47
    LANGUAGE = 0x48
    MIME_TYPE = 0x49
    MEMBER_NAME = 0x4a
    EXTENSION = 0x7f
    CUPS_MASK = 0x7fffffff
    CUPS_CONST = -0x7fffffff - 1


class IppJobState(IntEnum):
    PENDING = 0x03
    HELD = 0x04
    PROCESSING = 0x05
    STOPPED = 0x06
    CANCELED = 0x07
    ABORTED = 0x08
    COMPLETED = 0x09


class IppDocumentState(IntEnum):
    PENDING = 0x03
    PROCESSING = 0x05
    CANCELED = 0x07
    ABORTED = 0x08
    COMPLETED = 0x08


class IppPrinterState(IntEnum):
    IDLE = 0x0003
    PROCESSING = 0x0004
    STOPPED = 0x0005


_IPP_ATTRIBUTE_TAG_MAP = {
    'attributes-charset': IppTag.CHARSET,
    'attributes-natural-language': IppTag.LANGUAGE,
    'printer-uri': IppTag.URI,
    'requesting-user-name': IppTag.NAME,
    'job-id': IppTag.INTEGER,
    'document-name': IppTag.NAME,
    'job-name': IppTag.NAME,
    'document-format': IppTag.MIME_TYPE,
    'last-document': IppTag.BOOLEAN,
    'copies': IppTag.INTEGER,
    'job-hold-until': IppTag.KEYWORD,
    'job-priority': IppTag.INTEGER,
    'number-up': IppTag.INTEGER,
    'job-sheets': IppTag.NAME,
    'job-uri': IppTag.URI,
    'job-state': IppTag.ENUM,
    'job-state-reason': IppTag.KEYWORD,
    'requested-attributes': IppTag.KEYWORD,
    'member-uris': IppTag.URI,
    'ppd-name': IppTag.NAME,
    'printer-state-reason': IppTag.KEYWORD,
    'printer-is-shared': IppTag.BOOLEAN,
    'printer-error-policy': IppTag.NAME,
    'printer-info': IppTag.TEXT,
    'which-jobs': IppTag.KEYWORD,
    'my-jobs': IppTag.BOOLEAN,
    'purge-jobs': IppTag.BOOLEAN,
    'hold-job-until': IppTag.KEYWORD,
    'job-printer-uri': IppTag.URI,
    'printer-location': IppTag.TEXT,
    'document-number': IppTag.INTEGER,
    'printer-state': IppTag.ENUM,
    'document-state': IppTag.ENUM,
    'device-uri': IppTag.URI
}


class IppTransportException(Exception):
    pass


class IppException(Exception):
    def __init__(self, message, code):
        self.message = message
        self.code = code

    def __str__(self):
        return '{0} - {1}'.format(self.code, self.message)


def update_attribute_tag_map(attribute: str, tag: IppTag):
    _IPP_ATTRIBUTE_TAG_MAP[attribute] = tag


def __construct_attibute_values(tag: IppTag, value):
    bs = b''

    if tag in (IppTag.INTEGER, IppTag.ENUM):
        bs += struct.pack('>h', 4)
        bs += struct.pack('>i', value)
    elif tag == IppTag.BOOLEAN:
        bs += struct.pack('>h', 1)
        bs += struct.pack('>?', value)
    else:
        bs += struct.pack('>h', len(value))
        bs += value.encode('utf-8')

    return bs


def construct_attribute(name: str, value, tag=None):
    bs = b''

    if not tag:
        tag = _IPP_ATTRIBUTE_TAG_MAP.get(name, None)

    if not tag:
        return bs

    if isinstance(value, list) or isinstance(value, tuple) or isinstance(value, set):
        for index, v in enumerate(value):
            bs += struct.pack('>b', tag.value)

            if index == 0:
                bs += struct.pack('>h', len(name))
                bs += name.encode('utf-8')
            else:
                bs += struct.pack('>h', 0)

            bs += __construct_attibute_values(tag, v)
    else:
        bs = struct.pack('>b', tag.value)

        bs += struct.pack('>h', len(name))
        bs += name.encode('utf-8')

        bs += __construct_attibute_values(tag, value)

    return bs


def parse_response_without_check(ipp_raw_data: bytes, contains_data=False):
    """
    1 byte: Protocol Major Version - b
    1 byte: Protocol Minor Version - b
    2 byte: Operation ID - h
    4 byte: Request ID - i

    1 byte: Operation Attribute Byte (\0x01)

    N Mal: Attributes

    1 byte: Attribute End Byte (\0x03)
    """

    data = {}

    offset = 0

    data['version'] = struct.unpack_from('>bb', ipp_raw_data, offset)
    offset += 2

    data['status-code'] = struct.unpack_from('>h', ipp_raw_data, offset)[0]
    offset += 2

    data['request-id'] = struct.unpack_from('>i', ipp_raw_data, offset)[0]
    offset += 4

    data['operation-attributes'] = []
    data['jobs'] = []
    data['printers'] = []
    data['data'] = b''

    attribute_key = ''
    previous_attribute_name = ''
    tmp_data = {}

    while struct.unpack_from('b', ipp_raw_data, offset)[0] != IppTag.END.value:
        # check for operation, job or printer attribute start byte
        # if tmp data and attribute key is set, a another operation was send -> add it and reset tmp data
        if struct.unpack_from('b', ipp_raw_data, offset)[0] == IppTag.OPERATION.value:
            if tmp_data and attribute_key:
                data[attribute_key].append(tmp_data)
                tmp_data = {}

            attribute_key = 'operation-attributes'
            offset += 1
        elif struct.unpack_from('b', ipp_raw_data, offset)[0] == IppTag.JOB.value:
            if tmp_data and attribute_key:
                data[attribute_key].append(tmp_data)
                tmp_data = {}

            attribute_key = 'jobs'
            offset += 1
        elif struct.unpack_from('b', ipp_raw_data, offset)[0] == IppTag.PRINTER.value:
            if tmp_data and attribute_key:
                data[attribute_key].append(tmp_data)
                tmp_data = {}

            attribute_key = 'printers'
            offset += 1

        attribute, new_offset = parse_attribute(ipp_raw_data, offset)

        # print(attribute)

        # if attribute have a name -> add it
        # if attribute doesn't habe a name -> it is part of an array
        if attribute['name']:
            tmp_data[attribute['name']] = attribute['value']
            previous_attribute_name = attribute['name']
        elif previous_attribute_name:
            # check if attribute is already an array
            # else convert is to an array
            if isinstance(tmp_data[previous_attribute_name], list):
                tmp_data[previous_attribute_name].append(attribute['value'])
            else:
                tmp_value = tmp_data[previous_attribute_name]
                tmp_data[previous_attribute_name] = [tmp_value, attribute['value']]

        offset = new_offset

    data[attribute_key].append(tmp_data)

    if data['operation-attributes']:
        data['operation-attributes'] = data['operation-attributes'][0]

    if contains_data:
        data['data'] = ipp_raw_data[offset + 1:]

    return data


def parse_response(ipp_raw_data: bytes, contains_data=False):
    data = parse_response_without_check(ipp_raw_data, contains_data)

    _check_response_for_errors(data)

    return data


def parse_attribute(data: bytes, offset: int):
    """
    1 byte: Tag - b
    2 byte: Name Length - h
    N bytes: Name - direct access
    2 byte: Value Length -h
    N bytes: Value - direct access
    """

    attribute = {
        'tag': struct.unpack_from('>b', data, offset)[0]
    }

    offset += 1

    attribute['name-length'] = struct.unpack_from('>h', data, offset)[0]
    offset += 2

    attribute['name'] = data[offset:offset + attribute['name-length']].decode('utf-8')
    offset += attribute['name-length']

    attribute['value-length'] = struct.unpack_from('>h', data, offset)[0]
    offset += 2

    if attribute['tag'] in (IppTag.ENUM.value, IppTag.INTEGER.value):
        attribute['value'] = struct.unpack_from('>i', data, offset)[0]

        if attribute['tag'] == IppTag.ENUM.value:
            if attribute['name'] == 'job-state':
                attribute['value'] = IppJobState(attribute['value'])
            elif attribute['name'] == 'printer-state':
                attribute['value'] = IppPrinterState(attribute['value'])
            elif attribute['name'] == 'document-state':
                attribute['value'] = IppDocumentState(attribute['value'])

        offset += 4
    elif attribute['tag'] == IppTag.BOOLEAN.value:
        attribute['value'] = struct.unpack_from('>?', data, offset)[0]
        offset += 1
    elif attribute['tag'] == IppTag.DATE.value:
        attribute['value'] = struct.unpack_from('>' + 'b' * attribute['value-length'], data, offset)[0]
        offset += attribute['value-length']
    elif attribute['tag'] == IppTag.RESERVED_STRING.value:
        if attribute['value-length'] > 0:
            attribute['value'] = data[offset:offset + attribute['value-length']].decode('utf-8')
            offset += attribute['value-length']
        else:
            attribute['value'] = None
    elif attribute['tag'] == IppTag.RANGE.value:
        attribute['value'] = []
        for i in range(int(attribute['value-length'] / 4)):
            attribute['value'].append(struct.unpack_from('>i', data, offset + i * 4)[0])
        offset += attribute['value-length']
    elif attribute['tag'] == IppTag.RESOLUTION.value:
        attribute['value'] = struct.unpack_from('>iib', data, offset)
        offset += attribute['value-length']
    else:
        attribute['value'] = data[offset:offset + attribute['value-length']].decode('utf-8')
        offset += attribute['value-length']

    return attribute, offset


def _check_response_for_errors(response):
    if response['status-code'] != 0:
        raise IppException(response['operation-attributes']['status-message'], response['status-code'])


def construct_request(operation: IppOperation, request_id: int, operation_attributes=None, job_attributes=None,
                      printer_attributes=None):
    data = struct.pack('>bb', *IPP_PROTO_VERSION)
    data += struct.pack('>h', operation.value)
    data += struct.pack('>i', request_id)

    data += struct.pack('>b', IppTag.OPERATION.value)

    data += construct_attribute('attributes-charset', IPP_CHARSET)
    data += construct_attribute('attributes-natural-language', IPP_CHARSET_LANGUAGE)

    if isinstance(operation_attributes, dict):
        for attr, value in operation_attributes.items():
            data += construct_attribute(attr, value)

    if isinstance(job_attributes, dict):
        data += struct.pack('>b', IppTag.JOB.value)

        for attr, value in job_attributes.items():
            data += construct_attribute(attr, value)

    if isinstance(printer_attributes, dict):
        data += struct.pack('>b', IppTag.PRINTER.value)

        for attr, value in printer_attributes.items():
            data += construct_attribute(attr, value)

    data += struct.pack('>b', IppTag.END.value)

    return data


class IppClient:
    def __init__(self, host, port=631, user=None, password=None, use_ssl=True, verify_certificate=True):
        self.__host = host
        self.__port = port
        self.__user = user if user else getpass.getuser()

        if use_ssl:
            self._connection = http.client.HTTPSConnection(
                self.__host, port=self.__port,
                context=ssl._create_unverified_context() if not verify_certificate else None)
        else:
            self._connection = http.client.HTTPConnection(self.__host, port=self.__port)

        self.__headers = {
            'Content-Type': 'application/ipp'
        }

        if user and password:
            self.__headers['Authorization'] = "Basic {0}".format(
                base64.b64encode('{0}:{1}'.format(user, password).encode('utf-8')).decode('utf-8'))

        if not self.test_connection():
            raise IppTransportException('Could not connect to IPP Server')

    def __del__(self):
        self._connection.close()

    @property
    def host(self):
        return self.__host

    @property
    def port(self):
        return self.__port

    @property
    def user(self):
        return self.__user

    @user.setter
    def user(self, user):
        self.__user = user

    def _construct_headers(self, data, expect_continue=False):
        headers = copy.deepcopy(self.__headers)
        headers['Content-Length'] = len(data)
        if expect_continue:
            headers['Expect'] = '100-continue'

        return headers

    def _construct_uri(self, namespace: str, ipp_object: str):
        return "http://{0}:{1}/{2}/{3}".format(self.__host, self.__port, namespace, ipp_object)

    def _get_response_data(self):
        response = self._connection.getresponse()
        if response.getcode() == 200:
            return response.read()
        else:
            raise IppTransportException('Error: {0}'.format(response.getcode()))

    def send_request(self, uri: str, operation: IppOperation, request_id: int, operation_attributes=None,
                     job_attributes=None, printer_attributes=None):

        data = construct_request(operation, request_id, operation_attributes, job_attributes, printer_attributes)

        self._connection.request('POST', "http://{0}:{1}/{2}".format(self.__host, self.__port, uri),
                                 headers=self._construct_headers(data), body=data)

        return parse_response(self._get_response_data())

    def send_raw_request(self, uri: str, raw_request: bytes):
        self._connection.request('POST', "http://{0}:{1}/{2}".format(self.__host, self.__port, uri),
                                 headers=self._construct_headers(raw_request), body=raw_request)

        return parse_response(self._get_response_data())

    def print_file(self, printer: str, file_path: str, job_name=None, copies=1, priority=50):
        if not os.path.exists(file_path):
            return None

        printer_uri = 'ipp://localhost/printers/{0}'.format(printer)

        if not job_name:
            job_name = os.path.basename(file_path)

        # construct request body part 1
        # create job
        data = construct_request(IppOperation.CREATE_JOB, 1, operation_attributes={
            'printer-uri': printer_uri,
            'requesting-user-name': self.__user,
            'job-name': job_name
        }, job_attributes={
            'copies': copies,
            'job-priority': priority
        })

        self._connection.request('POST', self._construct_uri('printers', printer),
                                 headers=self._construct_headers(data, True), body=data)

        response_data = parse_response(self._get_response_data())

        job_id = response_data['jobs'][0]['job-id']

        # construct request body part 2
        # create document
        data = construct_request(IppOperation.SEND_DOCUMENT, 2, operation_attributes={
            'printer-uri': printer_uri,
            'requesting-user-name': self.__user,
            'job-id': job_id,
            'document-name': job_name,
            'document-format': 'application/octet-stream',
            'last-document': True
        })

        header = self._construct_headers(data)
        header['Content-Length'] += os.path.getsize(file_path)

        # with open(file_path, 'rb') as doc_obj:
        #     data += doc_obj.read()
        #
        # self._connection.request('POST', self._construct_uri('printers', printer),
        #                          headers=self._construct_headers(data), body=data)

        # send custom request
        # send file in chunks
        self._connection.putrequest('POST', self._construct_uri('printers', printer))
        for header_name, header_value in header.items():
            self._connection.putheader(header_name, header_value)
        self._connection.endheaders()

        self._connection.send(data)
        with open(file_path, 'rb') as doc_obj:
            self._connection.send(doc_obj)

        parse_response(self._get_response_data())

        return job_id

    def get_printer_attributes(self, printer: str, attributes=None):
        printer_uri = 'ipp://localhost/printers/{0}'.format(printer)

        # construct request body part 1
        # create job

        data = construct_request(IppOperation.GET_PRINTER_ATTRIBUTES, 1, {
            'printer-uri': printer_uri,
            'requesting-user-name': self.__user,
            'requested-attributes': attributes if attributes else IPP_DEFAULT_PRINTER_ATTRIBUTES
        })

        self._connection.request('POST', self._construct_uri('printers', printer),
                                 headers=self._construct_headers(data), body=data)

        response_data = parse_response(self._get_response_data())

        return response_data['printers'][0]

    def resume_printer(self, printer: str):
        printer_uri = 'ipp://localhost/printers/{0}'.format(printer)

        data = construct_request(IppOperation.RESUME_PRINTER, 1, {
            'printer-uri': printer_uri
        })

        self._connection.request('POST', self._construct_uri('admin', ''),
                                 headers=self._construct_headers(data), body=data)
        parse_response(self._get_response_data())

        return True

    def pause_printer(self, printer: str):
        printer_uri = 'ipp://localhost/printers/{0}'.format(printer)

        data = construct_request(IppOperation.PAUSE_PRINTER, 1, {
            'printer-uri': printer_uri
        })

        self._connection.request('POST', self._construct_uri('admin', ''),
                                 headers=self._construct_headers(data), body=data)
        parse_response(self._get_response_data())

        return True

    def get_jobs(self, printer=None, which_jobs='not-completed', my_jobs=False, attributes=None):
        data = construct_request(IppOperation.GET_JOBS, 1, {
            'printer-uri': 'ipp://localhost/printers/{0}'.format(printer if printer else ''),
            'which-jobs': which_jobs,
            'my-jobs': my_jobs,
            'requested-attributes': attributes + ['job-id'] if attributes else IPP_DEFAULT_JOB_ATTRIBUTES
        })

        self._connection.request('POST', self._construct_uri('', ''),
                                 headers=self._construct_headers(data), body=data)
        response_data = parse_response(self._get_response_data())

        return {j['job-id']: j for j in response_data['jobs']}

    def get_job_attributes(self, job_id: int, attributes=None):
        data = construct_request(IppOperation.GET_JOB_ATTRIBUTES, 1, {
            'job-uri': 'ipp://localhost/jobs/{0}'.format(job_id),
            'requested-attributes': attributes if attributes else IPP_DEFAULT_JOB_ATTRIBUTES
        })

        self._connection.request('POST', self._construct_uri('', ''),
                                 headers=self._construct_headers(data), body=data)
        response_data = parse_response(self._get_response_data())

        return response_data['jobs'][0]

    def cancel_job(self, job_id: int):
        data = construct_request(IppOperation.CANCEL_JOB, 1, {
            'job-uri': 'ipp://localhost/jobs/{0}'.format(job_id),
            'requesting-user-name': self.__user
        })

        self._connection.request('POST', self._construct_uri('jobs', ''),
                                 headers=self._construct_headers(data), body=data)
        parse_response(self._get_response_data())

        return True

    def cancel_all_jobs(self, printer: str):
        data = construct_request(IppOperation.CANCEL_JOB, 1, {
            'printer-uri': 'ipp://localhost/printers/{0}'.format(printer),
            'purge-jobs': True
        })

        self._connection.request('POST', self._construct_uri('admin', ''),
                                 headers=self._construct_headers(data), body=data)
        parse_response(self._get_response_data())

        return True

    def restart_job(self, job_id: int):
        data = construct_request(IppOperation.RESTART_JOB, 1, {
            'job-uri': 'ipp://localhost/jobs/{0}'.format(job_id),
            'requesting-user-name': self.__user
        })

        self._connection.request('POST', self._construct_uri('jobs', ''),
                                 headers=self._construct_headers(data), body=data)
        parse_response(self._get_response_data())

        return True

    def set_job_hold_until(self, job_id: int, hold_until: str):
        # hold_until: indefinite, no-hold

        data = construct_request(IppOperation.RESTART_JOB, 1, {
            'job-uri': 'ipp://localhost/jobs/{0}'.format(job_id),
            'requesting-user-name': self.__user
        }, job_attributes={
            'job-hold-until': hold_until
        })

        self._connection.request('POST', self._construct_uri('jobs', ''),
                                 headers=self._construct_headers(data), body=data)
        parse_response(self._get_response_data())

        return True

    def print_test_page(self, printer):
        printer_uri = 'ipp://localhost/printers/{0}'.format(printer)

        # construct request body part 1
        # create job
        data = construct_request(IppOperation.CREATE_JOB, 1, operation_attributes={
            'printer-uri': printer_uri,
            'requesting-user-name': self.__user,
            'job-name': 'Test Print'
        })

        self._connection.request('POST', self._construct_uri('printers', printer),
                                 headers=self._construct_headers(data, True), body=data)

        response_data = parse_response(self._get_response_data())

        job_id = response_data['jobs'][0]['job-id']

        # construct request body part 2
        # create document
        data = construct_request(IppOperation.SEND_DOCUMENT, 2, operation_attributes={
            'printer-uri': printer_uri,
            'requesting-user-name': self.__user,
            'job-id': job_id,
            'document-name': 'Test Print',
            'document-format': 'application/postscript',
            'last-document': True
        })

        data += bytes('#PDF-BANNER\n' +
                      'Template default-testpage.pdf\n' +
                      'Show printer-name printer-info printer-location printer-make-and-model printer-driver-name' +
                      'printer-driver-version paper-size imageable-area job-id options time-at-creation' +
                      'time-at-processing\n\n', 'utf-8')

        self._connection.request('POST', self._construct_uri('printers', printer),
                                 headers=self._construct_headers(data), body=data)
        parse_response(self._get_response_data())

        return job_id

    def test_connection(self):
        try:
            self._connection.connect()
            self._connection.close()

            return True
        except ConnectionRefusedError:
            return False


class CupsClient(IppClient):
    def __init__(self, host, port=631, user=None, password=None, use_ssl=True, verify_certificate=True):
        super().__init__(host, port, user, password, use_ssl, verify_certificate)

    def get_devices(self):
        data = construct_request(IppOperation.CUPS_GET_DEVICES, 1)

        self._connection.request('POST', self._construct_uri('', ''),
                                 headers=self._construct_headers(data), body=data)
        response_data = parse_response(self._get_response_data())

        return {p['device-uri']: p for p in response_data['printers']}

    def get_document(self, printer: str, job_id: int, document_id: int):
        data = construct_request(IppOperation.CUPS_GET_DOCUMENT, 1, operation_attributes={
            'printer-uri': 'ipp://localhost/printers/{0}'.format(printer),
            'job-id': job_id,
            'document-number': document_id
        })

        self._connection.request('POST', self._construct_uri('', ''),
                                 headers=self._construct_headers(data), body=data)
        response_data = parse_response(self._get_response_data(), True)

        if response_data['data']:
            with NamedTemporaryFile(delete=False) as tmp_file:
                tmp_file_name = tmp_file.name

                tmp_file.write(response_data['data'])

            return tmp_file_name

        return None

    def move_job(self, job_id: int, destination_printer: str):
        data = construct_request(IppOperation.CUPS_MOVE_JOB, 1, {
            'job-uri': 'ipp://localhost/jobs/{0}'.format(job_id),
            'requesting-user-name': self.__user
        }, job_attributes={
            'job-printer-uri': 'ipp://localhost/printers/{0}'.format(destination_printer),
        })

        self._connection.request('POST', self._construct_uri('jobs', ''),
                                 headers=self._construct_headers(data), body=data)
        parse_response(self._get_response_data())

        return True

    def move_all_jobs(self, source_printer: str, destination_printer: str):
        data = construct_request(IppOperation.CUPS_MOVE_JOB, 1, {
            'printer-uri': 'ipp://localhost/printers/{0}'.format(source_printer),
            'requesting-user-name': self.__user
        }, job_attributes={
            'job-printer-uri': 'ipp://localhost/printers/{0}'.format(destination_printer),
        })

        self._connection.request('POST', self._construct_uri('jobs', ''),
                                 headers=self._construct_headers(data), body=data)
        parse_response(self._get_response_data())

        return True

    def get_ppds(self):
        data = construct_request(IppOperation.CUPS_GET_PPDS, 1)

        self._connection.request('POST', self._construct_uri('', ''),
                                 headers=self._construct_headers(data), body=data)
        response_data = parse_response(self._get_response_data())

        return {p['ppd-name']: p for p in response_data['printers']}

    def accept_jobs(self, printer: str):
        printer_uri = 'ipp://localhost/printers/{0}'.format(printer)

        data = construct_request(IppOperation.CUPS_ACCEPT_JOBS, 1, {
            'printer-uri': printer_uri
        })

        self._connection.request('POST', self._construct_uri('admin', ''),
                                 headers=self._construct_headers(data), body=data)
        parse_response(self._get_response_data())

        return True

    def reject_jobs(self, printer: str):
        printer_uri = 'ipp://localhost/printers/{0}'.format(printer)

        data = construct_request(IppOperation.CUPS_REJECT_JOBS, 1, {
            'printer-uri': printer_uri
        })

        self._connection.request('POST', self._construct_uri('admin', ''),
                                 headers=self._construct_headers(data), body=data)
        parse_response(self._get_response_data())

        return True

    def add_printer_to_class(self, clazz: str, printer: str):
        class_uri = 'ipp://localhost/classes/{0}'.format(clazz)
        printer_uri = 'ipp://localhost/printers/{0}'.format(printer)

        try:
            # get current class members
            members = self.get_printer_attributes(clazz, ['member-uris'])['member-uris']

            # if class already exists, add printer to members
            # return True if printer is aleady a member
            if isinstance(members, list):
                if printer_uri in members:
                    return True

                members.append(printer_uri)
            else:
                if members == printer_uri:
                    return True

                members = [members, printer_uri]
        except IppException:
            # if class doesn't exists, set members to printer rui
            members = printer_uri

        data = construct_request(IppOperation.CUPS_ADD_MODIFY_CLASS, 1, operation_attributes={
            'printer-uri': class_uri
        }, printer_attributes={
            'member-uris': members
        })

        self._connection.request('POST', self._construct_uri('admin', ''),
                                 headers=self._construct_headers(data), body=data)
        parse_response(self._get_response_data())

        return True

    def delete_printer_from_class(self, clazz: str, printer: str):
        class_uri = 'ipp://localhost/classes/{0}'.format(clazz)

        try:
            # get current class members
            members = self.get_printer_attributes(clazz, ['member-uris'])['member-uris']
        except IppException:
            return True

        if isinstance(members, list):
            # update class if members are more then one
            members.remove('ipp://{0}:{1}/printers/{2}'.format(self.__host, self.__port, printer))

            data = construct_request(IppOperation.CUPS_ADD_MODIFY_CLASS, 1, operation_attributes={
                'printer-uri': class_uri
            }, printer_attributes={
                'member-uris': members
            })
        else:
            # delete class if only one member
            data = construct_request(IppOperation.CUPS_DELETE_CLASS, 1, operation_attributes={
                'printer-uri': class_uri
            })

        self._connection.request('POST', self._construct_uri('admin', ''),
                                 headers=self._construct_headers(data), body=data)
        parse_response(self._get_response_data())

        return True

    def delete_class(self, clazz: str):
        class_uri = 'ipp://localhost/classes/{0}'.format(clazz)

        data = construct_request(IppOperation.CUPS_DELETE_PRINTER, 1, {
            'printer-uri': class_uri
        })

        self._connection.request('POST', self._construct_uri('admin', ''),
                                 headers=self._construct_headers(data), body=data)
        parse_response(self._get_response_data())

        return True

    def create_printer(self, name, device_uri='/dev/null', ppd='raw', is_shared=False, error_policy='stop-printer',
                       information='', location=''):
        printer_uri = 'ipp://localhost/printers/{0}'.format(name)

        data = construct_request(IppOperation.CUPS_ADD_MODIFY_PRINTER, 1, {
            'printer-uri': printer_uri,
            'ppd-name': ppd,
            'printer-is-shared': is_shared
        }, printer_attributes={
            'printer-state-reason': 'none',
            'device-uri': device_uri,
            'printer-error-policy': error_policy,
            'printer-info': information,
            'printer-location': location
        })

        self._connection.request('POST', self._construct_uri('admin', ''),
                                 headers=self._construct_headers(data), body=data)
        parse_response(self._get_response_data())

        return True

    def set_printer_ppd(self, printer: str, ppd_name: str):
        printer_uri = 'ipp://localhost/printers/{0}'.format(printer)

        data = construct_request(IppOperation.CUPS_ADD_MODIFY_PRINTER, 1, {
            'printer-uri': printer_uri,
            'ppd-name': ppd_name
        })

        self._connection.request('POST', self._construct_uri('admin', ''),
                                 headers=self._construct_headers(data), body=data)
        parse_response(self._get_response_data())

        return True

    def set_printer_device_uri(self, printer: str, device_uri: str):
        printer_uri = 'ipp://localhost/printers/{0}'.format(printer)

        data = construct_request(IppOperation.CUPS_ADD_MODIFY_PRINTER, 1, {
            'printer-uri': printer_uri
        }, printer_attributes={
            'device-uri': device_uri
        })

        self._connection.request('POST', self._construct_uri('admin', ''),
                                 headers=self._construct_headers(data), body=data)
        parse_response(self._get_response_data())

        return True

    def set_printer_shared(self, printer: str, is_shared=False):
        printer_uri = 'ipp://localhost/printers/{0}'.format(printer)

        data = construct_request(IppOperation.CUPS_ADD_MODIFY_PRINTER, 1, {
            'printer-uri': printer_uri,
            'printer-is-shared': is_shared
        })

        self._connection.request('POST', self._construct_uri('admin', ''),
                                 headers=self._construct_headers(data), body=data)
        parse_response(self._get_response_data())

        return True

    def set_printer_error_policy(self, printer: str, error_policy: str):
        printer_uri = 'ipp://localhost/printers/{0}'.format(printer)

        data = construct_request(IppOperation.CUPS_ADD_MODIFY_PRINTER, 1, {
            'printer-uri': printer_uri
        }, printer_attributes={
            'printer-error-policy': error_policy
        })

        self._connection.request('POST', self._construct_uri('admin', ''),
                                 headers=self._construct_headers(data), body=data)
        parse_response(self._get_response_data())

        return True

    def set_printer_information(self, printer: str, information: str):
        printer_uri = 'ipp://localhost/printers/{0}'.format(printer)

        data = construct_request(IppOperation.CUPS_ADD_MODIFY_PRINTER, 1, {
            'printer-uri': printer_uri
        }, printer_attributes={
            'printer-info': information
        })

        self._connection.request('POST', self._construct_uri('admin', ''),
                                 headers=self._construct_headers(data), body=data)
        parse_response(self._get_response_data())

        return True

    def set_printer_location(self, printer: str, location: str):
        printer_uri = 'ipp://localhost/printers/{0}'.format(printer)

        data = construct_request(IppOperation.CUPS_ADD_MODIFY_PRINTER, 1, {
            'printer-uri': printer_uri
        }, printer_attributes={
            'printer-location': location
        })

        self._connection.request('POST', self._construct_uri('admin', ''),
                                 headers=self._construct_headers(data), body=data)
        parse_response(self._get_response_data())

        return True

    def delete_printer(self, printer: str):
        printer_uri = 'ipp://localhost/printers/{0}'.format(printer)

        data = construct_request(IppOperation.CUPS_DELETE_PRINTER, 1, {
            'printer-uri': printer_uri
        })

        self._connection.request('POST', self._construct_uri('admin', ''),
                                 headers=self._construct_headers(data), body=data)
        parse_response(self._get_response_data())

        return True

    def get_printers(self, attributes=None):
        data = construct_request(IppOperation.CUPS_GET_PRINTERS, 1, {
            'requesting-user-name': self.user,
            'requested-attributes': attributes + ['printer-name'] if attributes else IPP_DEFAULT_PRINTER_ATTRIBUTES
        })

        self._connection.request('POST', self._construct_uri('', ''),
                                 headers=self._construct_headers(data), body=data)

        response_data = parse_response(self._get_response_data())

        return {p['printer-name']: p for p in response_data['printers']}

    def get_classes(self, attributes=None):
        data = construct_request(IppOperation.CUPS_GET_CLASSES, 1, {
            'requesting-user-name': self.user,
            'requested-attributes': attributes + ['printer-name'] if attributes else IPP_DEFAULT_CLASS_ATTRIBUTES
        })

        self._connection.request('POST', self._construct_uri('', ''),
                                 headers=self._construct_headers(data), body=data)

        response_data = parse_response(self._get_response_data())

        return {p['printer-name']: p for p in response_data['printers']}


def parse_control_file(job_id: int, spool_dir='/var/spool/cups'):
    control_file = os.path.join(spool_dir, 'c{0}'.format(job_id))

    if not os.path.exists(control_file):
        return None

    with open(control_file, 'rb') as f:
        data = parse_response_without_check(f.read())

    return data
