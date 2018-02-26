# -*- coding: utf8 -*-

class OTSError(Exception):
    pass


class OTSClientError(OTSError):

    def __init__(self, message, http_status = None):
        self.message = message
        self.http_status = http_status

    def get_http_status(self):
        return self.http_status

    def __str__(self):
        return self.message 

    def get_error_message(self):
        return self.message


class OTSServiceError(OTSError):

    def __init__(self, http_status, code, message, request_id = ''):
        self.http_status = http_status
        self.code = code
        self.message = message
        self.request_id = request_id

    def __str__(self):
        err_string = "ErrorCode: %s, ErrorMessage: %s, RequestID: %s" % (
            self.code, self.message, self.request_id)
        return err_string

    def get_http_status(self):
        return self.http_status

    def get_error_code(self):
        return self.code

    def get_error_message(self):
        return self.message

    def get_request_id(self):
        return self.request_id
