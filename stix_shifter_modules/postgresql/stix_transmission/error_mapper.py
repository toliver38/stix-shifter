from stix_shifter_utils.utils.error_mapper_base import ErrorMapperBase
from stix_shifter_utils.utils.error_response import ErrorCode
from stix_shifter_utils.utils import logger

# PostgreSQL error mapping

error_mapping = {
    # Syntax error
    '42601': ErrorCode.TRANSMISSION_QUERY_PARSING_ERROR,
    # Undefined table
    '42P01': ErrorCode.TRANSMISSION_SEARCH_DOES_NOT_EXISTS,
    # Unique violation
    '23505': ErrorCode.TRANSMISSION_INVALID_PARAMETER,
    # Connection failure
    '08001': ErrorCode.TRANSMISSION_REMOTE_SYSTEM_IS_UNAVAILABLE,
    # Invalid authorization specification
    '28000': ErrorCode.TRANSMISSION_AUTH_CREDENTIALS,
    # Database does not exist
    '3D000': ErrorCode.TRANSMISSION_SEARCH_DOES_NOT_EXISTS,
    # Invalid catalog name
    '3D000': ErrorCode.TRANSMISSION_SEARCH_DOES_NOT_EXISTS,
    # Out of memory
    '53200': ErrorCode.TRANSMISSION_MODULE_DEFAULT_ERROR.value,
    # Disk full
    '53100': ErrorCode.TRANSMISSION_MODULE_DEFAULT_ERROR.value,
    # Other generic errors
    'XX000': ErrorCode.TRANSMISSION_MODULE_DEFAULT_ERROR.value
}


class ErrorMapper():
    logger = logger.set_logger(__name__)
    DEFAULT_ERROR = ErrorCode.TRANSMISSION_MODULE_DEFAULT_ERROR

    @staticmethod
    def set_error_code(json_data, return_obj, connector=None):
        code = None
        try:
            code = str(json_data['code'])
        except Exception:
            pass

        error_code = ErrorMapper.DEFAULT_ERROR

        if code in error_mapping:
            error_code = error_mapping[code]

        if error_code == ErrorMapper.DEFAULT_ERROR:
            ErrorMapper.logger.error("failed to map: " + str(json_data))

        ErrorMapperBase.set_error_code(return_obj, error_code, connector=None)
