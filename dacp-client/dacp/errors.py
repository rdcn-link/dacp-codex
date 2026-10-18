"""Public DACP errors, independent of the transport implementation."""
from functools import wraps
import pyarrow as pa
import pyarrow.flight as flight


TRANSPORT_ERRORS = (pa.ArrowException, flight.FlightError)


class DacpError(Exception):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


def translate(error):
    """Use fixed messages; never echo remote descriptions or diagnostic context."""
    detail = str(error).lower()
    if 'source submission requires debug' in detail:
        return DacpError('SOURCE_DISABLED', 'Source submission requires server DEBUG mode.')
    if 'dataset not found' in detail:
        return DacpError('NOT_FOUND', 'Dataset not found. Check the dataset name.')
    if isinstance(error, (flight.FlightUnauthorizedError, flight.FlightUnauthenticatedError)) or 'forbidden' in detail:
        return DacpError('ACCESS_DENIED', 'Access denied. Check your credentials and permissions.')
    if 'not_found' in detail or 'logical resource unavailable' in detail or isinstance(error, pa.ArrowKeyError):
        return DacpError('NOT_FOUND', 'Item or dataset not found. Check the name and path.')
    if isinstance(error, flight.FlightTimedOutError):
        return DacpError('TIMEOUT', 'The request timed out. Try again later.')
    if isinstance(error, flight.FlightUnavailableError):
        return DacpError('UNAVAILABLE', 'Cannot reach the DACP server. Check the address and connection.')
    if isinstance(error, flight.FlightCancelledError):
        return DacpError('CANCELLED', 'The request was cancelled.')
    if 'scan_error' in detail:
        return DacpError('INVALID_ARGUMENT', 'Invalid records query. Check column names, literal types, projection, and limit.')
    if 'format_error' in detail:
        return DacpError('FORMAT_ERROR', 'Cannot read this data format. Check the file, sheet selection, and format settings.')
    if 'schema_conflict' in detail:
        return DacpError('SCHEMA_CONFLICT', 'The data does not match its declared schema.')
    for phrase, message in (
        ('folder has no content stream', 'Folders have no readable content. Use ls to browse them.'),
        ('listing requires a folder', 'Only folders can be listed.'),
        ('unsupported stream view', 'This item does not support the requested view.'),
    ):
        if phrase in detail:
            return DacpError('INVALID_ARGUMENT', message)
    return DacpError('REQUEST_FAILED', 'The DACP request failed. Try again or contact the server administrator.')


def public_errors(function):
    @wraps(function)
    def call(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except TRANSPORT_ERRORS as error:
            raise translate(error) from None
    return call


def user_message(error):
    if isinstance(error, TRANSPORT_ERRORS):
        return str(translate(error))
    if isinstance(error, (DacpError, ValueError)):
        return str(error)
    if isinstance(error, OSError):
        return 'Cannot access the connection or a required local file. Check your settings.'
    return 'The operation failed. Try again or contact the server administrator.'
