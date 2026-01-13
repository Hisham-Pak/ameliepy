import requests

from . import errors as e


class Protocol:
    """Protocol class to handle communication with the Amelie database over HTTP.

    This class is responsible for sending queries to the database and parsing the responses.
    """

    def __init__(self, connection):
        self.connection = connection

    def send_request(self, query):
        """Private helper to send the HTTP request to the DB and handle errors."""
        try:
            resp = self._post_request(query)
            return self._parse_response(resp)
        except requests.RequestException as req_err:
            self._handle_request_exception(req_err, query)

    def _parse_response(self, resp):
        """Parse the HTTP response and return JSON data or an empty list."""
        resp.raise_for_status()
        try:
            return resp.json()
        except ValueError:
            return []

    def _post_request(self, query):
        """Send the POST request to the database."""
        return requests.post(
            f"{self.connection.host}/v1/execute",
            headers={"Content-Type": "text/plain", "Prefer": "return=json-obj-pretty"},
            data=query,
        )

    def _handle_request_exception(self, req_err, query):
        """Handle exceptions raised during the request."""
        resp_obj = getattr(req_err, "response", None)
        msg = self._extract_error_message(resp_obj)
        if not msg:
            raise e.OperationalError(f"Request failed: {str(req_err)}")
        raise e.ProgrammingError(f"{msg} \nFull Executed Query: {query}")

    def _extract_error_message(self, resp_obj):
        """Extract error message from the response object."""
        if resp_obj is not None:
            try:
                body = resp_obj.json()
                if isinstance(body, dict):
                    return body.get("msg")
            except ValueError:
                return self._get_plain_text_error(resp_obj)
        return None

    def _get_plain_text_error(self, resp_obj):
        """Get plain text error message from the response object."""
        try:
            text = resp_obj.text
            if text:
                return text
        except Exception:
            return None
