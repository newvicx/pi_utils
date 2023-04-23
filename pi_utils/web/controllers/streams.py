from requests.models import Response
from uplink import Consumer, Query, get, headers



@headers({"Accept": "application/json"})
class Streams(Consumer):
    """https://docs.osisoft.com/bundle/pi-web-api-reference/page/help/controllers/stream.html"""
    
    @get("/piwebapi/streams/{web_id}/interpolated")
    def get_interpolated(
        self,
        web_id: str,
        startTime: Query = None,
        endTime: Query = None,
        timeZone: Query = None,
        interval: Query = None,
        syncTime: Query = None,
        syncTimeBoundaryType: Query = None,
        desiredUnits: Query = None,
        filterExpression: Query = None,
        includeFilteredValues: Query = None,
        selectedFields: Query = None
    ) -> Response:
        """https://docs.osisoft.com/bundle/pi-web-api-reference/page/help/controllers/stream/actions/getinterpolated.html"""

    @get("/piwebapi/streams/{web_id}/recorded")
    def get_recorded(
        self,
        web_id: str,
        startTime: Query = None,
        endTime: Query = None,
        timeZone: Query = None,
        boundaryType: Query = None,
        desiredUnits: Query = None,
        filterExpression: Query = None,
        includeFilteredValues: Query = None,
        maxCount: Query = None,
        selectedFields: Query = None,
        associations: Query = None
    ) -> Response:
        """https://docs.osisoft.com/bundle/pi-web-api-reference/page/help/controllers/stream/actions/getrecorded.html"""

    @get("/piwebapi/streams/{web_id}/recordedattime")
    def get_recorded_at_time(
        self,
        web_id: str,
        time: Query = None,
        timeZone: Query = None,
        retrievalMode: Query = None,
        desiredUnits: Query = None,
        selectedFields: Query = None,
        associations: Query = None
    ) -> Response:
        """https://docs.osisoft.com/bundle/pi-web-api-reference/page/help/controllers/stream/actions/getrecordedattime.html"""