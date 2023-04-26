from requests.models import Response
from uplink import Consumer, Query, get, headers


@headers({"Accept": "application/json"})
class DataServers(Consumer):
    """https://docs.osisoft.com/bundle/pi-web-api-reference/page/help/controllers/dataserver.html"""

    @get("/piwebapi/dataservers")
    def list(self, selectedFields: Query = None, webIdType: Query = None) -> Response:
        """https://docs.osisoft.com/bundle/pi-web-api-reference/page/help/controllers/dataserver/actions/list.html"""

    @get("/piwebapi/dataservers/{web_id}/points")
    def get_points(
        self,
        web_id: str,
        nameFilter: Query = None,
        sourceFilter: Query = None,
        startIndex: Query = None,
        maxCount: Query = None,
        selectedFields: Query = None,
        webIdType: Query = None,
    ) -> Response:
        """https://docs.osisoft.com/bundle/pi-web-api-reference/page/help/controllers/dataserver/actions/getpoints.html"""
