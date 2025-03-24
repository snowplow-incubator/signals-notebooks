from typing import Any
import httpx
from mcp.server.fastmcp import FastMCP

# Initialize FastMCP server
mcp = FastMCP("snowplow_signals_mcp")

from snowplow_signals import Signals, Feature, FeatureView, session_entity, user_entity
sp_signals = Signals(api_url='https://0fcfdf97-6447-4208-8cd0-39f82befbd07.svc.snplow.net')
count_page_views_feature = Feature(
    name="num_page_views",
    dtype="INT32",
    events=[
        "iglu:com.snowplowanalytics.snowplow/page_view/jsonschema/1-0-0"
    ],
    type="counter",
)
performed_events_feature = Feature(
    name="performed_events",
    scope="session",
    dtype="STRING_LIST",
    events=[
        "iglu:com.snowplowanalytics.snowplow/page_view/jsonschema/1-0-0",
        "iglu:com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0",
        "iglu:com.snowplowanalytics.snowplow.media/ready_event/jsonschema/1-0-0",
        "iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1"
    ],
    type="unique_list",
    property="event_name",
)
engaged_10s_periods = Feature(
    name="engaged_10s_periods",
    scope="session",
    dtype="INT32",
    events=[
        "iglu:com.snowplowanalytics.snowplow/page_ping/jsonschema/1-0-0"
    ],
    type="counter"
)
visited_pages_feature = Feature(
    # TODO: add a field for Human/LLM-ready feature description?
    name="visited_pages",
    scope="session",
    dtype="STRING_LIST",
    events=[
        "iglu:com.snowplowanalytics.snowplow/page_view/jsonschema/1-0-0"
    ],
    type="unique_list",
    property="page_title",
)

feature_view_events = FeatureView(
    name="demo_web_event_features",
    version=1,
    entities=[
        session_entity,
    ],
    features=[
        count_page_views_feature,
        performed_events_feature,
        engaged_10s_periods,
        visited_pages_feature,
    ],
)

# TODO: missing smth like .get_feature_views()
#       to get all live feature_views
#       so that I don't need to create it from scratch here each time,
#       and I can just call 
#
#       data = sp_signals.get_online_features(
#           feature_view="my_web_features",
#           entity=session_id,
#           entity_type_id="session",
#       )

@mcp.tool()
async def get_features(session_id: str) -> str:
    """Get Snowplow features for a specific session. Returns web aggregated session details.

    Args:
        session_id: A string that defines session_id, can be some hash or UUID or similar.
    """
    
    data = sp_signals.get_online_features(
        # features=[feature_view],
        features=[feature_view_events],
        entity=session_id,
        entity_type_id="session",
    )
    features = data.data

    if not data or not features:
        return "Unable to fetch attributes for the session"

    # TODO: need to enrich this with a descriptive feature description
    return "\n---\n".join(f"{k}: {v}" for k, v in features.items() if k != 'domain_sessionid')

if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')
