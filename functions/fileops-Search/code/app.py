from .services.advance_search_service import get_job_runs
from .utils.utils import response_body, HTTP_STATUS_CODES, API_MESSAGES, QUERY_RESOURCES
from .utils.search_type_indexes import JOB_RUNS_SEARCH_TYPE_INDEXES


def lambda_handler(event, context):
    try:
        print("lambda_handler::app", "Received Event: \n", str(event))

        query_params = event.get("queryStringParameters", None)
        multi_value_query_string_params = event.get("multiValueQueryStringParameters", None)

        print("lambda_handler::app", f"query_params: f{query_params}")
        print("lambda_handler::app", f"multi_value_query_string_params: f{multi_value_query_string_params}")

        if query_params is None or multi_value_query_string_params is None:
            return response_body(HTTP_STATUS_CODES.FAILED, API_MESSAGES.INVALID_QUERY_PARAMS, None)

        query_resource = query_params.get("query_resource")

        # This will help us differentiate different types of searches for a particular resource
        search_type_index = query_params.get("search_type_index")

        print("lambda_handler::app", f"query_resource: {query_resource}")

        if (event['resource'] == "/search"
                and event['httpMethod'] == 'GET'
                and query_resource == QUERY_RESOURCES.JOB_RUNS.value
                and search_type_index == JOB_RUNS_SEARCH_TYPE_INDEXES.ADVANCE_SEARCH_1.value):

            response = get_job_runs(query_params, multi_value_query_string_params)

            return response_body(HTTP_STATUS_CODES.SUCCESS, API_MESSAGES.JOB_RUN_RECORDS_RETRIEVE_SUCCESS,
                                 response)
        else:
            return response_body(HTTP_STATUS_CODES.FAILED, API_MESSAGES.REQUEST_NOT_SUPPORTED, None)
    except Exception as e:
        print("lambda_handler::app", f"An Exception occurred: {e}")

        return response_body(HTTP_STATUS_CODES.FAILED, API_MESSAGES.LAMBDA_ERROR, None)
