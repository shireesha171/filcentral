from ..services.db_data_service import get_target_config_data

from ..utils.utils import handle_errors, parse_location_pattern


def test_get_target_config_data(job_uuid: str):
    result = get_target_config_data(job_uuid)

    return result


def test_parse_location_pattern(key: str):
    result = parse_location_pattern(key)

    print("test_parse_location_pattern::module_test", f"Location Pattern: {result}")


@handle_errors
def test_module(test_case: int):
    if test_case == 100:
        test_get_target_config_data(job_uuid="8e7b8941-56cd-49de-b462-07ba573b2177")
    elif test_case == 200: \
            test_parse_location_pattern(key='s3://fileops-storage-qa/target-files/redshift-test/${%d-%m-%Y}/')
    else:
        print("test_module::module_test", f"Select a valid test case: {test_case}")


test_module(200)
