from datetime import datetime, timedelta
import calendar
import os
import requests
from requests.auth import HTTPBasicAuth
import sys
from sas7bdat import SAS7BDAT

# ============================================================
# Environment
# ============================================================
devops_base_url = os.getenv("AZDO_BASE_URL")
devops_pat = os.getenv("AZDO_PAT")
server_env = os.getenv("SERVER_ENV")

cert = "Data_Warehouse/Common/Cert/Public Bank Group Root CA.pem"
base_dir = sys.prefix
parquet_path = os.path.join(base_dir, cert)  

API_VERSION = "7.0-preview.1"

# ============================================================
# Common helpers (reduce duplication)
# ============================================================

def _get_auth():
    """Get HTTP Basic Auth for Azure DevOps"""
    return HTTPBasicAuth("PAT_USER", devops_pat)

def _parse_datetime(date_str):
    """Parse datetime string to datetime object"""
    return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

def _format_datetime(dt):
    """Format datetime object to string"""
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def _get_variable_groups():
    """Fetch all variable groups from Azure DevOps"""
    url = f"{devops_base_url}/_apis/distributedtask/variablegroups?api-version={API_VERSION}"
    resp = requests.get(url, auth=_get_auth(), verify=cert)
    resp.raise_for_status()
    return resp.json().get("value", [])

def _get_variable_group_by_name(group_name):
    """Get variable group ID by name"""
    groups = _get_variable_groups()
    group = next((g for g in groups if g["name"] == group_name), None)
    if not group:
        raise ValueError(f"Group {group_name} not found")
    return group["id"]

def _get_variable_group_details(group_id):
    """Get detailed info about a variable group"""
    url = f"{devops_base_url}/_apis/distributedtask/variablegroups/{group_id}?api-version={API_VERSION}"
    resp = requests.get(url, auth=_get_auth(), verify=cert)
    resp.raise_for_status()
    return resp.json()

# ============================================================
# Batch date access (No pandas)
# ============================================================

def _read_sas_file(sas_file_path):
    """
    Read SAS7BDAT file and return dict of SOURCE_SYSTEM_CD -> BATCH_DTTM.
    No pandas needed.
    """
    result = {}
    try:
        with SAS7BDAT(sas_file_path) as reader:
            for row in reader:
                # row is a tuple; access columns by index
                # Assumes format: (SOURCE_SYSTEM_CD, BATCH_DTTM)
                if len(row) >= 2:
                    source_system_cd = row[0]
                    batch_dttm = row[1]
                    result[source_system_cd] = _format_datetime(batch_dttm) if isinstance(batch_dttm, datetime) else batch_dttm
    except Exception as e:
        print(f"Error reading SAS file {sas_file_path}: {e}")
        return None
    return result if result else None

def _batch_date_access(ctl_path, source_system_cd, system_name):
    """
    Get batch date for a source system from local SAS file or Azure DevOps fallback.
    No pandas needed.
    """
    # === LOCAL SAS FILE ===
    if os.path.exists(ctl_path):
        sas_data = _read_sas_file(ctl_path)
        if sas_data and source_system_cd in sas_data:
            return sas_data[source_system_cd]

    # === AZURE DEVOPS FALLBACK ===
    group_name = f"{system_name}_BATCH_DATE_{server_env}"
    group_id = _get_variable_group_by_name(group_name)
    group_data = _get_variable_group_details(group_id)

    variables = group_data.get("variables", {})
    if source_system_cd not in variables:
        raise ValueError(f"Source system code {source_system_cd} not found")

    return variables[source_system_cd]["value"]

# ============================================================
# Public batch date APIs
# ============================================================

def get_batch_date_dwh(source_system_cd):
    """Get DWH batch date for a source system"""
    ctl_path = "/sasdata/dwh/control/ctl_dwh_batch_dttm.sas7bdat"
    return _batch_date_access(ctl_path, source_system_cd, "DWH")

def get_batch_date_mart(source_system_cd):
    """Get MART batch date for a source system"""
    ctl_path = "/sasdata/dwh/control/ctl_mart_batch_dttm.sas7bdat"
    return _batch_date_access(ctl_path, source_system_cd, "MART")

# ============================================================
# Date utilities (No pandas)
# ============================================================

def first_date_of_month(date):
    """Get first date of month"""
    dt = _parse_datetime(date)
    return _format_datetime(dt.replace(day=1, hour=0, minute=0, second=0))

def last_date_of_month(date):
    """Get last date of month"""
    dt = _parse_datetime(date)
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    return _format_datetime(dt.replace(day=last_day, hour=23, minute=59, second=59))

def format_date(date, format_str):
    """Format date with custom format string"""
    return _parse_datetime(date).strftime(format_str)

def mid15_25_of_month(date):
    """
    Get date range for mid-month batch (15th to 25th).
    Returns dict with 'start_date' and 'end_date'.
    """
    dt = _parse_datetime(date)
    dd = dt.day

    if dd == 25:
        start_date = dt.replace(day=15)
        end_date = dt.replace(day=24)
    elif dd == 15:
        start_date = (dt.replace(day=1) - timedelta(days=1)).replace(day=25)
        end_date = dt.replace(day=14)
    else:
        start_date = None
        end_date = None

    return {
        "start_date": _format_datetime(start_date) if start_date else None,
        "end_date": _format_datetime(end_date) if end_date else None
    }

def get_past_n_date(date, n):
    """Get date n days in the past"""
    dt = _parse_datetime(date)
    return _format_datetime(dt - timedelta(days=n))

def last_date_of_batch_date(date):
    """Get end of day for a given date"""
    dt = _parse_datetime(date)
    return _format_datetime(dt.replace(hour=23, minute=59, second=59))

# ============================================================
# Update batch date (No pandas)
# ============================================================

def get_azure_batch_dates():
    """
    Retrieve batch dates from Azure DevOps variable groups.
    Returns tuple of dicts: (dwh_dates, mart_dates) or (None, None) on failure.
    No pandas needed.
    """
    try:
        groups = _get_variable_groups()

        # DWH dates
        group_name = f"DWH_BATCH_DATE_{server_env}"
        group = next((g for g in groups if g["name"] == group_name), None)
        if not group:
            print(f"Invalid library {group_name} in Azure DevOps")
            return None, None

        dwh_dates = {
            var_name: var_data["value"]
            for var_name, var_data in group.get("variables", {}).items()
        }

        # MART dates
        group_name_mart = f"MART_BATCH_DATE_{server_env}"
        group_mart = next((g for g in groups if g["name"] == group_name_mart), None)
        if not group_mart:
            print(f"Invalid library {group_name_mart} in Azure DevOps")
            return None, None

        mart_dates = {
            var_name: var_data["value"]
            for var_name, var_data in group_mart.get("variables", {}).items()
        }

        return dwh_dates, mart_dates

    except Exception as e:
        print(f"Azure Connection failed: {e}")
        return None, None

def update_batch_date(source_system_cd, new_value, mart_identifier=0):
    """
    Update batch date for a source system in Azure DevOps.
    mart_identifier=0 for DWH, 1 for MART.
    """
    group_name = (
        f"MART_BATCH_DATE_{server_env}"
        if mart_identifier
        else f"DWH_BATCH_DATE_{server_env}"
    )

    group_id = _get_variable_group_by_name(group_name)
    group_data = _get_variable_group_details(group_id)

    variables = group_data.get("variables", {})
    if source_system_cd not in variables:
        raise ValueError(f"Source system code {source_system_cd} not found")

    group_data["variables"][source_system_cd]["value"] = new_value

    url = f"{devops_base_url}/_apis/distributedtask/variablegroups/{group_id}?api-version={API_VERSION}"
    headers = {"Content-Type": "application/json"}

    resp = requests.put(url, json=group_data, auth=_get_auth(), headers=headers, verify=cert)
    resp.raise_for_status()
    
    print(f"Successfully updated {source_system_cd} to {new_value}")