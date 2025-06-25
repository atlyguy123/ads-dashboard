"""
Meta API Service

This module handles communication with the Meta (Facebook) Marketing API.
Supports both synchronous and asynchronous data retrieval with automatic
mode selection for optimal performance.

🚨 IMPORTANT FOR SCALING/PARALLELIZATION:
Read META_API_RATE_LIMITING_GUIDE.md before implementing high-volume patterns!

Key throttling headers to monitor:
- X-FB-Ads-Insights-Throttle (per-app/account realtime load %)
- X-Business-Use-Case-Usage (rolling hourly totals)

Sustainable production rate: ~150-250 Insights requests/second
Always prefer async jobs for date ranges > 1 day or heavy breakdowns.
"""

import requests
import json
import os
import sys
import time
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import urllib.parse # Helper for query string
import logging

# Load environment variables from project root
project_root = Path(__file__).resolve().parent.parent.parent.parent
env_file = project_root / '.env'
load_dotenv(env_file)

logger = logging.getLogger(__name__)

def get_meta_credentials():
    """Get Meta API credentials from environment variables"""
    access_token = os.getenv('META_ACCESS_TOKEN')
    account_id = os.getenv('META_ACCOUNT_ID', '340824298709668')  # Use env var or default
    
    if not access_token:
        return None, "META_ACCESS_TOKEN not found in .env file"
    
    return {
        'access_token': access_token,
        'account_id': account_id
    }, None

def start_async_meta_job(start_date, end_date, time_increment, fields=None, breakdowns=None, action_breakdowns=None):
    """
    Start an async Meta API job for large data requests
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        time_increment (int): Time increment in days
        fields (str, optional): Comma-separated list of fields to retrieve
        breakdowns (str, optional): Comma-separated list of breakdowns to apply
        
    Returns:
        tuple: (job_info, error) where job_info contains report_run_id and status
    """
    # Get credentials
    creds, error = get_meta_credentials()
    if error:
        return None, error
    
    access_token = creds['access_token']
    account_id = creds['account_id']
    
    # API parameters
    api_version = os.getenv('META_API_VERSION', 'v22.0')
    
    # Construct API URL for async job
    api_url = f"https://graph.facebook.com/{api_version}/act_{account_id}/insights"
    
    # Default fields if none provided
    default_fields = 'ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,impressions,clicks,spend'
    
    # Meta's async API has specific requirements - might not work for very simple requests
    # Calculate if this request is substantial enough for async processing
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    date_range_days = (end_dt - start_dt).days
    
    # Parameters for the async request
    params = {
        'access_token': access_token,
        'level': 'ad',
        'time_increment': time_increment,
        'fields': fields or default_fields,
        'time_range': json.dumps({
            'since': start_date,
            'until': end_date
        }),
        'limit': 5000,  # Higher limit for async jobs
        'async': 'true'  # Enable async processing
        # Note: export_format removed - we'll use API pagination instead of file download
    }
    
    # Add breakdowns if provided
    if breakdowns:
        params['breakdowns'] = breakdowns
    
    # Add action_breakdowns if provided
    if action_breakdowns:
        params['action_breakdowns'] = action_breakdowns
    
    try:
        # Make async job request
        query_string = urllib.parse.urlencode(params, doseq=True)
        logger.debug(f'Meta Async API URL: {api_url}?{query_string}')
        
        response = requests.post(api_url, data=params)
        response.raise_for_status()
        
        job_data = response.json()
        
        return {
            'report_run_id': job_data.get('report_run_id'),
            'async_status': job_data.get('async_status', 'Job Started'),
            'start_date': start_date,
            'end_date': end_date,
            'fields': fields or default_fields,
            'breakdowns': breakdowns,
            'action_breakdowns': action_breakdowns
        }, None
        
    except requests.exceptions.HTTPError as e:
        error_message = f"Async Job Start Failed with status {e.response.status_code}"
        try:
            error_data = e.response.json()
            error_details = json.dumps(error_data)
            return None, f"{error_message}: {error_details}"
        except:
            return None, f"{error_message}: {e.response.text}"
    
    except Exception as e:
        return None, f"Error starting async job: {str(e)}"

def check_async_job_status(report_run_id):
    """
    Check the status of an async Meta API job
    
    Args:
        report_run_id (str): The report run ID from start_async_meta_job
        
    Returns:
        tuple: (status_info, error) where status_info contains job progress
    """
    # Get credentials
    creds, error = get_meta_credentials()
    if error:
        return None, error
    
    access_token = creds['access_token']
    api_version = os.getenv('META_API_VERSION', 'v22.0')
    
    # Construct status check URL
    status_url = f"https://graph.facebook.com/{api_version}/{report_run_id}"
    
    params = {
        'access_token': access_token
    }
    
    try:
        response = requests.get(status_url, params=params)
        response.raise_for_status()
        
        status_data = response.json()
        
        return {
            'report_run_id': report_run_id,
            'async_status': status_data.get('async_status'),
            'async_percent_completion': status_data.get('async_percent_completion', 0),
            'file_url': status_data.get('file_url'),
            'date_start': status_data.get('date_start'),
            'date_stop': status_data.get('date_stop'),
            'time_completed': status_data.get('time_completed')
        }, None
        
    except requests.exceptions.HTTPError as e:
        error_message = f"Job Status Check Failed with status {e.response.status_code}"
        try:
            error_data = e.response.json()
            error_details = json.dumps(error_data)
            return None, f"{error_message}: {error_details}"
        except:
            return None, f"{error_message}: {e.response.text}"
    
    except Exception as e:
        return None, f"Error checking job status: {str(e)}"

def get_async_job_results(report_run_id, use_file_url=False):
    """
    Get results from a completed async Meta API job
    
    Args:
        report_run_id (str): The report run ID from start_async_meta_job
        use_file_url (bool): Whether to download from file_url or paginate through API
        
    Returns:
        tuple: (results, error) where results contains the data
    """
    # Get credentials
    creds, error = get_meta_credentials()
    if error:
        return None, error
    
    access_token = creds['access_token']
    api_version = os.getenv('META_API_VERSION', 'v22.0')
    
    try:
        # First check if job is complete and get file_url if available
        status_info, status_error = check_async_job_status(report_run_id)
        if status_error:
            return None, status_error
            
        if status_info['async_status'] != 'Job Completed':
            return None, f"Job not completed yet. Status: {status_info['async_status']}"
        
        # If we have a file_url and want to use it, download the file
        if use_file_url and status_info.get('file_url'):
            file_response = requests.get(status_info['file_url'])
            file_response.raise_for_status()
            
            # The file is gzipped JSON - we'd need to handle decompression here
            # For now, fall back to API pagination
            pass
        
        # Get results via API pagination (more reliable for now)
        results_url = f"https://graph.facebook.com/{api_version}/{report_run_id}/insights"
        
        params = {
            'access_token': access_token,
            'limit': 5000  # Large page size for async results
        }
        
        all_data = []
        page_count = 0
        
        response = requests.get(results_url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        # Process first page of results
        if 'data' in data and len(data['data']) > 0:
            all_data.extend(data['data'])
            page_count += 1
            
            # Follow pagination to get all results
            while 'paging' in data and 'next' in data['paging']:
                response = requests.get(data['paging']['next'])
                response.raise_for_status()
                data = response.json()
                
                if 'data' in data and len(data['data']) > 0:
                    all_data.extend(data['data'])
                    page_count += 1
                else:
                    break
        
        return {
            'data': {
                'data': all_data
            },
            'meta': {
                'total_records': len(all_data),
                'pages_fetched': page_count,
                'report_run_id': report_run_id,
                'async_completion': status_info['async_percent_completion'],
                'date_start': status_info.get('date_start'),
                'date_stop': status_info.get('date_stop')
            }
        }, None
        
    except requests.exceptions.HTTPError as e:
        error_message = f"Results Fetch Failed with status {e.response.status_code}"
        try:
            error_data = e.response.json()
            error_details = json.dumps(error_data)
            return None, f"{error_message}: {error_details}"
        except:
            return None, f"{error_message}: {e.response.text}"
    
    except Exception as e:
        return None, f"Error fetching results: {str(e)}"

def fetch_meta_data(start_date, end_date, time_increment, fields=None, breakdowns=None, action_breakdowns=None, use_async=None):
    """
    Fetch data from Meta API - automatically chooses sync vs async based on request size
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        time_increment (int): Time increment in days
        fields (str, optional): Comma-separated list of fields to retrieve
        breakdowns (str, optional): Comma-separated list of breakdowns to apply
        use_async (bool, optional): Force async mode. If None, auto-detect based on request
        
    Returns:
        tuple: (data, error) where data is the API response and error is an error message if any
    """
    # Always use async for the best user experience!
    # Async provides: progress tracking, no timeouts, cancel functionality, better UX
    should_use_async = use_async if use_async is not None else True
    
    if should_use_async:
        # Try async processing first
        job_info, error = start_async_meta_job(start_date, end_date, time_increment, fields, breakdowns, action_breakdowns)
        if error:
            # If async fails, fall back to sync mode with a note
            print(f"Async job creation failed: {error}, falling back to sync mode")
            return fetch_meta_data_sync(start_date, end_date, time_increment, fields, breakdowns, action_breakdowns)
            
        return {
            'async_job': True,
            'report_run_id': job_info['report_run_id'],
            'async_status': job_info['async_status'],
            'meta': {
                'start_date': start_date,
                'end_date': end_date,
                'fields': fields,
                'breakdowns': breakdowns,
                'action_breakdowns': action_breakdowns,
                'processing_mode': 'async'
            }
        }, None
    else:
        # Use original synchronous method for small requests
        return fetch_meta_data_sync(start_date, end_date, time_increment, fields, breakdowns, action_breakdowns)

def fetch_meta_data_sync(start_date, end_date, time_increment, fields=None, breakdowns=None, action_breakdowns=None):
    """
    Original synchronous fetch method for small requests
    """
    # Get credentials
    creds, error = get_meta_credentials()
    if error:
        return None, error
    
    access_token = creds['access_token']
    account_id = creds['account_id']
    
    # API parameters
    api_version = os.getenv('META_API_VERSION', 'v22.0')
    
    # Construct API URL
    api_url = f"https://graph.facebook.com/{api_version}/act_{account_id}/insights"
    
    # Default fields if none provided
    default_fields = 'ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,impressions,clicks,spend'
    
    # Parameters for the request
    params = {
        'access_token': access_token,
        'level': 'ad',
        'time_increment': time_increment,
        'fields': fields or default_fields,
        'time_range': json.dumps({
            'since': start_date,
            'until': end_date
        }),
        'limit': 100,
    }
    
    # Add breakdowns if provided
    if breakdowns:
        params['breakdowns'] = breakdowns
    
    # Add action_breakdowns if provided
    if action_breakdowns:
        params['action_breakdowns'] = action_breakdowns
    
    all_data = []
    page_count = 0
    
    try:
        # Make initial request
        query_string = urllib.parse.urlencode(params, doseq=True)
        logger.debug(f'Meta API URL: {api_url}?{query_string}')
        response = requests.get(api_url, params=params)
        response.raise_for_status()  # Check for HTTP errors
        
        data = response.json()
        
        # Process first page of results
        if 'data' in data and len(data['data']) > 0:
            all_data.extend(data['data'])
            page_count += 1
            
            # Follow pagination to get all results
            while 'paging' in data and 'next' in data['paging']:
                response = requests.get(data['paging']['next'])
                response.raise_for_status()
                data = response.json()
                
                if 'data' in data and len(data['data']) > 0:
                    all_data.extend(data['data'])
                    page_count += 1
                else:
                    break
        
        # Return all collected data (including empty results)
        return {
            'data': {
                'data': all_data
            },
            'meta': {
                'total_records': len(all_data),
                'pages_fetched': page_count,
                'start_date': start_date,
                'end_date': end_date,
                'time_increment': time_increment,
                'fields': fields or default_fields,
                'breakdowns': breakdowns,
                'action_breakdowns': action_breakdowns,
                'processing_mode': 'sync'
            }
        }, None
    
    except requests.exceptions.HTTPError as e:
        error_message = f"API Request Failed with status {e.response.status_code}"
        try:
            error_data = e.response.json()
            error_details = json.dumps(error_data)
            return None, f"{error_message}: {error_details}"
        except:
            return None, f"{error_message}: {e.response.text}"
    
    except Exception as e:
        return None, f"Error: {str(e)}" 