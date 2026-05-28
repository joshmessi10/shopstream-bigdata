import pytest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import validate_event

def test_valid_page_view():
    valid_event = {
        "event_type": "page_view",
        "user_id": "u123",
        "session_id": "s123",
        "timestamp": "2023-10-10T10:00:00Z",
        "page_url": "http://example.com/home",
        "page_type": "home",
        "time_on_page_seconds": 30,
        "device_type": "mobile",
        "country": "CO"
    }
    validate_event(valid_event)

def test_invalid_event_type():
    invalid_event = {
        "event_type": "unknown_event",
        "user_id": "u123"
    }
    with pytest.raises(ValueError, match="Unknown event type"):
        validate_event(invalid_event)

def test_missing_fields():
    invalid_event = {
        "event_type": "page_view",
        "user_id": "u123",
        "session_id": "s123",
        "timestamp": "2023-10-10T10:00:00Z",
        "page_url": "http://example.com/home",
        "page_type": "home",
        "time_on_page_seconds": 30,
        "device_type": "mobile"
    }
    with pytest.raises(ValueError, match="Missing fields"):
        validate_event(invalid_event)

def test_negative_price_product_view():
    invalid_event = {
        "event_type": "product_view",
        "user_id": "u123",
        "session_id": "s123",
        "timestamp": "2023-10-10T10:00:00Z",
        "product_id": "p123",
        "category": "electronics",
        "price": -10.5,
        "time_on_page_seconds": 60
    }
    with pytest.raises(ValueError, match="Negative price"):
        validate_event(invalid_event)

def test_invalid_action_cart_event():
    invalid_event = {
        "event_type": "cart_event",
        "user_id": "u123",
        "session_id": "s123",
        "timestamp": "2023-10-10T10:00:00Z",
        "product_id": "p123",
        "action": "buy"
    }
    with pytest.raises(ValueError, match="Invalid action"):
        validate_event(invalid_event)

def test_invalid_device_type():
    invalid_event = {
        "event_type": "page_view",
        "user_id": "u123",
        "session_id": "s123",
        "timestamp": "2023-10-10T10:00:00Z",
        "page_url": "http://example.com/home",
        "page_type": "home",
        "time_on_page_seconds": 30,
        "device_type": "smartwatch",
        "country": "CO"
    }
    with pytest.raises(ValueError, match="Invalid device_type"):
        validate_event(invalid_event)

def test_invalid_page_type():
    invalid_event = {
        "event_type": "page_view",
        "user_id": "u123",
        "session_id": "s123",
        "timestamp": "2023-10-10T10:00:00Z",
        "page_url": "http://example.com/home",
        "page_type": "admin_dashboard",
        "time_on_page_seconds": 30,
        "device_type": "mobile",
        "country": "CO"
    }
    with pytest.raises(ValueError, match="Invalid page_type"):
        validate_event(invalid_event)
