import pytest
import json
from server0 import analyze, user_register, user_login, empty_analysis

def test_analysis_engine():
    """Test analysis with empty data"""
    result = analyze({'history': [], 'price': 100})
    assert result['signal'] == 'NEUTRAL'
    assert result['total'] == 0

def test_empty_analysis():
    """Test empty analysis function"""
    result = empty_analysis()
    assert result['signal'] == 'NEUTRAL'
    assert len(result['scores']) == 19

def test_user_auth():
    """Test user registration and login"""
    # Clean up if exists
    try:
        ok, _ = user_register('testuser_v21', 'testpass123')
        assert ok
    except:
        pass
    
    token, user = user_login('testuser_v21', 'testpass123')
    assert token is not None
    assert user['username'] == 'testuser_v21'