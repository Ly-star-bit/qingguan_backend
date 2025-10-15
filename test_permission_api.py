import requests
import json

# Base URL of your FastAPI application
BASE_URL = "http://localhost:8084"

def test_permission_endpoints():
    """Test the permission item API endpoints"""
    
    # Test 1: Get all permissions (should return empty list initially)
    print("Testing GET /permission_item...")
    try:
        response = requests.get(f"{BASE_URL}/permission_item")
        print(f"Status: {response.status_code}")
        print(f"Response: {response.json()}")
    except Exception as e:
        print(f"Error: {e}")
    
    # Test 2: Create a permission
    print("\nTesting POST /permission_item...")
    new_permission = {
        "code": "user:read",
        "name": "用户查看",
        "resource": "user",
        "action": "read",
        "description": "查看用户信息的权限"
    }
    try:
        response = requests.post(f"{BASE_URL}/permission_item", json=new_permission)
        print(f"Status: {response.status_code}")
        print(f"Response: {response.json()}")
        
        if response.status_code == 200:
            permission_id = response.json().get("id")
            
            # Test 3: Get the created permission by ID
            print(f"\nTesting GET /permission_item/{permission_id}...")
            response = requests.get(f"{BASE_URL}/permission_item/{permission_id}")
            print(f"Status: {response.status_code}")
            print(f"Response: {response.json()}")
            
            # Test 4: Update the permission
            print(f"\nTesting PUT /permission_item/{permission_id}...")
            updated_permission = {
                "code": "user:read",
                "name": "用户查看权限",
                "resource": "user",
                "action": "read",
                "description": "更新后的查看用户信息的权限"
            }
            response = requests.put(f"{BASE_URL}/permission_item/{permission_id}", json=updated_permission)
            print(f"Status: {response.status_code}")
            print(f"Response: {response.json()}")
            
            # Test 5: Search permissions
            print(f"\nTesting GET /permission_item/search...")
            response = requests.get(f"{BASE_URL}/permission_item/search?resource=user")
            print(f"Status: {response.status_code}")
            print(f"Response: {response.json()}")
            
            # Test 6: Generate test data
            print(f"\nTesting POST /permission_item/generate_test_data...")
            response = requests.post(f"{BASE_URL}/permission_item/generate_test_data")
            print(f"Status: {response.status_code}")
            print(f"Response: {response.json()}")
            
    except Exception as e:
        print(f"Error: {e}")
    
    # Test 7: Delete the permission (if we have an ID)
    if 'permission_id' in locals():
        print(f"\nTesting DELETE /permission_item/{permission_id}...")
        try:
            response = requests.delete(f"{BASE_URL}/permission_item/{permission_id}")
            print(f"Status: {response.status_code}")
            print(f"Response: {response.json()}")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    test_permission_endpoints()