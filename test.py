
from utils import get_rpc_connection_user_pw

def test_get_rpc_connection_paramiko():
    try:
        rpc_connection = get_rpc_connection_user_pw()
        # Test if the connection is established by calling a simple RPC method
        block_count = rpc_connection.getblockcount()
        print(f"Connection successful. Current block count: {block_count}")
    except Exception as e:
        print(f"Failed to establish RPC connection: {e}")

# Run the test
test_get_rpc_connection_paramiko()
